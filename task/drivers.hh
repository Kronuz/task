/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The schedulers a caller drives itself: inline_scheduler (deterministic, manual clock) and
// blocking_scheduler (one driving thread, steady clock), both with a timer wheel.

#pragma once

#include "scheduler.hh"

namespace task {

// ---------------------------------------------------------------------------------------------
// Timers

namespace detail {
struct timed_cancel_state;
template <typename S>
class wheel_timer;
}  // namespace detail

class inline_scheduler;

namespace detail {
// The inline_scheduler whose run() is draining on this thread, if any.
inline thread_local inline_scheduler* draining_scheduler = nullptr;

// A queued resume or post_call: fn(arg), a resume being resume_handle(h.address()).
struct queued_call {
	void (*fn)(void*);
	void* arg;
	static void resume_handle(void* h) { std::coroutine_handle<>::from_address(h).resume(); }
	static queued_call of(std::coroutine_handle<> h) noexcept { return {&resume_handle, h.address()}; }
	void operator()() const { fn(arg); }
};
}

// A deterministic single-threaded scheduler: FIFO, demoted handles only when nothing
// else is ready, and a manual clock whose advance() fires the timers it passes, with no real
// time. For tests and simulation.
class inline_scheduler {
public:
	// The manual clock: nanoseconds that only advance() moves. It has no static now(); the
	// scheduler's now() reads it.
	struct clock {
		using rep = std::int64_t;
		using period = std::nano;
		using duration = std::chrono::nanoseconds;
		using time_point = std::chrono::time_point<clock>;
		static constexpr bool is_steady = true;
	};

	void post(std::coroutine_handle<> h) { ready_.push_back(detail::queued_call::of(h)); }
	void post_demoted(std::coroutine_handle<> h) { demoted_.push_back(h); }
	// Queues fn(arg) among the ready handles, in order, on the thread that drives it (the last to
	// call run); from another thread it waits in an inbox that the next step moves behind them,
	// before any demoted handle (a cross-thread cancel, a flush thread).
	void post_call(void (*fn)(void*), void* arg) {
		if (detail::draining_scheduler == this) {
			ready_.push_back({fn, arg});
			return;
		}
		std::lock_guard lock(inbox_mutex_);  // a driver takes it to become the owner
		if (owner_ == std::this_thread::get_id()) {
			ready_.push_back({fn, arg});
			return;
		}
		inbox_.push_back({fn, arg});
		inbox_waiting_.store(true, std::memory_order_release);
	}
	std::uint64_t now() const noexcept { return now_; }
	// Moves the clock and fires every timer due by then.
	void advance(std::uint64_t ns) noexcept {
		now_ += ns;
		if (wheel_) { wheel_->expire(now_); }
	}

	// Files e to fire in the advance() that passes `at`, or moves it if armed.
	void arm(wheel::timer_entry& e, std::uint64_t at) { wheel().arm(e, at); }
	// Takes e off the timers; true if it was armed.
	bool cancel(wheel::timer_entry& e) noexcept {
		if (!e.armed()) { return false; }
		wheel_->cancel(e);
		return true;
	}
	// Entries armed on its timers.
	std::size_t armed_timers() const noexcept { return wheel_ ? wheel_->size() : 0; }
	// Files task::deadline's entry on the wheel at `at`; it fires in the advance() that passes it.
	template <typename F>
	detail::wheel_timer<inline_scheduler> start_timer(clock::time_point at, F&& on_fire);

	// Runs until no handle is queued; returns how many ran.
	std::size_t run() {
		return run_until([] { return false; });
	}

	// Like run(), but stops as soon as stop() is true, checked before each handle.
	template <typename P>
	std::size_t run_until(P&& stop) {
#ifndef NDEBUG
		struct driving {
			std::atomic<std::thread::id>& driver;
			bool took;
			~driving() {
				if (took) { driver.store({}, std::memory_order_release); }
			}
		};
		std::thread::id idle{};
		driving guard{driver_, driver_.compare_exchange_strong(idle, std::this_thread::get_id(), std::memory_order_acq_rel)};
		assert((guard.took || idle == std::this_thread::get_id()) && "one thread drives an inline_scheduler at a time");
#endif
		running_on scope(scheduler_ref::of(*this));
		struct mark {
			inline_scheduler* previous = detail::draining_scheduler;
			~mark() { detail::draining_scheduler = previous; }
		} marker;
		detail::draining_scheduler = this;
		{
			std::lock_guard lock(inbox_mutex_);
			owner_ = std::this_thread::get_id();
		}
		std::size_t n = 0;
		while (!stop()) {
			take_inbox();
			detail::slice_start = cycles::read();  // each resume is its own slice
			if (!ready_.empty()) {
				const detail::queued_call c = ready_.front();
				ready_.pop_front();
				c();
			} else if (!demoted_.empty()) {
				const auto h = demoted_.front();
				demoted_.pop_front();
				h.resume();
			} else {
				break;
			}
			++n;
		}
		return n;
	}

	// What the next run() resumes, a post_call waiting in the inbox included.
	std::size_t queued() const noexcept {
		std::size_t n = ready_.size() + demoted_.size();
		if (inbox_waiting_.load(std::memory_order_acquire)) {
			std::lock_guard lock(inbox_mutex_);
			n += inbox_.size();
		}
		return n;
	}

private:
	friend class detail::wheel_timer<inline_scheduler>;
	static void drop_timer(inline_scheduler* self, detail::timed_cancel_state* s) noexcept;
	// Moves what other threads posted behind the ready handles.
	void take_inbox() {
		if (!inbox_waiting_.load(std::memory_order_acquire)) { return; }
		std::lock_guard lock(inbox_mutex_);
		for (const auto& c : inbox_) { ready_.push_back(c); }
		inbox_.clear();
		inbox_waiting_.store(false, std::memory_order_relaxed);
	}
	// The timers advance() fires, made on first use.
	wheel::timer_wheel& wheel() {
		if (!wheel_) { wheel_ = std::make_unique<wheel::timer_wheel>(now_); }
		return *wheel_;
	}

	std::deque<detail::queued_call> ready_;
	std::deque<std::coroutine_handle<>> demoted_;
	std::uint64_t now_ = 0;
	std::unique_ptr<wheel::timer_wheel> wheel_;
	mutable std::mutex inbox_mutex_;
	std::thread::id owner_ = std::this_thread::get_id();  // the thread that drives it; under inbox_mutex_
	std::atomic<std::thread::id> driver_{};  // the thread inside run_until, if any; in every build, one layout
	std::vector<detail::queued_call> inbox_;  // post_call from other threads
	std::atomic<bool> inbox_waiting_{false};
};

// A scheduler one thread drives with drive(), on a steady clock: post and post_call are
// thread-safe and wake its wait; its timers are a wheel armed, cancelled and expired on the
// driving thread. With nothing queued, drive() sleeps while its may_wait() holds, toward the
// earliest timer or, with none armed, until a post; by default only while a timer is armed, so
// a lost wake returns false at once (async-drivers D5, step 5). It owns no roots; destroyed, it
// drops what is queued unrun and fires what is armed with expired false.
class blocking_scheduler {
public:
	using clock = std::chrono::steady_clock;

	blocking_scheduler() = default;
	blocking_scheduler(const blocking_scheduler&) = delete;
	blocking_scheduler& operator=(const blocking_scheduler&) = delete;
	~blocking_scheduler() {
		if (wheel_) { wheel_->release(); }  // the driver is done: this thread drops what is armed
	}

	// Any thread. Each signals under the lock: once the driver sees its work it may return and
	// the scheduler be destroyed, so nothing touches it after the unlock.
	void post(std::coroutine_handle<> h) { push(detail::queued_call::of(h)); }
	void post_call(void (*fn)(void*), void* arg) { push({fn, arg}); }
	// True on the thread that drives it (the constructing thread until a drive()).
	bool runs_here() const noexcept { return driver_.load(std::memory_order_relaxed) == std::this_thread::get_id(); }
	std::uint64_t now() const noexcept {
		return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch()).count());
	}

	// The driving thread: files e at `at` (nanoseconds, as now()), or moves it if armed; it fires
	// in drive(), at most one tick late.
	void arm(wheel::timer_entry& e, std::uint64_t at) {
		assert(runs_here() && "blocking_scheduler::arm off the driving thread");
		wheel().arm(e, at);
	}
	// The driving thread: takes e off the timers; true if it was armed.
	bool cancel(wheel::timer_entry& e) noexcept {
		assert(runs_here() && "blocking_scheduler::cancel off the driving thread");
		if (!e.armed()) { return false; }
		wheel_->cancel(e);
		return true;
	}
	std::size_t armed_timers() const noexcept { return wheel_ ? wheel_->size() : 0; }
	// Files task::deadline's entry at `at`, on the driving thread; destroyed there too.
	template <typename F>
	detail::wheel_timer<blocking_scheduler> start_timer(clock::time_point at, F&& on_fire);

	// Runs queued work and due timers on the calling thread until done() holds with nothing
	// queued, then returns true. Each pass runs what was queued when it began, then expires the
	// wheel. With nothing queued, while may_wait() holds it sleeps toward the earliest timer, or
	// with none armed until a post; a post cuts either short. may_wait() false returns false.
	template <typename Done, typename MayWait>
	bool drive(Done&& done, MayWait&& may_wait) {
		driver_.store(std::this_thread::get_id(), std::memory_order_relaxed);
		std::unique_lock lock(mutex_);
		for (;;) {
			for (std::size_t n = ready_.size(); n > 0; --n) {  // a task re-posting itself waits for the next pass
				const detail::queued_call c = ready_.front();
				ready_.pop_front();
				lock.unlock();
				{
					running_on scope(scheduler_ref::of(*this));
					c();
				}
				lock.lock();
			}
			lock.unlock();
			if (wheel_) { wheel_->expire(now()); }
			const bool finished = done();
			const bool waits = !finished && may_wait();
			const std::uint64_t next = wheel_ ? wheel_->next() : wheel::timer_wheel::never;
			lock.lock();
			if (!ready_.empty()) { continue; }  // a fire, or another thread, queued work
			if (finished) { return true; }
			if (!waits) { return false; }
			const auto queued = [&] { return !ready_.empty(); };
			if (next == wheel::timer_wheel::never) {  // nothing to fire: only a post ends the wait
				wake_.wait(lock, queued);
				continue;
			}
			constexpr std::uint64_t latest = std::uint64_t(std::chrono::nanoseconds::max().count()) / 2;
			const auto until = next > latest ? clock::now() + std::chrono::hours(24) : clock::time_point(std::chrono::nanoseconds(std::int64_t(next)));
			wake_.wait_until(lock, until, queued);
		}
	}
	// Waits only while a timer is armed (one the clock reaches): with none, a lost wake.
	template <typename Done>
	bool drive(Done&& done) {
		return drive(std::forward<Done>(done), [this] { return wheel_ && wheel_->next() != wheel::timer_wheel::never; });
	}

private:
	friend class detail::wheel_timer<blocking_scheduler>;
	static void drop_timer(blocking_scheduler* self, detail::timed_cancel_state* s) noexcept;
	void push(detail::queued_call c) {
		std::lock_guard lock(mutex_);
		ready_.push_back(c);
		wake_.notify_one();
	}
	wheel::timer_wheel& wheel() {
		if (!wheel_) { wheel_ = std::make_unique<wheel::timer_wheel>(now()); }
		return *wheel_;
	}

	std::atomic<std::thread::id> driver_{std::this_thread::get_id()};
	std::mutex mutex_;
	std::condition_variable wake_;
	std::deque<detail::queued_call> ready_;
	std::unique_ptr<wheel::timer_wheel> wheel_;  // the driving thread's
};

}  // namespace task
