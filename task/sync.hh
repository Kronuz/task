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

// Waiting on each other: event, async_mutex (lock(token)) and when_all.

#pragma once

#include "timing.hh"

namespace task {

// ---------------------------------------------------------------------------------------------
// Events and locks

// Resumes h on s, or on the calling thread when no scheduler was recorded.
namespace detail {
inline void resume_on(scheduler_ref s, std::coroutine_handle<> h) {
	if (s) { s.post(h); } else { h.resume(); }
}
}  // namespace detail

namespace detail {
#ifndef NDEBUG
inline std::atomic<std::size_t> orphan_count{0};
#endif
// Waiters an event's destructor left behind (Debug builds; 0 otherwise).
inline std::size_t orphaned_waiters() noexcept {
#ifndef NDEBUG
	return orphan_count.load(std::memory_order_relaxed);
#else
	return 0;
#endif
}
}  // namespace detail

// A one-shot completion. Awaiting it suspends until set(); each waiter resumes through the
// scheduler it suspended on, in the order it began waiting, so a waiter never runs inside
// set(). set() is idempotent and callable from any thread. At teardown (no thread can call
// set()), a waiting frame may be destroyed: it unlinks itself; an event destroyed with
// waiters marks them orphaned, counted in Debug by detail::orphaned_waiters().
class event {
public:
	event() = default;
	event(const event&) = delete;
	event& operator=(const event&) = delete;
	~event() {
		node* n = head_.load(std::memory_order_acquire);
		if (n == set_marker()) { return; }
		for (; n; n = n->next) {
			n->state = node::orphaned;
#ifndef NDEBUG
			detail::orphan_count.fetch_add(1, std::memory_order_relaxed);
#endif
		}
	}

	bool is_set() const noexcept { return head_.load(std::memory_order_acquire) == set_marker(); }

	void set() noexcept {
		node* list = head_.exchange(set_marker(), std::memory_order_acq_rel);
		if (list == set_marker()) { return; }
		node* first = nullptr;  // the stack holds the newest waiter first
		while (list) {
			node* n = list;
			list = n->next;
			n->next = first;
			first = n;
		}
		while (first) {
			node* n = std::exchange(first, first->next);  // read before n's frame can end
			n->state = node::taken;
			detail::resume_on(n->sched, n->handle);
		}
	}

	struct awaiter {
		enum : unsigned char { idle, linked, taken, orphaned };
		event& e;
		std::coroutine_handle<> handle;
		scheduler_ref sched;
		awaiter* next = nullptr;
		unsigned char state = idle;
		// A frame destroyed while waiting (teardown only): leave the event consistent.
		~awaiter() {
			if (state == linked) { e.unlink(this); }
		}
		bool await_ready() const noexcept { return e.is_set(); }
		bool await_suspend(std::coroutine_handle<> h) noexcept {
			handle = h;
			sched = current();
			state = linked;
			node* head = e.head_.load(std::memory_order_acquire);
			do {
				if (head == set_marker()) {
					state = idle;
					return false;
				}
				next = head;
			} while (!e.head_.compare_exchange_weak(head, this, std::memory_order_acq_rel, std::memory_order_acquire));
			return true;  // set() may already be resuming this frame: touch nothing more
		}
		void await_resume() const noexcept {}
	};
	awaiter operator co_await() noexcept { return awaiter{*this, {}, {}}; }

private:
	using node = awaiter;
	// Teardown only: no thread may be in set().
	void unlink(node* w) noexcept {
		node* n = head_.load(std::memory_order_relaxed);
		if (n == w) {
			head_.store(w->next, std::memory_order_relaxed);
			return;
		}
		for (; n && n != set_marker(); n = n->next) {
			if (n->next == w) {
				n->next = w->next;
				return;
			}
		}
	}
	static node* set_marker() noexcept { return reinterpret_cast<node*>(std::uintptr_t(1)); }
	std::atomic<node*> head_{nullptr};
};

class async_mutex;

namespace detail {

// One queued lock request. The queue entry and the awaiter in the waiting frame each hold a
// reference, so whichever ends last frees it: a frame that ends after a cancel, or an unlock
// that reaches an entry whose frame has ended, never meets freed state. Exactly one of grant
// and cancel (timed_out) settles it.
struct lock_waiter : pooled_frame {
	enum : int { waiting, granted, timed_out };
	lock_waiter* next = nullptr;
	std::coroutine_handle<> handle;
	scheduler_ref sched;
	std::atomic<int> state{waiting};
	std::atomic<int> refs{2};  // the queue entry and the awaiter
	std::atomic<bool> parked{false};

	void retain() noexcept { refs.fetch_add(1, std::memory_order_relaxed); }
	void release() noexcept {
		if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) { delete this; }
	}
	bool settle(int outcome) noexcept {
		int expected = waiting;
		return state.compare_exchange_strong(expected, outcome, std::memory_order_acq_rel, std::memory_order_acquire);
	}
	// The outcome is settled. The waiter resumes now, or, if its await_suspend has not
	// finished, there: that call then returns false and the frame continues in place.
	void wake() noexcept {
		if (parked.exchange(true, std::memory_order_acq_rel)) { resume_on(sched, handle); }
	}
	// The last step of await_suspend: true to suspend (wake() resumes later), false when
	// the outcome is already settled.
	bool park() noexcept { return !parked.exchange(true, std::memory_order_acq_rel); }
};

// What a cancel callback does to a queued waiter: settle it as timed_out and wake it. The
// awaiter's reference keeps the waiter alive while the registration exists.
struct lock_cancel {
	lock_waiter* w;
	void operator()() const noexcept {
		if (w->settle(lock_waiter::timed_out)) {
			TASK_TEST_HOOK(cancel_settled);
			w->wake();
		}
	}
};

}  // namespace detail

// A FIFO mutex for coroutines, usable from every scheduler at once. A waiter suspends
// instead of blocking and resumes through the scheduler it suspended on (on the unlocking
// thread when it had none) when the holder unlocks, which hands the lock over without
// releasing it, so a late arrival cannot barge in. The holder need not be the coroutine's
// thread: a guard may be released anywhere. lock(token) gives up when the token is requested.
// Internally a short mutex guards a queue; no critical section waits or does I/O.
// The destructor asserts that the mutex is unlocked and nobody is waiting.
class async_mutex {
public:
	// Owns the lock until unlock() or destruction; empty when the wait was cancelled.
	class [[nodiscard]] guard {
	public:
		guard() = default;
		guard(guard&& o) noexcept : m_(std::exchange(o.m_, nullptr)) {}
		guard& operator=(guard&& o) noexcept {
			if (this != &o) {
				unlock();
				m_ = std::exchange(o.m_, nullptr);
			}
			return *this;
		}
		guard(const guard&) = delete;
		guard& operator=(const guard&) = delete;
		~guard() { unlock(); }
		explicit operator bool() const noexcept { return m_ != nullptr; }
		void unlock() noexcept {
			if (auto* m = std::exchange(m_, nullptr)) { m->unlock(); }
		}

	private:
		friend class async_mutex;
		explicit guard(async_mutex* m) noexcept : m_(m) {}
		async_mutex* m_ = nullptr;
	};

	async_mutex() = default;
	async_mutex(const async_mutex&) = delete;
	async_mutex& operator=(const async_mutex&) = delete;
	~async_mutex() {
		assert(!locked_);
		while (detail::lock_waiter* w = head_) {
			head_ = w->next;
			w->release();
		}
	}

	// Takes the lock if it is free and nobody is queued.
	[[nodiscard]] bool try_lock() noexcept {
		std::lock_guard g(m_);
		return std::exchange(locked_, true) == false;
	}

	// Hands the lock to the first waiter that has not timed out, or frees it.
	void unlock() noexcept {
		detail::lock_waiter* granted = nullptr;
		{
			std::lock_guard g(m_);
			assert(locked_);
			while (detail::lock_waiter* w = head_) {
				head_ = w->next;
				if (!head_) { tail_ = nullptr; }
				if (w->settle(detail::lock_waiter::granted)) {
					granted = w;
					break;
				}
				w->release();  // timed out while queued
			}
			if (!granted) { locked_ = false; }
		}
		if (granted) {
			granted->wake();
			granted->release();
		}
	}

	class lock_awaiter {
	public:
		explicit lock_awaiter(async_mutex& m) noexcept : m_(m) {}
		lock_awaiter(const lock_awaiter&) = delete;
		lock_awaiter& operator=(const lock_awaiter&) = delete;
		~lock_awaiter() {
			if (w_) { m_.abandon(w_); }
		}
		bool await_ready() noexcept { return m_.try_lock(); }
		bool await_suspend(std::coroutine_handle<> h) {
			w_ = m_.enqueue(h, current());
			return w_ && w_->park();
		}
		guard await_resume() noexcept {
			if (w_) { std::exchange(w_, nullptr)->release(); }
			return guard{&m_};
		}

	private:
		async_mutex& m_;
		detail::lock_waiter* w_ = nullptr;
	};

	// lock() that gives up when its token is requested: the guard is then empty. A free lock
	// is taken whatever the token; a requested token never queues. The cancel callback is
	// registered only after the waiter is queued, and a request that lands in between makes
	// the coroutine continue in place.
	class cancellable_lock_awaiter {
	public:
		cancellable_lock_awaiter(async_mutex& m, cancel_token t) noexcept : m_(m), token_(std::move(t)) {}
		cancellable_lock_awaiter(const cancellable_lock_awaiter&) = delete;
		cancellable_lock_awaiter& operator=(const cancellable_lock_awaiter&) = delete;
		~cancellable_lock_awaiter() {
			cb_.reset();  // waits for a callback running elsewhere, which uses w_
			if (w_) { m_.abandon(w_); }
		}
		bool await_ready() noexcept {
			if (m_.try_lock()) {
				got_ = true;
				return true;
			}
			return token_.requested();
		}
		bool await_suspend(std::coroutine_handle<> h) {
			w_ = m_.enqueue(h, current());
			if (!w_) {
				got_ = true;
				return false;
			}
			TASK_TEST_HOOK(after_enqueue);
			cb_.emplace(token_, detail::lock_cancel{w_});
			return w_->park();
		}
		guard await_resume() noexcept {
			cb_.reset();
			if (w_) {
				got_ = w_->state.load(std::memory_order_acquire) == detail::lock_waiter::granted;
				std::exchange(w_, nullptr)->release();
			}
			return got_ ? guard{&m_} : guard{};
		}

	private:
		async_mutex& m_;
		cancel_token token_;
		detail::lock_waiter* w_ = nullptr;
		bool got_ = false;
		std::optional<cancel_callback<detail::lock_cancel>> cb_;
	};

	// Takes the lock, waiting its turn: `auto g = co_await m.lock();`
	[[nodiscard]] lock_awaiter lock() noexcept { return lock_awaiter{*this}; }

	// Takes the lock unless t is requested first: `auto g = co_await m.lock(dl.token());`
	// with a task::deadline gives a timed lock; the guard is empty when cancelled.
	[[nodiscard]] cancellable_lock_awaiter lock(cancel_token t) noexcept { return cancellable_lock_awaiter{*this, std::move(t)}; }

private:
	// The waiting frame is destroyed before await_resume: a queued waiter settles as
	// timed_out, so unlock() sheds it; a granted one hands the lock on.
	void abandon(detail::lock_waiter* w) noexcept {
		int expected = detail::lock_waiter::waiting;
		if (!w->state.compare_exchange_strong(expected, detail::lock_waiter::timed_out, std::memory_order_acq_rel, std::memory_order_acquire) &&
			expected == detail::lock_waiter::granted) {
			unlock();
		}
		w->release();
	}

	// Queues a waiter, or takes the free lock and returns null.
	detail::lock_waiter* enqueue(std::coroutine_handle<> h, scheduler_ref sched) {
		std::lock_guard g(m_);
		if (!locked_) {
			locked_ = true;
			return nullptr;
		}
		while (head_ && head_->state.load(std::memory_order_acquire) != detail::lock_waiter::waiting) {  // shed timed-out entries
			detail::lock_waiter* dead = head_;
			head_ = dead->next;
			dead->release();
		}
		if (!head_) { tail_ = nullptr; }
		auto* w = new detail::lock_waiter;
		w->handle = h;
		w->sched = sched;
		(tail_ ? tail_->next : head_) = w;
		tail_ = w;
		return w;
	}

	std::mutex m_;
	bool locked_ = false;
	detail::lock_waiter* head_ = nullptr;
	detail::lock_waiter* tail_ = nullptr;
};

// ---------------------------------------------------------------------------------------------
// when_all

namespace detail {

template <typename T>
using result_t = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

struct when_all_state {
	std::atomic<std::size_t> pending;
	std::coroutine_handle<> parent;
	std::exception_ptr error;
	std::atomic<bool> errored{false};
};

// Owns one child: the parent's container destroys a child that has not finished (a frame
// destroyed at teardown); a finished child destroys itself and clears its owner.
struct when_all_child {
	struct promise_type : pooled_frame {
		when_all_state* state = nullptr;
		std::coroutine_handle<promise_type>* owner = nullptr;
		when_all_child get_return_object() noexcept { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
		std::suspend_always initial_suspend() noexcept { return {}; }
		struct final_awaiter {
			bool await_ready() noexcept { return false; }
			std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
				when_all_state* s = h.promise().state;
				*h.promise().owner = nullptr;
				h.destroy();
				if (s->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) { return transfer(s->parent); }
				return std::noop_coroutine();
			}
			void await_resume() noexcept {}
		};
		final_awaiter final_suspend() noexcept { return {}; }
		void return_void() noexcept {}
		void unhandled_exception() noexcept { std::terminate(); }
	};
	when_all_child(std::coroutine_handle<promise_type> c) noexcept : h(c) {}
	when_all_child(when_all_child&& o) noexcept : h(std::exchange(o.h, {})) {}
	when_all_child& operator=(when_all_child&&) = delete;
	~when_all_child() {
		if (h) { h.destroy(); }
	}
	std::coroutine_handle<promise_type> h;
};

template <typename T>
when_all_child run_child(task<T> t, result_t<T>& out, when_all_state& s) {
	try {
		if constexpr (std::is_void_v<T>) {
			co_await std::move(t);
		} else {
			out = co_await std::move(t);
		}
	} catch (...) {
		if (!s.errored.exchange(true)) { s.error = std::current_exception(); }
	}
}

// Awaiting starts every child, then suspends until the last one finishes.
struct when_all_awaiter {
	when_all_state& state;
	when_all_child* children;
	std::size_t n;
	bool await_ready() const noexcept { return n == 0; }
	bool await_suspend(std::coroutine_handle<> parent) {
		state.parent = parent;
		for (std::size_t i = 0; i < n; ++i) {
			auto h = children[i].h;  // the child may finish, and clear its slot, inside resume()
			h.promise().state = &state;
			h.promise().owner = &children[i].h;
			h.resume();
		}
		// The extra count held while starting: the last child may already be done.
		return state.pending.fetch_sub(1, std::memory_order_acq_rel) != 1;
	}
	void await_resume() const noexcept {}
};

}  // namespace detail

// Runs all tasks concurrently (interleaved at their suspension points); returns their
// results in order. The first exception is rethrown after every task has finished.
template <typename T>
task<std::vector<detail::result_t<T>>> when_all(std::vector<task<T>> tasks) {
	std::vector<detail::result_t<T>> results(tasks.size());
	detail::when_all_state state{tasks.size() + 1, {}, {}};
	std::vector<detail::when_all_child> children;
	children.reserve(tasks.size());
	for (std::size_t i = 0; i < tasks.size(); ++i) { children.push_back(detail::run_child(std::move(tasks[i]), results[i], state)); }
	co_await detail::when_all_awaiter{state, children.data(), children.size()};
	if (state.error) { std::rethrow_exception(state.error); }
	co_return results;
}

template <typename... T>
task<std::tuple<detail::result_t<T>...>> when_all(task<T>... tasks) {
	std::tuple<detail::result_t<T>...> results;
	detail::when_all_state state{sizeof...(T) + 1, {}, {}};
	auto children = [&]<std::size_t... I>(std::index_sequence<I...>) {
		return std::array<detail::when_all_child, sizeof...(T)>{detail::run_child(std::move(tasks), std::get<I>(results), state)...};
	}(std::index_sequence_for<T...>{});
	co_await detail::when_all_awaiter{state, children.data(), children.size()};
	if (state.error) { std::rethrow_exception(state.error); }
	co_return results;
}

}  // namespace task
