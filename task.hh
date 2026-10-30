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

// C++20 coroutine tasks for code that runs on someone else's event loop.
//
// task<T> is lazy and resumes its awaiter through symmetric transfer, or by posting
// to the awaiter's scheduler when it completes on a different one. Schedulers are
// anything with post(handle) and now(); the host's loop is the run queue. Long
// computations bound their time slices with budget checkpoints, which also observe
// cancellation. Frames come from a per-thread pool, so steady-state tasks never
// call malloc.

#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

// Frames cached for reuse stay poisoned under AddressSanitizer, so a use after free of a
// pooled block is reported instead of reading a recycled one.
#if defined(__SANITIZE_ADDRESS__)
#define TASK_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define TASK_ASAN 1
#endif
#endif
#ifdef TASK_ASAN
#include <sanitizer/asan_interface.h>
#define TASK_POISON(p, n) ASAN_POISON_MEMORY_REGION(p, n)
#define TASK_UNPOISON(p, n) ASAN_UNPOISON_MEMORY_REGION(p, n)
#else
#define TASK_POISON(p, n) ((void)(p), (void)(n))
#define TASK_UNPOISON(p, n) ((void)(p), (void)(n))
#endif

namespace task {

// A monotonic cycle counter calibrated to nanoseconds once per process.
namespace cycles {

inline std::uint64_t read() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
	return __rdtsc();
#elif defined(__aarch64__)
	std::uint64_t v;
	asm volatile("mrs %0, cntvct_el0" : "=r"(v));
	return v;
#else
	return std::uint64_t(std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Counter ticks per nanosecond, measured against steady_clock over about 2 ms on first
// use. Hosts call calibrate() at startup so no event loop pays that wait.
inline double per_ns() {
	static const double ratio = [] {
		auto t0 = std::chrono::steady_clock::now();
		auto c0 = read();
		while (std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(2)) {}
		auto c1 = read();
		auto ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t0).count();
		return double(c1 - c0) / ns;
	}();
	return ratio;
}

inline void calibrate() { (void)per_ns(); }

}  // namespace cycles

// ---------------------------------------------------------------------------------------------
// Schedulers

// A scheduler runs posted coroutine handles on its own thread(s), and tells time in
// nanoseconds. post_demoted, when present, queues behind ordinary posts.
template <typename S>
concept scheduler = requires(S& s, std::coroutine_handle<> h) {
	s.post(h);
	{ s.now() } -> std::convertible_to<std::uint64_t>;
};

// A type-erased reference to a scheduler: two pointers and a demotion hook.
class scheduler_ref {
public:
	scheduler_ref() = default;

	template <scheduler S>
	static scheduler_ref of(S& s) noexcept {
		scheduler_ref r;
		r.self_ = &s;
		r.post_ = [](void* p, std::coroutine_handle<> h) { static_cast<S*>(p)->post(h); };
		if constexpr (requires { s.post_demoted(std::coroutine_handle<>{}); }) {
			r.demote_ = [](void* p, std::coroutine_handle<> h) { static_cast<S*>(p)->post_demoted(h); };
		} else {
			r.demote_ = r.post_;
		}
		return r;
	}

	void post(std::coroutine_handle<> h) const { post_(self_, h); }
	void post_demoted(std::coroutine_handle<> h) const { demote_(self_, h); }
	explicit operator bool() const noexcept { return self_ != nullptr; }
	friend bool operator==(const scheduler_ref& a, const scheduler_ref& b) noexcept { return a.self_ == b.self_; }

private:
	void* self_ = nullptr;
	void (*post_)(void*, std::coroutine_handle<>) = nullptr;
	void (*demote_)(void*, std::coroutine_handle<>) = nullptr;
};

namespace detail {
inline thread_local scheduler_ref current_scheduler;
// When the current resume began, in cycle-counter ticks; budgets measure their slice from it.
inline thread_local std::uint64_t slice_start = 0;
}

// The scheduler running on this thread right now (empty outside any scheduler).
inline scheduler_ref current() noexcept { return detail::current_scheduler; }

// Marks code as running on a scheduler; schedulers wrap each resume in one. Entering a scope
// starts a time slice (see budget), and leaving it restores the enclosing scope's.
class running_on {
public:
	explicit running_on(scheduler_ref s) noexcept
		: previous_(detail::current_scheduler), previous_start_(detail::slice_start) {
		detail::current_scheduler = s;
		detail::slice_start = cycles::read();
	}
	~running_on() {
		detail::current_scheduler = previous_;
		detail::slice_start = previous_start_;
	}
	running_on(const running_on&) = delete;
	running_on& operator=(const running_on&) = delete;

private:
	scheduler_ref previous_;
	std::uint64_t previous_start_;
};

class inline_scheduler;

namespace detail {
// The inline_scheduler whose run() is draining on this thread, if any.
inline thread_local inline_scheduler* draining_scheduler = nullptr;
}

// A deterministic single-threaded scheduler: FIFO, demoted handles only when nothing
// else is ready, and a manual clock. For tests and simulation.
class inline_scheduler {
public:
	void post(std::coroutine_handle<> h) { ready_.push_back(h); }
	void post_demoted(std::coroutine_handle<> h) { demoted_.push_back(h); }
	std::uint64_t now() const noexcept { return now_; }
	void advance(std::uint64_t ns) noexcept { now_ += ns; }

	// Runs until no handle is queued; returns how many ran.
	std::size_t run() {
		return run_until([] { return false; });
	}

	// Like run(), but stops as soon as stop() is true, checked before each handle.
	template <typename P>
	std::size_t run_until(P&& stop) {
		running_on scope(scheduler_ref::of(*this));
		struct mark {
			inline_scheduler* previous = detail::draining_scheduler;
			~mark() { detail::draining_scheduler = previous; }
		} marker;
		detail::draining_scheduler = this;
		std::size_t n = 0;
		while (!stop() && (!ready_.empty() || !demoted_.empty())) {
			auto& queue = ready_.empty() ? demoted_ : ready_;
			auto h = queue.front();
			queue.pop_front();
			detail::slice_start = cycles::read();  // each resume is its own slice
			h.resume();
			++n;
		}
		return n;
	}

	std::size_t queued() const noexcept { return ready_.size() + demoted_.size(); }

private:
	std::deque<std::coroutine_handle<>> ready_, demoted_;
	std::uint64_t now_ = 0;
};

// ---------------------------------------------------------------------------------------------
// Frame allocation

namespace detail {

// Per-thread free lists in 64-byte size classes up to 2 KiB; larger frames use operator new.
// A frame freed on another thread joins that thread's lists, each class holding at most
// `cap` frames so one-way cross-thread flows cannot grow a list without bound.
class frame_pool {
public:
	static constexpr std::size_t granularity = 64, classes = 32, cap = 1024;
	static constexpr std::size_t poison_from = 16;  // the free-list link stays readable

	~frame_pool() {
		for (std::size_t c = 0; c < classes; ++c) {
			while (node* p = free_[c]) {
				TASK_UNPOISON(p, (c + 1) * granularity);
				free_[c] = p->next;
				::operator delete(p);
			}
		}
	}

	void* allocate(std::size_t n) {
		std::size_t c = (n + granularity - 1) / granularity;
		if (c > classes) { return ::operator new(n); }
		if (node* p = free_[c - 1]) {
			TASK_UNPOISON(p, c * granularity);
			free_[c - 1] = p->next;
			--count_[c - 1];
			return p;
		}
		return ::operator new(c * granularity);
	}

	void deallocate(void* p, std::size_t n) noexcept {
		std::size_t c = (n + granularity - 1) / granularity;
		if (c > classes) {
			::operator delete(p);
			return;
		}
		if (count_[c - 1] >= cap) {
			::operator delete(p);
			return;
		}
		auto* f = static_cast<node*>(p);
		f->next = free_[c - 1];
		free_[c - 1] = f;
		++count_[c - 1];
		TASK_POISON(static_cast<char*>(p) + poison_from, c * granularity - poison_from);
	}

	// Frames held across all classes.
	std::size_t cached() const noexcept {
		std::size_t n = 0;
		for (auto c : count_) { n += c; }
		return n;
	}

private:
	struct node { node* next; };
	node* free_[classes] = {};
	std::uint32_t count_[classes] = {};
};

inline frame_pool& pool() {
	thread_local frame_pool p;
	return p;
}

struct pooled_frame {
	static void* operator new(std::size_t n) { return pool().allocate(n); }
	static void operator delete(void* p, std::size_t n) noexcept { pool().deallocate(p, n); }
};

}  // namespace detail

// ---------------------------------------------------------------------------------------------
// Transfers between coroutines

// Symmetric transfer keeps chains of synchronous completions in constant stack only when
// the compiler turns the resume into a tail call. Clang always does; GCC does only when
// optimizing without sanitizers. Elsewhere, transfers go through a per-thread trampoline
// that resumes queued handles in a loop instead of recursing.
#ifndef TASK_SYMMETRIC_TRANSFER
#if defined(__clang__)
#define TASK_SYMMETRIC_TRANSFER 1
#else
#define TASK_SYMMETRIC_TRANSFER 0
#endif
#endif

namespace detail {

#if !TASK_SYMMETRIC_TRANSFER
struct trampoline {
	std::vector<std::coroutine_handle<>> queue;
	std::size_t head = 0;
	bool active = false;
};

inline trampoline& local_trampoline() {
	thread_local trampoline t;
	return t;
}
#endif

// What an await_suspend returns to continue with h without growing the stack.
inline std::coroutine_handle<> transfer(std::coroutine_handle<> h) {
#if TASK_SYMMETRIC_TRANSFER
	return h;
#else
	trampoline& t = local_trampoline();
	t.queue.push_back(h);
	if (t.active) { return std::noop_coroutine(); }  // the running loop resumes it
	t.active = true;
	while (t.head < t.queue.size()) { t.queue[t.head++].resume(); }
	t.queue.clear();
	t.head = 0;
	t.active = false;
	return std::noop_coroutine();
#endif
}

}  // namespace detail

// ---------------------------------------------------------------------------------------------
// task<T>

template <typename T = void>
class task;

namespace detail {

struct promise_base : pooled_frame {
	std::coroutine_handle<> continuation;
	scheduler_ref continuation_scheduler;
	std::exception_ptr error;

	std::suspend_always initial_suspend() noexcept { return {}; }

	struct final_awaiter {
		bool await_ready() noexcept { return false; }
		template <typename P>
		std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
			promise_base& p = h.promise();
			if (!p.continuation) { return std::noop_coroutine(); }
			// Same scheduler (or none recorded): transfer directly; otherwise go home first.
			if (!p.continuation_scheduler || p.continuation_scheduler == current()) { return transfer(p.continuation); }
			p.continuation_scheduler.post(p.continuation);
			return std::noop_coroutine();
		}
		void await_resume() noexcept {}
	};
	final_awaiter final_suspend() noexcept { return {}; }
	void unhandled_exception() noexcept { error = std::current_exception(); }
};

template <typename T>
struct promise : promise_base {
	std::optional<T> value;
	task<T> get_return_object() noexcept;
	template <typename U>
	void return_value(U&& v) { value.emplace(std::forward<U>(v)); }
	T take() {
		if (error) { std::rethrow_exception(error); }
		return std::move(*value);
	}
};

template <>
struct promise<void> : promise_base {
	task<void> get_return_object() noexcept;
	void return_void() noexcept {}
	void take() {
		if (error) { std::rethrow_exception(error); }
	}
};

}  // namespace detail

// A lazily started coroutine producing T. Awaiting it runs it; the awaiter resumes
// on its own scheduler with the value or the exception.
template <typename T>
class [[nodiscard]] task {
public:
	using promise_type = detail::promise<T>;
	using value_type = T;

	task() = default;
	explicit task(std::coroutine_handle<promise_type> h) noexcept : h_(h) {}
	task(task&& o) noexcept : h_(std::exchange(o.h_, {})) {}
	task& operator=(task&& o) noexcept {
		if (this != &o) {
			if (h_) { h_.destroy(); }
			h_ = std::exchange(o.h_, {});
		}
		return *this;
	}
	task(const task&) = delete;
	task& operator=(const task&) = delete;
	~task() {
		if (h_) { h_.destroy(); }
	}

	bool valid() const noexcept { return bool(h_); }
	bool done() const noexcept { return h_ && h_.done(); }

	auto operator co_await() && noexcept {
		struct awaiter {
			std::coroutine_handle<promise_type> h;
			bool await_ready() const noexcept { return false; }
			std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
				h.promise().continuation = awaiting;
				h.promise().continuation_scheduler = current();
				return detail::transfer(h);
			}
			T await_resume() { return h.promise().take(); }
		};
		return awaiter{h_};
	}

	std::coroutine_handle<promise_type> release() noexcept { return std::exchange(h_, {}); }
	std::coroutine_handle<promise_type> handle() const noexcept { return h_; }

private:
	std::coroutine_handle<promise_type> h_;
};

namespace detail {
template <typename T>
task<T> promise<T>::get_return_object() noexcept { return task<T>{std::coroutine_handle<promise<T>>::from_promise(*this)}; }
inline task<void> promise<void>::get_return_object() noexcept { return task<void>{std::coroutine_handle<promise<void>>::from_promise(*this)}; }
}  // namespace detail

// ---------------------------------------------------------------------------------------------
// Starting work from outside a coroutine

namespace detail {

// A self-destroying coroutine that runs one task to completion.
struct detached {
	struct promise_type : pooled_frame {
		detached get_return_object() noexcept { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
		std::suspend_always initial_suspend() noexcept { return {}; }
		std::suspend_never final_suspend() noexcept { return {}; }
		void return_void() noexcept {}
		void unhandled_exception() noexcept { std::terminate(); }
	};
	std::coroutine_handle<promise_type> h;
};

template <typename T, typename F>
detached run_then(task<T> t, F done) {
	if constexpr (std::is_void_v<T>) {
		std::exception_ptr e;
		try { co_await std::move(t); } catch (...) { e = std::current_exception(); }
		done(e);
	} else {
		std::exception_ptr e;
		std::optional<T> v;
		try { v.emplace(co_await std::move(t)); } catch (...) { e = std::current_exception(); }
		done(e, std::move(v));
	}
}

}  // namespace detail

// Queues t on scheduler s; done(exception_ptr[, optional<T>]) runs when it finishes.
template <typename T, typename F>
void spawn(scheduler_ref s, task<T> t, F done) {
	auto d = detail::run_then(std::move(t), std::move(done));
	s.post(d.h);
}

// Fire and forget: an escaping exception terminates the program.
inline void spawn(scheduler_ref s, task<void> t) {
	spawn(s, std::move(t), [](std::exception_ptr e) {
		if (e) { std::terminate(); }
	});
}

// Runs t to completion on an inline scheduler and returns its result (tests, tools).
template <typename T>
T run(inline_scheduler& s, task<T> t) {
	std::exception_ptr error;
	if constexpr (std::is_void_v<T>) {
		spawn(scheduler_ref::of(s), std::move(t), [&](std::exception_ptr e) { error = e; });
		s.run();
		if (error) { std::rethrow_exception(error); }
	} else {
		std::optional<T> result;
		spawn(scheduler_ref::of(s), std::move(t), [&](std::exception_ptr e, std::optional<T> v) {
			error = e;
			result = std::move(v);
		});
		s.run();
		if (error) { std::rethrow_exception(error); }
		return std::move(*result);
	}
}

// Blocks the calling thread until t completes on whatever scheduler it resumes on, then
// returns its value or rethrows its exception. It must never run on a thread the awaited
// work needs (a reactor, or a pool's only worker): that deadlocks. t starts on the calling
// thread with no current scheduler, so it resumes its awaiter wherever it finishes; when
// the caller is draining an inline_scheduler, only that scheduler is run (queued handles
// ahead of the task, nested, until it finishes). T needs only to be move-constructible.
template <typename T>
T sync_wait(task<T> t) {
	using result = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
	struct state {
		std::mutex mutex;
		std::condition_variable cv;
		bool done = false;
		std::exception_ptr error;
		std::optional<result> value;
	} st;
	auto drive = [](task<T> work, state& s) -> detail::detached {
		try {
			task<T> mine = std::move(work);  // destroyed before the caller wakes
			if constexpr (std::is_void_v<T>) {
				co_await std::move(mine);
				s.value.emplace();
			} else {
				s.value.emplace(co_await std::move(mine));
			}
		} catch (...) {
			s.error = std::current_exception();
		}
		// Only the caller reads error and value, after taking this lock.
		std::lock_guard lock(s.mutex);
		s.done = true;
		s.cv.notify_one();
	};
	{
#if !TASK_SYMMETRIC_TRANSFER
		// Inside a trampoline loop, a transfer only queues; run this one on a fresh loop.
		detail::trampoline outer;
		detail::trampoline& local = detail::local_trampoline();
		const bool nested = local.active;
		if (nested) { std::swap(local, outer); }
		struct restore {
			detail::trampoline& local;
			detail::trampoline& outer;
			bool nested;
			~restore() { if (nested) { std::swap(local, outer); } }
		} guard{local, outer, nested};
#endif
		auto d = drive(std::move(t), st);
		running_on scope{scheduler_ref{}};
		d.h.resume();  // d may already be gone when this returns
		if (auto* s = detail::draining_scheduler) {
			s->run_until([&] {
				std::lock_guard lock(st.mutex);
				return st.done;
			});
		}
	}
	std::unique_lock lock(st.mutex);
	st.cv.wait(lock, [&] { return st.done; });
	if (st.error) { std::rethrow_exception(st.error); }
	if constexpr (!std::is_void_v<T>) { return std::move(*st.value); }
}

// ---------------------------------------------------------------------------------------------
// Yielding, budgets, and cancellation

// Suspends and requeues the current coroutine on the current scheduler.
struct yield_awaiter {
	bool demote = false;
	bool await_ready() const noexcept { return !current(); }
	void await_suspend(std::coroutine_handle<> h) const {
		demote ? current().post_demoted(h) : current().post(h);
	}
	void await_resume() const noexcept {}
};
inline yield_awaiter yield() noexcept { return {false}; }

// Moves the current coroutine to scheduler s (it resumes from s's queue).
struct schedule_on {
	scheduler_ref target;
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) const { target.post(h); }
	void await_resume() const noexcept {}
};

struct cancelled : std::exception {
	const char* what() const noexcept override { return "task cancelled"; }
};

class cancel_token {
public:
	cancel_token() = default;
	bool requested() const noexcept { return flag_ && flag_->load(std::memory_order_relaxed); }

private:
	friend class cancel_source;
	explicit cancel_token(std::shared_ptr<std::atomic<bool>> f) noexcept : flag_(std::move(f)) {}
	std::shared_ptr<std::atomic<bool>> flag_;
};

class cancel_source {
public:
	cancel_source() : flag_(std::make_shared<std::atomic<bool>>(false)) {}
	cancel_token token() const noexcept { return cancel_token(flag_); }
	void request() noexcept { flag_->store(true, std::memory_order_relaxed); }
	bool requested() const noexcept { return flag_->load(std::memory_order_relaxed); }

private:
	std::shared_ptr<std::atomic<bool>> flag_;
};

// A time slice. checkpoint() is free while the slice lasts; once it is spent, it
// requeues the coroutine as demoted. Both throw cancelled when the token is set.
// On a scheduler the slice is the current resume: time spent suspended does not count,
// and a task resumed after an event, a lock or a completion starts a fresh slice. Off a
// scheduler it runs from construction, or from the last demotion.
class budget {
public:
	explicit budget(std::chrono::nanoseconds slice, cancel_token token = {})
		: slice_(std::uint64_t(double(slice.count()) * cycles::per_ns())), deadline_(cycles::read() + slice_), token_(std::move(token)) {}

	bool exhausted() const noexcept {
		return cycles::read() >= (current() ? detail::slice_start + slice_ : deadline_);
	}
	const cancel_token& token() const noexcept { return token_; }

	auto checkpoint() noexcept {
		struct awaiter {
			budget& b;
			bool await_ready() const noexcept { return b.token_.requested() || !b.exhausted() || !current(); }
			void await_suspend(std::coroutine_handle<> h) const { current().post_demoted(h); }
			void await_resume() const {
				if (b.token_.requested()) { throw cancelled{}; }
				if (b.exhausted()) { b.deadline_ = cycles::read() + b.slice_; }
			}
		};
		return awaiter{*this};
	}

private:
	std::uint64_t slice_;
	std::uint64_t deadline_;
	cancel_token token_;
};

// ---------------------------------------------------------------------------------------------
// Events and locks

// Resumes h on s, or on the calling thread when no scheduler was recorded.
namespace detail {
inline void resume_on(scheduler_ref s, std::coroutine_handle<> h) {
	if (s) { s.post(h); } else { h.resume(); }
}
}  // namespace detail

// A one-shot completion. Awaiting it suspends until set(); each waiter resumes through the
// scheduler it suspended on, in the order it began waiting, so a waiter never runs inside
// set(). set() is idempotent and callable from any thread. Waiters must run to completion:
// the event's destructor asserts that none is left.
class event {
public:
	event() = default;
	event(const event&) = delete;
	event& operator=(const event&) = delete;
	~event() { assert(head_.load(std::memory_order_relaxed) == set_marker() || head_.load(std::memory_order_relaxed) == nullptr); }

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
			detail::resume_on(n->sched, n->handle);
		}
	}

	struct awaiter {
		event& e;
		std::coroutine_handle<> handle;
		scheduler_ref sched;
		awaiter* next = nullptr;
		bool await_ready() const noexcept { return e.is_set(); }
		bool await_suspend(std::coroutine_handle<> h) noexcept {
			handle = h;
			sched = current();
			node* head = e.head_.load(std::memory_order_acquire);
			do {
				if (head == set_marker()) { return false; }
				next = head;
			} while (!e.head_.compare_exchange_weak(head, this, std::memory_order_acq_rel, std::memory_order_acquire));
			return true;  // set() may already be resuming this frame: touch nothing more
		}
		void await_resume() const noexcept {}
	};
	awaiter operator co_await() noexcept { return awaiter{*this, {}, {}}; }

private:
	using node = awaiter;
	static node* set_marker() noexcept { return reinterpret_cast<node*>(std::uintptr_t(1)); }
	std::atomic<node*> head_{nullptr};
};

class async_mutex;

namespace detail {

// One queued lock request. The queue entry, the awaiter in the waiting frame, and a timer
// handler each hold a reference, so whichever ends last frees it: a frame that ends after
// a timeout, or a timer that fires after a grant, never meets freed state. Exactly one of
// grant and timeout settles it.
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

// What a timer callback does when its deadline passes. It owns one reference, dropped
// whether or not it ever runs.
class lock_timeout {
public:
	explicit lock_timeout(lock_waiter* w) noexcept : w_(w) { w_->retain(); }
	lock_timeout(lock_timeout&& o) noexcept : w_(std::exchange(o.w_, nullptr)) {}
	lock_timeout(const lock_timeout&) = delete;
	lock_timeout& operator=(const lock_timeout&) = delete;
	~lock_timeout() {
		if (w_) { w_->release(); }
	}
	void operator()() noexcept {
		if (w_->settle(lock_waiter::timed_out)) { w_->wake(); }
	}

private:
	lock_waiter* w_;
};

}  // namespace detail

// A FIFO mutex for coroutines, usable from every scheduler at once. A waiter suspends
// instead of blocking and resumes through the scheduler it suspended on (on the unlocking
// thread when it had none) when the holder unlocks, which hands the lock over without
// releasing it, so a late arrival cannot barge in. The holder need not be the coroutine's thread: a guard may be released anywhere.
// Internally a short mutex guards a queue; no critical section waits or does I/O.
// The destructor asserts that the mutex is unlocked and nobody is waiting.
class async_mutex {
public:
	// Owns the lock until unlock() or destruction; empty after a timeout.
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
			if (w_) { w_->release(); }
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

	// Takes the lock, waiting its turn: `auto g = co_await m.lock();`
	[[nodiscard]] lock_awaiter lock() noexcept { return lock_awaiter{*this}; }

	// lock() with a deadline: the guard is empty if the lock was not granted by then. S is
	// the scheduler the task resumes on; it also starts the deadline timer:
	// `start_timer(steady_clock::time_point, F&&)` runs F once at the deadline on S (not at
	// all when the returned timer is destroyed first), and `now()` tells time in nanoseconds
	// on the same clock. A lock that is free is taken whatever the deadline.
	template <typename S>
	class timed_awaiter {
	public:
		timed_awaiter(async_mutex& m, S& s, std::chrono::steady_clock::time_point deadline) noexcept : m_(m), s_(s), deadline_(deadline) {}
		timed_awaiter(const timed_awaiter&) = delete;
		timed_awaiter& operator=(const timed_awaiter&) = delete;
		~timed_awaiter() {
			if (w_) { w_->release(); }
		}
		bool await_ready() noexcept { return m_.try_lock(); }
		bool await_suspend(std::coroutine_handle<> h) {
			w_ = m_.enqueue(h, scheduler_ref::of(s_));
			if (!w_) { return false; }
			try {
				timer_.emplace(s_.start_timer(deadline_, detail::lock_timeout{w_}));
			} catch (...) {
				if (!w_->settle(detail::lock_waiter::timed_out)) { m_.unlock(); }  // granted meanwhile: give it back
				throw;
			}
			return w_->park();
		}
		guard await_resume() noexcept {
			timer_.reset();
			if (!w_) { return guard{&m_}; }
			const bool got = w_->state.load(std::memory_order_acquire) == detail::lock_waiter::granted;
			std::exchange(w_, nullptr)->release();
			return got ? guard{&m_} : guard{};
		}

	private:
		async_mutex& m_;
		S& s_;
		std::chrono::steady_clock::time_point deadline_;
		detail::lock_waiter* w_ = nullptr;
		std::optional<decltype(std::declval<S&>().start_timer(std::chrono::steady_clock::time_point{}, std::declval<detail::lock_timeout>()))> timer_;
	};

	template <typename S>
	[[nodiscard]] timed_awaiter<S> lock_until(S& s, std::chrono::steady_clock::time_point deadline) noexcept {
		return timed_awaiter<S>{*this, s, deadline};
	}
	template <typename S>
	[[nodiscard]] timed_awaiter<S> lock_for(S& s, std::chrono::nanoseconds timeout) noexcept {
		using clock = std::chrono::steady_clock;
		using ns = std::chrono::nanoseconds;
		const ns now{s.now()};
		const ns at = timeout > ns::max() - now ? ns::max() : now + timeout;  // saturates: no signed overflow
		return lock_until(s, clock::time_point{std::chrono::duration_cast<clock::duration>(at)});
	}

private:
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

struct when_all_child {
	struct promise_type : pooled_frame {
		when_all_state* state = nullptr;
		when_all_child get_return_object() noexcept { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
		std::suspend_always initial_suspend() noexcept { return {}; }
		struct final_awaiter {
			bool await_ready() noexcept { return false; }
			std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
				when_all_state* s = h.promise().state;
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
	std::vector<when_all_child>& children;
	bool await_ready() const noexcept { return children.empty(); }
	bool await_suspend(std::coroutine_handle<> parent) {
		state.parent = parent;
		for (auto& c : children) {
			c.h.promise().state = &state;
			c.h.resume();
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
	co_await detail::when_all_awaiter{state, children};
	if (state.error) { std::rethrow_exception(state.error); }
	co_return results;
}

template <typename... T>
task<std::tuple<detail::result_t<T>...>> when_all(task<T>... tasks) {
	std::tuple<detail::result_t<T>...> results;
	detail::when_all_state state{sizeof...(T) + 1, {}, {}};
	std::vector<detail::when_all_child> children;
	children.reserve(sizeof...(T));
	[&]<std::size_t... I>(std::index_sequence<I...>) {
		(children.push_back(detail::run_child(std::move(tasks), std::get<I>(results), state)), ...);
	}(std::index_sequence_for<T...>{});
	co_await detail::when_all_awaiter{state, children};
	if (state.error) { std::rethrow_exception(state.error); }
	co_return results;
}

}  // namespace task
