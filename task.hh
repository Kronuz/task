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
#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
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

namespace task {

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
}

// The scheduler running on this thread right now (empty outside any scheduler).
inline scheduler_ref current() noexcept { return detail::current_scheduler; }

// Marks code as running on a scheduler; schedulers wrap each resume in one.
class running_on {
public:
	explicit running_on(scheduler_ref s) noexcept : previous_(detail::current_scheduler) { detail::current_scheduler = s; }
	~running_on() { detail::current_scheduler = previous_; }
	running_on(const running_on&) = delete;
	running_on& operator=(const running_on&) = delete;

private:
	scheduler_ref previous_;
};

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
		running_on scope(scheduler_ref::of(*this));
		std::size_t n = 0;
		while (!ready_.empty() || !demoted_.empty()) {
			auto& queue = ready_.empty() ? demoted_ : ready_;
			auto h = queue.front();
			queue.pop_front();
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
// A frame freed on another thread joins that thread's lists.
class frame_pool {
public:
	static constexpr std::size_t granularity = 64, classes = 32;

	~frame_pool() {
		for (auto*& head : free_) {
			while (head) { ::operator delete(std::exchange(head, head->next)); }
		}
	}

	void* allocate(std::size_t n) {
		std::size_t c = (n + granularity - 1) / granularity;
		if (c > classes) { return ::operator new(n); }
		if (node* p = free_[c - 1]) {
			free_[c - 1] = p->next;
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
		auto* f = static_cast<node*>(p);
		f->next = free_[c - 1];
		free_[c - 1] = f;
	}

private:
	struct node { node* next; };
	node* free_[classes] = {};
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
			if (!p.continuation_scheduler || p.continuation_scheduler == current()) { return p.continuation; }
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
				return h;
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

// Counter ticks per nanosecond, measured against steady_clock over about 2 ms.
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

}  // namespace cycles

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
// requeues the coroutine as demoted and starts a new slice. Both throw cancelled
// when the token is set.
class budget {
public:
	explicit budget(std::chrono::nanoseconds slice, cancel_token token = {})
		: slice_(std::uint64_t(double(slice.count()) * cycles::per_ns())), deadline_(cycles::read() + slice_), token_(std::move(token)) {}

	bool exhausted() const noexcept { return cycles::read() >= deadline_; }
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
				if (s->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) { return s->parent; }
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
