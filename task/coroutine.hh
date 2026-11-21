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

// task<T> and starting work from outside a coroutine: spawn, run, sync_wait and the root list.

#pragma once

#include "frame_pool.hh"

namespace task {

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

// A self-destroying coroutine that runs one task to completion. A spawned one is a root:
// linked into its scheduler's root_list until it ends or the list destroys it.
struct detached {
	struct promise_type : pooled_frame {
		root_list* list = nullptr;
		promise_type* prev = nullptr;
		promise_type* next = nullptr;
		~promise_type();
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

// The tasks spawned on a scheduler that have not finished, so its owner can destroy the
// ones still suspended at teardown (close(), then destroy_all(), after every loop a root
// may be suspended on has stopped). Linking and unlinking take one uncontended lock.
class root_list {
public:
	root_list() = default;
	root_list(const root_list&) = delete;
	root_list& operator=(const root_list&) = delete;
	~root_list() { assert(!head_); }

	// After close(), spawn destroys the task it is given instead of linking it.
	void close() noexcept {
		std::lock_guard g(m_);
		closed_.store(true, std::memory_order_release);
	}
	bool closed() const noexcept { return closed_.load(std::memory_order_acquire); }
	bool empty() const noexcept {
		std::lock_guard g(m_);
		return head_ == nullptr;
	}

	// Destroys every root, one at a time outside the lock; each destroys its awaited chain.
	void destroy_all() noexcept {
		for (;;) {
			detail::detached::promise_type* r;
			{
				std::lock_guard g(m_);
				r = head_;
				if (!r) { return; }
				head_ = r->next;
				if (head_) { head_->prev = nullptr; }
				r->list = nullptr;
				r->next = nullptr;
			}
			std::coroutine_handle<detail::detached::promise_type>::from_promise(*r).destroy();
		}
	}

	// Links r; false when closed.
	bool link(detail::detached::promise_type* r) noexcept {
		std::lock_guard g(m_);
		if (closed_.load(std::memory_order_relaxed)) { return false; }
		r->list = this;
		r->prev = nullptr;
		r->next = head_;
		if (head_) { head_->prev = r; }
		head_ = r;
		return true;
	}
	void unlink(detail::detached::promise_type* r) noexcept {
		std::lock_guard g(m_);
		(r->prev ? r->prev->next : head_) = r->next;
		if (r->next) { r->next->prev = r->prev; }
		r->list = nullptr;
	}

private:
	mutable std::mutex m_;
	std::atomic<bool> closed_{false};
	detail::detached::promise_type* head_ = nullptr;
};

inline detail::detached::promise_type::~promise_type() {
	if (list) { list->unlink(this); }
}

// Queues t on scheduler s; done(exception_ptr[, optional<T>]) runs when it finishes. On a
// scheduler that owns roots the task is one until it finishes; on a closed one, t and done
// are destroyed at once, unrun, and done is never called.
template <typename T, typename F>
void spawn(scheduler_ref s, task<T> t, F done) {
	auto d = detail::run_then(std::move(t), std::move(done));
	if (root_list* roots = s.roots()) {
		if (!roots->link(&d.h.promise())) {
			d.h.destroy();
			return;
		}
	}
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

}  // namespace task
