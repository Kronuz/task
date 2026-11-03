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

// Binds task<T> to standalone Asio: a scheduler over an Asio executor that owns its spawned
// tasks as roots, and await_asio(), which awaits an Asio operation with an optional token.

#pragma once

#include "task.hh"

#include <asio/any_io_executor.hpp>
#include <asio/bind_cancellation_slot.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <atomic>
#include <cassert>
#include <exception>
#include <optional>
#include <tuple>
#include <type_traits>

namespace task {

// Runs handles on an Asio executor. Demoted handles wait in a local queue that a
// self-reposting drainer empties one handle per pass, so ordinary posts that arrive
// meanwhile run first. All calls happen on the executor's thread except post(),
// post_call(), demoted(), and the teardown calls. Spawned tasks are roots until they end.
//
// Teardown, across every scheduler and pool a root may be suspended on: (1) stop and join
// every loop; (2) close() every scheduler; (3) destroy_roots() on every one, with every
// io_context alive; (4) destroy the schedulers, then the io_contexts. close() and
// destroy_roots() are idempotent and run on the owner's thread after (1), never on a loop
// thread; the destructor repeats them. The io_context is not run again after (3): what it
// still holds is dropped unrun when closed, but the owner flushes nothing by running it.
class asio_scheduler {
public:
	explicit asio_scheduler(asio::any_io_executor executor)
		: executor_(std::move(executor)), io_(executor_.target<asio::io_context::executor_type>()) {}
	~asio_scheduler() {
		close();
		destroy_roots();
	}
	asio_scheduler(const asio_scheduler&) = delete;
	asio_scheduler& operator=(const asio_scheduler&) = delete;

	void post(std::coroutine_handle<> h) {
		if (closed()) { return; }
		asio::post(executor_, [this, h] {
			if (closed()) { return; }  // the frame may be gone: roots were destroyed after this was queued
			running_on scope(scheduler_ref::of(*this));
			h.resume();
		});
	}

	void post_demoted(std::coroutine_handle<> h) {
		if (closed()) { return; }
		demoted_.push_back(h);
		demoted_count_.fetch_add(1, std::memory_order_relaxed);
		if (!draining_) {
			draining_ = true;
			asio::post(executor_, [this] { drain(); });
		}
	}

	// Runs fn(arg) on the executor; dropped when closed, destroyed unrun with a stopped loop.
	void post_call(void (*fn)(void*), void* arg) {
		if (closed()) { return; }
		asio::post(executor_, [this, fn, arg] {
			if (closed()) { return; }  // arg may belong to a destroyed frame
			running_on scope(scheduler_ref::of(*this));
			fn(arg);
		});
	}

	// True on a thread running the io_context; false for other executor types.
	bool runs_here() const noexcept { return io_ && io_->running_in_this_thread(); }

	root_list* roots() noexcept { return &roots_; }

	// Phase 2: post, post_demoted and post_call drop what they are given; spawn destroys its task.
	void close() noexcept {
		assert(!runs_here());  // phases 2 and 3 run after the loops joined
		roots_.close();
	}
	bool closed() const noexcept { return roots_.closed(); }
	// Phase 3: destroys every root, then forgets the demoted handles (they were the roots').
	void destroy_roots() noexcept {
		assert(!runs_here());
		assert(roots_.empty() || !io_ || io_->context().stopped());  // live roots need a stopped loop
		roots_.destroy_all();
		demoted_.clear();
		demoted_count_.store(0, std::memory_order_relaxed);
	}

	std::uint64_t now() const noexcept {
		return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
	}

	// Handles waiting in the demoted queue; readable from any thread (admission, metrics).
	std::size_t demoted() const noexcept { return demoted_count_.load(std::memory_order_relaxed); }

	// Runs on_fire() on the executor at the deadline. Destroying the returned timer before
	// the deadline destroys on_fire unrun, but an expiry Asio has already queued still runs
	// it, so on_fire must hold what it touches and check whether it is still wanted, as
	// task::deadline's disarm bit does.
	template <typename F>
	asio::steady_timer start_timer(std::chrono::steady_clock::time_point deadline, F&& on_fire) {
		asio::steady_timer timer(executor_, deadline);
		timer.async_wait([f = std::forward<F>(on_fire)](const asio::error_code& ec) mutable {
			if (!ec) { f(); }
		});
		return timer;
	}

	const asio::any_io_executor& executor() const noexcept { return executor_; }

private:
	void drain() {
		if (demoted_.empty()) {  // destroy_roots() forgot them
			draining_ = false;
			return;
		}
		auto h = demoted_.front();
		demoted_.pop_front();
		demoted_count_.fetch_sub(1, std::memory_order_relaxed);
		{
			running_on scope(scheduler_ref::of(*this));
			h.resume();
		}
		if (demoted_.empty()) {
			draining_ = false;
		} else {
			asio::post(executor_, [this] { drain(); });
		}
	}

	asio::any_io_executor executor_;
	const asio::io_context::executor_type* io_;  // into executor_; null for other executor types
	root_list roots_;
	std::deque<std::coroutine_handle<>> demoted_;
	std::atomic<std::size_t> demoted_count_{0};
	bool draining_ = false;
};

namespace detail {

template <typename... A>
struct asio_result {
	using type = std::tuple<A...>;
	static type take(std::tuple<A...>& t) { return std::move(t); }
};
template <>
struct asio_result<> {
	using type = void;
};
template <typename A>
struct asio_result<A> {
	using type = A;
	static A take(std::tuple<A>& t) { return std::move(std::get<0>(t)); }
};

// How many inline resumes nest on this thread, and the bound past which a resume is posted.
inline constexpr int inline_resume_bound = 4;
inline thread_local int inline_resume_depth = 0;

// One resume count: await_suspend's park and the handler hold one each, and a cancel posted
// from another thread adds one. Whoever drops it to zero resumes the frame; await_suspend
// dropping last continues in place, so a handler run inside initiation never resumes it.
struct asio_park {
	std::atomic<int> count_{2};
	std::coroutine_handle<> h_;
	scheduler_ref sched_;
	void drop() {
		if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) { resume(sched_, h_); }
	}
	bool park() noexcept { return count_.fetch_sub(1, std::memory_order_acq_rel) != 1; }

	// Inline on the scheduler's own thread, in its scope (a fresh slice), when no other
	// scheduler is current and fewer than inline_resume_bound inline resumes nest; else posted.
	static void resume(scheduler_ref s, std::coroutine_handle<> h) {
		if (inline_resume_depth < inline_resume_bound && s.runs_here() && (!current() || current() == s)) {
			struct nest {
				nest() noexcept { ++inline_resume_depth; }
				~nest() { --inline_resume_depth; }
			} n;
			running_on scope(s);
			h.resume();
		} else {
			resume_on(s, h);
		}
	}
};

// How await_asio returns an argument R after an error_code, with or without a token: R itself when
// it can be value-initialized (the result of a request that came before the operation started),
// else a std::optional<R>, empty then. One signature has one result type in both overloads.
template <typename R>
using abort_arg_t = std::conditional_t<std::is_default_constructible_v<R>, R, std::optional<R>>;

template <typename... A>
struct result_args {
	using type = asio_result<A...>;
	using tuple = std::tuple<A...>;
};
template <typename... R>
struct result_args<asio::error_code, R...> {
	using type = asio_result<asio::error_code, abort_arg_t<R>...>;
	using tuple = std::tuple<asio::error_code, abort_arg_t<R>...>;
};

template <typename Sig, typename Init>
class asio_awaiter;

template <typename... A, typename Init>
class asio_awaiter<void(A...), Init> : asio_park {
public:
	explicit asio_awaiter(Init init) : init_(std::move(init)) {}
	asio_awaiter(const asio_awaiter&) = delete;
	asio_awaiter& operator=(const asio_awaiter&) = delete;

	bool await_ready() const noexcept { return false; }
	bool await_suspend(std::coroutine_handle<> h) {
		h_ = h;
		sched_ = current();
		init_(handler{this});  // a throw means no handler runs; it resumes the frame with it
		return park();
	}
	typename result_args<A...>::type::type await_resume() {
		if constexpr (sizeof...(A) > 0) { return result_args<A...>::type::take(*result_); }
	}

private:
	struct handler {
		asio_awaiter* self;
		void operator()(A... a) {
			self->result_.emplace(std::move(a)...);
			self->drop();  // nothing touches *self after this
		}
	};
	Init init_;
	std::optional<typename result_args<A...>::tuple> result_;
};

template <typename>
inline constexpr bool always_false = false;

template <typename Sig, typename Init>
class cancellable_asio_awaiter {
	static_assert(always_false<Sig>, "await_asio with a token needs a signature beginning with asio::error_code");
};

// await_asio with a token. The callback is registered before the operation starts, a
// requested token never starts it, and a cancel ends it through a cancellation slot:
// emitted on the loop, or by a function posted there when requested from another thread.
template <typename... R, typename Init>
class cancellable_asio_awaiter<void(asio::error_code, R...), Init> : asio_park {
public:
	cancellable_asio_awaiter(Init init, cancel_token token) : init_(std::move(init)), token_(std::move(token)) {}
	cancellable_asio_awaiter(const cancellable_asio_awaiter&) = delete;
	cancellable_asio_awaiter& operator=(const cancellable_asio_awaiter&) = delete;
	~cancellable_asio_awaiter() { cb_.reset(); }  // waits for a callback running elsewhere

	bool await_ready() {
		if (!token_.requested()) { return false; }
		result_.emplace(asio::error::operation_aborted, abort_arg_t<R>{}...);
		return true;
	}

	bool await_suspend(std::coroutine_handle<> h) {
		h_ = h;
		sched_ = current();
		if (detail::token_empty(token_)) {  // nothing can cancel it: no slot, no node
			started_ = true;
			init_(handler{this});
			return park();
		}
#ifndef NDEBUG
		on_loop_ = sched_.runs_here();
#endif
		cb_.emplace(token_, on_cancel{this});
		TASK_TEST_HOOK(asio_registered);
		if (token_.requested()) {
			// Requested in between: initiate nothing. After the reset no callback starts, so
			// a posted emit's count is the last; give up the handler's and suspend on it.
			cb_.reset();
			result_.emplace(asio::error::operation_aborted, abort_arg_t<R>{}...);
			count_.fetch_sub(1, std::memory_order_relaxed);
			return park();
		}
		try {
			init_(asio::bind_cancellation_slot(signal_.slot(), handler{this}));
		} catch (...) {
			cb_.reset();
			error_ = std::current_exception();
			if (!completed_) { count_.fetch_sub(1, std::memory_order_relaxed); }
			return park();  // await_resume rethrows, after a posted emit if one is pending
		}
		started_ = true;
		if (pending_) { signal_.emit(asio::cancellation_type::all); }  // requested during initiation
		return park();
	}

	typename asio_result<asio::error_code, abort_arg_t<R>...>::type await_resume() {
		if (error_) { std::rethrow_exception(error_); }
		return asio_result<asio::error_code, abort_arg_t<R>...>::take(*result_);
	}

private:
	struct handler {
		cancellable_asio_awaiter* self;
		void operator()(asio::error_code ec, R... r) {
#ifndef NDEBUG
			assert(!self->on_loop_ || self->sched_.runs_here());  // token-bound operations are loop-local
#endif
			self->result_.emplace(ec, std::move(r)...);
			self->completed_ = true;
			self->cb_.reset();  // waits for a callback on another thread; none starts after it
			self->drop();
		}
	};

	struct on_cancel {
		cancellable_asio_awaiter* self;
		void operator()() const { self->cancel(); }
	};

	void cancel() {
		if (sched_.runs_here()) {
			if (started_) {
				signal_.emit(asio::cancellation_type::all);
			} else {
				pending_ = true;  // await_suspend emits once the slot holds the operation
			}
			return;
		}
		count_.fetch_add(1, std::memory_order_relaxed);
		bool posted = false;
		[[maybe_unused]] bool threw = false;
		try {
			posted = sched_.post_call(&emit_posted, this);
		} catch (...) {
			threw = true;
		}
		// No post_call (or no current scheduler) means no cross-thread cancel (D7); say so in Debug.
		assert((posted || threw) && "a cross-thread cancel of a token-bound await needs a scheduler with post_call");
		if (!posted) { count_.fetch_sub(1, std::memory_order_relaxed); }
	}

	static void emit_posted(void* p) {
		auto* self = static_cast<cancellable_asio_awaiter*>(p);
		self->signal_.emit(asio::cancellation_type::all);
		self->drop();
	}

	Init init_;
	cancel_token token_;
	std::optional<std::tuple<asio::error_code, abort_arg_t<R>...>> result_;
	std::exception_ptr error_;
	asio::cancellation_signal signal_;
	bool started_ = false;    // the loop's thread only
	bool pending_ = false;    // the loop's thread only
	bool completed_ = false;  // the loop's thread only
#ifndef NDEBUG
	bool on_loop_ = false;
#endif
	std::optional<cancel_callback<on_cancel>> cb_;
};

}  // namespace detail

// Awaits an Asio operation: `init(handler)` starts it, and when the handler runs with the
// arguments of the completion signature Sig, the task resumes: inside the handler when it
// runs on the scheduler's own thread with no other scheduler current (up to four such
// resumes nested), else through the scheduler it suspended on. A handler that runs inside
// init (a dispatch on the loop, a direct call) only stores the result: the task then
// continues in place after init returns. The result
// is void, the single argument, or a std::tuple of the arguments; an error_code is
// returned like any other argument.
//   auto [ec, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
//       [&](auto h) { socket.async_read_some(buffer, std::move(h)); });
// The operation must complete (or be cancelled, which completes it) before the task ends,
// except at teardown.
template <typename Sig, typename Init>
[[nodiscard]] detail::asio_awaiter<Sig, std::decay_t<Init>> await_asio(Init&& init) {
	return detail::asio_awaiter<Sig, std::decay_t<Init>>(std::forward<Init>(init));
}

// The same with a token; Sig must begin with asio::error_code. A requested token never
// calls init and completes with operation_aborted (other arguments value-initialized; one
// without a default constructor, such as a socket, comes back as a std::optional, empty then);
// a request while the operation is pending cancels it through a cancellation slot. The
// operation's I/O object must run on the awaiting scheduler's loop. A cancel that loses the
// race returns the operation's real result: check token.requested() to know whether to stop.
template <typename Sig, typename Init>
[[nodiscard]] detail::cancellable_asio_awaiter<Sig, std::decay_t<Init>> await_asio(Init&& init, cancel_token token) {
	return detail::cancellable_asio_awaiter<Sig, std::decay_t<Init>>(std::forward<Init>(init), std::move(token));
}

}  // namespace task
