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

// Binds task<T> to standalone Asio: a scheduler over an Asio executor, and
// as_awaitable() so asio::awaitable code can co_await a task.

#pragma once

#include "task.hh"

#include <asio/any_io_executor.hpp>
#include <asio/associated_executor.hpp>
#include <asio/async_result.hpp>
#include <asio/awaitable.hpp>
#include <asio/dispatch.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>

#include <atomic>
#include <cassert>
#include <optional>
#include <tuple>

namespace task {

// Runs handles on an Asio executor. Demoted handles wait in a local queue that a
// self-reposting drainer empties one handle per pass, so ordinary posts that arrive
// meanwhile run first. All calls happen on the executor's thread except post(),
// demoted(), and the destructor. Handles queued in it must run to completion before it
// is destroyed: the destructor asserts that no demoted handle remains.
class asio_scheduler {
public:
	explicit asio_scheduler(asio::any_io_executor executor) : executor_(std::move(executor)) {}
	~asio_scheduler() { assert(demoted() == 0); }
	asio_scheduler(const asio_scheduler&) = delete;
	asio_scheduler& operator=(const asio_scheduler&) = delete;

	void post(std::coroutine_handle<> h) {
		asio::post(executor_, [this, h] {
			running_on scope(scheduler_ref::of(*this));
			h.resume();
		});
	}

	void post_demoted(std::coroutine_handle<> h) {
		demoted_.push_back(h);
		demoted_count_.fetch_add(1, std::memory_order_relaxed);
		if (!draining_) {
			draining_ = true;
			asio::post(executor_, [this] { drain(); });
		}
	}

	std::uint64_t now() const noexcept {
		return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
	}

	// Handles waiting in the demoted queue; readable from any thread (admission, metrics).
	std::size_t demoted() const noexcept { return demoted_count_.load(std::memory_order_relaxed); }

	// Runs on_fire() on the executor at the deadline, unless the returned timer is
	// destroyed first (on_fire is then destroyed unrun). For async_mutex::lock_until.
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
	std::deque<std::coroutine_handle<>> demoted_;
	std::atomic<std::size_t> demoted_count_{0};
	bool draining_ = false;
};

// Runs t on scheduler s and completes on the awaiting coroutine's executor; exceptions
// thrown by t propagate to the awaiter.
template <typename T>
asio::awaitable<T> as_awaitable(asio_scheduler& s, task<T> t) {
	auto initiate = [&s](auto handler, task<T> work) {
		spawn(scheduler_ref::of(s), std::move(work), [h = std::move(handler)](std::exception_ptr e, auto&&... value) mutable {
			auto ex = asio::get_associated_executor(h);
			asio::dispatch(ex, [h = std::move(h), e, ... value = std::move(value)]() mutable { std::move(h)(e, std::move(value)...); });
		});
	};
	if constexpr (std::is_void_v<T>) {
		co_await asio::async_initiate<const asio::use_awaitable_t<>&, void(std::exception_ptr)>(initiate, asio::use_awaitable, std::move(t));
	} else {
		auto v = co_await asio::async_initiate<const asio::use_awaitable_t<>&, void(std::exception_ptr, std::optional<T>)>(
			initiate, asio::use_awaitable, std::move(t));
		co_return std::move(*v);
	}
}

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

template <typename Sig, typename Init>
class asio_awaiter;

template <typename... A, typename Init>
class asio_awaiter<void(A...), Init> {
public:
	explicit asio_awaiter(Init init) : init_(std::move(init)) {}
	asio_awaiter(const asio_awaiter&) = delete;
	asio_awaiter& operator=(const asio_awaiter&) = delete;

	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) {
		h_ = h;
		sched_ = current();
		init_(handler{this});  // the operation may complete before this returns, on another thread
	}
	typename asio_result<A...>::type await_resume() {
		if constexpr (sizeof...(A) > 0) { return asio_result<A...>::take(*result_); }
	}

private:
	struct handler {
		asio_awaiter* self;
		void operator()(A... a) {
			self->result_.emplace(std::move(a)...);
			resume_on(self->sched_, self->h_);  // nothing touches *self after this
		}
	};
	Init init_;
	std::coroutine_handle<> h_;
	scheduler_ref sched_;
	std::optional<std::tuple<A...>> result_;
};

}  // namespace detail

// Awaits an Asio operation: `init(handler)` starts it, and when the handler runs with the
// arguments of the completion signature Sig, the task resumes through the scheduler it
// suspended on, so the resumed run is a fresh slice on its own thread. The result is
// void, the single argument, or a std::tuple of the arguments; an error_code is returned
// like any other argument.
//   auto [ec, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
//       [&](auto h) { socket.async_read_some(buffer, std::move(h)); });
// The operation must complete (or be cancelled, which completes it) before the task ends.
template <typename Sig, typename Init>
[[nodiscard]] detail::asio_awaiter<Sig, std::decay_t<Init>> await_asio(Init&& init) {
	return detail::asio_awaiter<Sig, std::decay_t<Init>>(std::forward<Init>(init));
}

}  // namespace task
