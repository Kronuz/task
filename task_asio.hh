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
#include <asio/use_awaitable.hpp>

namespace task {

// Runs handles on an Asio executor. Demoted handles wait in a local queue that a
// self-reposting drainer empties one handle per pass, so ordinary posts that arrive
// meanwhile run first. All calls happen on the executor's thread except post().
class asio_scheduler {
public:
	explicit asio_scheduler(asio::any_io_executor executor) : executor_(std::move(executor)) {}
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
		if (!draining_) {
			draining_ = true;
			asio::post(executor_, [this] { drain(); });
		}
	}

	std::uint64_t now() const noexcept {
		return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
	}

	const asio::any_io_executor& executor() const noexcept { return executor_; }

private:
	void drain() {
		auto h = demoted_.front();
		demoted_.pop_front();
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

}  // namespace task
