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

// Housekeeping on a loop's wheel: a group of tasks one owner joins, periodic work, and a
// per-key debounce. Each wait is a task::sleep_until entry on the scheduler's wheel, so no
// thread and no fixed-interval poll is involved. Needs only task.hh.

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "task.hh"

namespace task {

// Tasks on one scheduler, counted until their frames end. join() wakes at zero, with no poll.
// An exception escaping a task goes to `failed` (terminate when it is empty).
class group {
public:
	explicit group(scheduler_ref s) noexcept : sched_(s) {}
	group(const group&) = delete;
	group& operator=(const group&) = delete;
	~group() { assert(running() == 0 && "a group outlives its tasks: join it first"); }

	scheduler_ref scheduler() const noexcept { return sched_; }
	std::size_t running() const noexcept { return running_.load(std::memory_order_acquire); }

	// Any thread: queues t on the group's scheduler.
	void spawn(task<void> t) {
		running_.fetch_add(1, std::memory_order_relaxed);
		::task::spawn(sched_, counted(std::move(t), ended{this}));
	}

	struct join_awaiter;
	// true once running() is zero; false when `token` is requested first.
	[[nodiscard]] join_awaiter join(cancel_token token = {}) noexcept;

	std::function<void(std::exception_ptr)> failed;

private:
	// Drops the count however the frame ends, a teardown's destroy included.
	struct ended {
		group* g;
		ended(group* p) noexcept : g(p) {}
		ended(ended&& o) noexcept : g(std::exchange(o.g, nullptr)) {}
		~ended() {
			if (g) { g->finish(); }
		}
	};
	task<void> counted(task<void> t, ended e) {
		try {
			task<void> mine = std::move(t);  // its frame ends before the count drops
			co_await std::move(mine);
		} catch (...) {
			if (failed) {
				failed(std::current_exception());
			} else {
				std::terminate();
			}
		}
	}
	void finish() noexcept;

	scheduler_ref sched_;
	std::atomic<std::size_t> running_{0};
	std::mutex m_;
	join_awaiter* waiters_ = nullptr;  // under m_
};

struct group::join_awaiter {
	enum : unsigned char { idle, linked, done };
	group& g;
	cancel_token token;
	std::coroutine_handle<> handle{};
	scheduler_ref sched{};
	join_awaiter* next = nullptr;
	unsigned char state = idle;  // under g.m_
	bool zero = false;
	struct on_cancel {
		join_awaiter* self;
		void operator()() noexcept { self->settle(false); }
	};
	std::optional<cancel_callback<on_cancel>> callback{};

	~join_awaiter() {
		callback.reset();
		std::lock_guard lock(g.m_);
		if (state == linked) { unlink(); }  // a frame destroyed while joining (teardown)
	}
	bool await_ready() noexcept {
		zero = g.running() == 0;
		return zero || token.requested();
	}
	bool await_suspend(std::coroutine_handle<> h) {
		handle = h;
		sched = current();
		callback.emplace(token, on_cancel{this});
		std::lock_guard lock(g.m_);
		if (state == done) { return false; }  // the token, while registering
		if (g.running() == 0) {
			state = done;
			zero = true;
			return false;
		}
		state = linked;
		next = g.waiters_;
		g.waiters_ = this;
		return true;  // finish() or the token may resume this frame now: touch nothing more
	}
	bool await_resume() const noexcept { return zero; }

	// The token: unlinks and resumes the waiter with false, unless the count got there first.
	void settle(bool at_zero) noexcept {
		std::unique_lock lock(g.m_);
		if (state == done) { return; }
		const bool was_linked = state == linked;
		if (was_linked) { unlink(); }
		state = done;
		zero = at_zero;
		lock.unlock();
		if (was_linked) { detail::resume_on(sched, handle); }
	}
	void unlink() noexcept {
		for (join_awaiter** p = &g.waiters_; *p; p = &(*p)->next) {
			if (*p == this) {
				*p = next;
				return;
			}
		}
	}
};

inline group::join_awaiter group::join(cancel_token token) noexcept { return join_awaiter{*this, std::move(token)}; }

inline void group::finish() noexcept {
	std::unique_lock lock(m_);
	if (running_.fetch_sub(1, std::memory_order_acq_rel) != 1) { return; }
	join_awaiter* list = std::exchange(waiters_, nullptr);
	for (join_awaiter* w = list; w; w = w->next) {
		w->state = join_awaiter::done;
		w->zero = true;
	}
	lock.unlock();
	while (list) {
		join_awaiter* w = std::exchange(list, list->next);  // read before w's frame can end
		detail::resume_on(w->sched, w->handle);
	}
}

namespace detail {
template <typename F>
task<void> run_tick(F& tick) {
	if constexpr (std::is_same_v<std::invoke_result_t<F&>, task<void>>) {
		co_await tick();
	} else {
		tick();
	}
}
}  // namespace detail

// Runs tick() `period` after the previous tick ends, until `stop`, which ends the wait at once.
// A tick that throws goes to on_throw, and the next tick runs. tick may return task<void>.
template <std::invocable F, std::invocable<std::exception_ptr> E>
task<void> periodic(std::chrono::nanoseconds period, cancel_token stop, F tick, E on_throw) {
	while (co_await sleep_for(period, stop)) {
		try {
			co_await detail::run_tick(tick);
		} catch (...) {
			on_throw(std::current_exception());
		}
	}
}

// fire(key) runs on the group's scheduler once the key has been quiet for `quiet` after its first
// touch, later while touches keep coming (`busy` after each), and never later than a random time
// in [min_force, max_force] after the first touch. A touch moves a key's target, never earlier.
// One task per pending key sleeps on the wheel. `stop` ends every wait without firing; the owner
// then joins the group before the debounce dies.
template <typename Key, typename Hash = std::hash<Key>>
class debounce {
public:
	struct timing {
		std::chrono::nanoseconds quiet, busy, min_force, max_force;
	};

	debounce(group& g, timing t, cancel_token stop, std::function<void(const Key&)> fire)
		: group_(g), timing_(t), stop_(std::move(stop)), fire_(std::move(fire)), rng_(std::random_device{}()) {}
	debounce(const debounce&) = delete;
	debounce& operator=(const debounce&) = delete;

	// Any thread. True when a fire for the key is pending; false once stop is requested: the
	// caller then runs the call itself.
	bool touch(const Key& key) {
		bool fresh = false;
		{
			std::lock_guard lock(m_);
			if (stop_.requested()) { return false; }
			const std::uint64_t now = group_.scheduler().now();
			auto [it, inserted] = keys_.try_emplace(key);
			pending& p = it->second;
			if (inserted) {
				p.force = now + force_after();
				p.target = std::min(now + ns(timing_.quiet), p.force);
				fresh = true;
			} else {
				p.target = std::max(p.target, std::min(now + ns(timing_.busy), p.force));
			}
		}
		if (fresh) { group_.spawn(watch(key)); }
		return true;
	}

	std::size_t pending_keys() const {
		std::lock_guard lock(m_);
		return keys_.size();
	}
	std::uint64_t fired() const noexcept { return fired_.load(std::memory_order_relaxed); }

private:
	struct pending {
		std::uint64_t force = 0, target = 0;
	};
	static std::uint64_t ns(std::chrono::nanoseconds d) noexcept { return std::uint64_t(std::max<std::int64_t>(0, d.count())); }
	std::uint64_t force_after() {  // under m_
		const auto low = timing_.min_force.count();
		const auto high = std::max(low, timing_.max_force.count());
		return ns(std::chrono::nanoseconds(std::uniform_int_distribution<std::int64_t>(low, high)(rng_)));
	}

	task<void> watch(Key key) {
		for (;;) {
			std::uint64_t target;
			bool due = false;
			{
				std::lock_guard lock(m_);
				auto it = keys_.find(key);
				if (it == keys_.end()) { co_return; }
				if (stop_.requested()) {
					keys_.erase(it);
					co_return;
				}
				target = it->second.target;
				if (target <= group_.scheduler().now()) {
					keys_.erase(it);  // a touch from here on starts a new fire
					due = true;
				}
			}
			if (due) {
				fired_.fetch_add(1, std::memory_order_relaxed);
				fire_(key);
				co_return;
			}
			co_await sleep_until(target, stop_);
		}
	}

	group& group_;
	timing timing_;
	cancel_token stop_;
	std::function<void(const Key&)> fire_;
	mutable std::mutex m_;
	std::unordered_map<Key, pending, Hash> keys_;
	std::mt19937_64 rng_;
	std::atomic<std::uint64_t> fired_{0};
};

}  // namespace task
