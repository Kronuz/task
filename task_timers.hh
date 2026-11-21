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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
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

// fire(key, flags) runs on the group's scheduler once the key has been quiet for `quiet` after its
// first touch, later while touches keep coming (`busy` after each), and never later than a random
// time in [min_force, max_force] after the first touch; `flags` ORs every touch's since the last
// fire. A touch moves a key's target, never earlier. With `throttle`, a fired key keeps its entry
// until `throttle` passes, and a touch meanwhile fires no earlier than that. One task per key
// with an entry sleeps on the wheel, but for a key fire_pending() holds. `stop` ends every wait
// without firing and keeps the pending keys for fire_pending(); the owner then joins the group
// before the debounce dies.
template <typename Key, typename Hash = std::hash<Key>>
class debounce {
public:
	struct timing {
		std::chrono::nanoseconds quiet, busy, min_force, max_force, throttle{0};
	};

	debounce(group& g, timing t, cancel_token stop, std::function<void(const Key&, unsigned)> fire)
		: group_(g), timing_(t), stop_(std::move(stop)), fire_(std::move(fire)), rng_(std::random_device{}()) {}
	debounce(const debounce&) = delete;
	debounce& operator=(const debounce&) = delete;

	// Any thread. True when a fire for the key, with `flags`, is pending; false once stop is
	// requested: the caller then runs the call itself.
	bool touch(const Key& key, unsigned flags = 0) {
		std::uint64_t fresh = 0;
		{
			std::lock_guard lock(m_);
			if (stop_.requested()) { return false; }
			const std::uint64_t now = group_.scheduler().now();
			auto [it, inserted] = keys_.try_emplace(key);
			entry& e = it->second;
			if (inserted) { fresh = e.gen = ++last_gen_; }
			if (!e.pending) {  // new, held, or throttled: its target is the throttle's end
				e.pending = true;
				++pending_;
				e.force = now + force_after();
				e.target = std::max(e.target, std::min(now + ns(timing_.quiet), e.force));
			} else {
				e.target = std::max(e.target, std::min(now + ns(timing_.busy), e.force));
			}
			e.flags |= flags;
		}
		if (fresh) { group_.spawn(watch(key, fresh)); }
		return true;
	}

	// Any thread but a fire's own (a Debug assert): takes every pending key and fires it on this
	// thread, after the loop's fires in progress at the call and an earlier call's fires of a key it
	// took end, so every touch before it has fired, or is firing in an earlier call, when it returns.
	// A touch of a taken key meanwhile fires after this call's fires, on the loop or in a later call:
	// no key fires twice at once. Every key fires; the first throw is rethrown after. Returns how
	// many it fired.
	std::size_t fire_pending() {
		assert(!fires_here() && "fire_pending() inside this debounce's fire waits for itself");
		std::vector<std::pair<Key, unsigned>> taken;
		std::vector<std::pair<const entry*, std::uint64_t>> turns;  // keys an earlier call still holds
		{
			std::unique_lock lock(m_);
			taken.reserve(pending_);
			turns.reserve(pending_);
			for (const auto& [key, e] : keys_) {  // copies first: a throw leaves every entry as it was
				if (e.pending) { taken.emplace_back(key, e.flags); }
			}
			for (auto& [key, e] : keys_) {
				if (!e.pending) { continue; }
				if (e.served != e.issued) { turns.emplace_back(&e, e.issued); }
				e.gen = 0;  // held: its watch ends, a touch spawns none
				e.target = 0;
				e.flags = 0;
				e.pending = false;
				++e.issued;
			}
			pending_ = 0;
			const std::uint64_t last = tickets_;  // later loop fires do not hold this call
			auto ready = [&] {
				if (oldest_ && oldest_->ticket <= last) { return false; }
				return std::all_of(turns.begin(), turns.end(), [](const auto& t) { return t.first->served >= t.second; });
			};
			if (!ready()) {
				TASK_TEST_HOOK(pending_waits);
				idle_.wait(lock, ready);
			}
		}
		firing mark;  // on this thread's chain only, for the assert
		mark.owner = this;
		mark.outer = std::exchange(here_, &mark);
		std::exception_ptr first;
		for (const auto& [key, flags] : taken) {
			fired_.fetch_add(1, std::memory_order_relaxed);
			try {
				fire_(key, flags);
			} catch (...) {
				if (!first) { first = std::current_exception(); }
			}
		}
		here_ = mark.outer;
		for (const auto& [key, flags] : taken) { hand_off(key, 0); }
		if (first) { std::rethrow_exception(first); }
		return taken.size();
	}

	std::size_t pending_keys() const {
		std::lock_guard lock(m_);
		return pending_;
	}
	std::uint64_t fired() const noexcept { return fired_.load(std::memory_order_relaxed); }

private:
	struct entry {
		std::uint64_t force = 0, target = 0;
		std::uint64_t gen = 0;  // its watch's; 0 while held
		unsigned flags = 0;
		bool pending = false;  // else throttled until target, or held
		std::uint64_t issued = 0, served = 0;  // fire_pending() turns: held while they differ
	};
	// A fire in progress: the loop's, in its watch's frame and the list, or a fire_pending() call's.
	struct firing {
		const debounce* owner = nullptr;
		firing* outer = nullptr;  // this thread's enclosing fire
		std::uint64_t ticket = 0;
		firing* prev = nullptr;  // under m_: fires in progress, oldest first
		firing* next = nullptr;
	};
	static std::uint64_t ns(std::chrono::nanoseconds d) noexcept { return std::uint64_t(std::max<std::int64_t>(0, d.count())); }
	std::uint64_t force_after() {  // under m_
		const auto low = timing_.min_force.count();
		const auto high = std::max(low, timing_.max_force.count());
		return ns(std::chrono::nanoseconds(std::uniform_int_distribution<std::int64_t>(low, high)(rng_)));
	}

	task<void> watch(Key key, std::uint64_t gen) {
		for (;;) {
			firing mark;
			std::uint64_t target = 0;
			unsigned flags = 0;
			bool due = false;
			{
				std::lock_guard lock(m_);
				auto it = keys_.find(key);
				if (it == keys_.end() || it->second.gen != gen) { co_return; }
				entry& e = it->second;
				if (stop_.requested()) {
					if (!e.pending) { keys_.erase(it); }  // a pending key waits for fire_pending()
					co_return;
				}
				const std::uint64_t now = group_.scheduler().now();
				if (e.target <= now && !e.pending) {
					keys_.erase(it);  // the throttle passed
					co_return;
				}
				if (e.target <= now) {
					due = true;
					flags = std::exchange(e.flags, 0u);
					e.pending = false;  // a touch from here on starts a new fire
					--pending_;
					begin_fire(mark);
					if (timing_.throttle.count() > 0) {
						e.target = now + ns(timing_.throttle);
					} else {
						keys_.erase(it);
					}
				} else {
					target = e.target;
				}
			}
			if (!due) {
				co_await sleep_until(target, stop_);
				continue;
			}
			fired_.fetch_add(1, std::memory_order_relaxed);
			try {
				fire_(key, flags);
			} catch (...) {
				end_fire(mark);
				hand_off(key, gen);
				throw;  // to the group's `failed`
			}
			end_fire(mark);
			if (timing_.throttle.count() <= 0) { co_return; }
		}
	}
	// Under m_, on the thread about to fire: a ticket, the tail of the list, and this thread's mark.
	void begin_fire(firing& f) noexcept {
		f.owner = this;
		f.ticket = ++tickets_;
		f.prev = newest_;
		(newest_ ? newest_->next : oldest_) = &f;
		newest_ = &f;
		f.outer = std::exchange(here_, &f);
	}
	void end_fire(firing& f) {
		here_ = f.outer;
		std::lock_guard lock(m_);
		(f.prev ? f.prev->next : oldest_) = f.next;
		(f.next ? f.next->prev : newest_) = f.prev;
		if (!f.prev) { idle_.notify_all(); }  // the oldest ended: a waiter's bound may be met
	}
	bool fires_here() const noexcept {
		for (const firing* f = here_; f; f = f->outer) {
			if (f->owner == this) { return true; }
		}
		return false;
	}
	// After a throwing loop fire (its gen) or a fire_pending() call's (gen 0, once its last holder
	// ends): drops a throttled or fired entry; a pending one gets a watch of its own.
	void hand_off(const Key& key, std::uint64_t gen) {
		std::uint64_t fresh = 0;
		{
			std::lock_guard lock(m_);
			auto it = keys_.find(key);
			if (it == keys_.end() || it->second.gen != gen) { return; }
			if (gen == 0 && ++it->second.served != it->second.issued) {
				idle_.notify_all();  // the next call's turn
				return;
			}
			if (!it->second.pending) {
				keys_.erase(it);
			} else if (!stop_.requested()) {
				fresh = it->second.gen = ++last_gen_;
			}
		}
		if (fresh) { group_.spawn(watch(key, fresh)); }
	}

	group& group_;
	timing timing_;
	cancel_token stop_;
	std::function<void(const Key&, unsigned)> fire_;
	mutable std::mutex m_;
	std::condition_variable idle_;  // the oldest fire in progress ended
	std::unordered_map<Key, entry, Hash> keys_;
	std::size_t pending_ = 0;  // entries with a fire pending
	firing* oldest_ = nullptr;  // fires in progress on the group's scheduler, by ticket
	firing* newest_ = nullptr;
	std::uint64_t tickets_ = 0;
	static inline thread_local firing* here_ = nullptr;  // this thread's innermost fire
	std::uint64_t last_gen_ = 0;
	std::mt19937_64 rng_;
	std::atomic<std::uint64_t> fired_{0};
};

}  // namespace task
