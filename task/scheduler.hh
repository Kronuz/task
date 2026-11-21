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

// Schedulers and what runs on them: the cycle counter, the scheduler concept and scheduler_ref,
// symmetric transfer, and the running_on scope each resume sits in.

#pragma once

#include <algorithm>
#include <array>
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
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "wheel.hh"

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
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
// nanoseconds. post_demoted, when present, queues behind ordinary posts; post_heavy, when
// present, queues in the heavy lane and resumes in a heavy scope (docs/lanes.md).
template <typename S>
concept scheduler = requires(S& s, std::coroutine_handle<> h) {
	s.post(h);
	{ s.now() } -> std::convertible_to<std::uint64_t>;
};

class root_list;

namespace detail {

// What scheduler_ref calls through. post_call, runs_here, roots, arm and cancel are optional:
// null when the scheduler does not declare them. post_heavy falls back to post_demoted, and
// post_demoted to post.
struct scheduler_vtable {
	void (*post)(void*, std::coroutine_handle<>);
	void (*post_demoted)(void*, std::coroutine_handle<>);
	void (*post_heavy)(void*, std::coroutine_handle<>);
	std::uint64_t (*now)(void*);
	void (*post_call)(void*, void (*)(void*), void*);
	bool (*runs_here)(void*) noexcept;
	root_list* (*roots)(void*) noexcept;
	void (*arm)(void*, wheel::timer_entry&, std::uint64_t);
	bool (*cancel)(void*, wheel::timer_entry&) noexcept;
};

template <typename S>
constexpr scheduler_vtable make_scheduler_vtable() noexcept {
	scheduler_vtable t{};
	t.post = [](void* p, std::coroutine_handle<> h) { static_cast<S*>(p)->post(h); };
	if constexpr (requires(S& s) { s.post_demoted(std::coroutine_handle<>{}); }) {
		t.post_demoted = [](void* p, std::coroutine_handle<> h) { static_cast<S*>(p)->post_demoted(h); };
	} else {
		t.post_demoted = t.post;
	}
	if constexpr (requires(S& s) { s.post_heavy(std::coroutine_handle<>{}); }) {
		t.post_heavy = [](void* p, std::coroutine_handle<> h) { static_cast<S*>(p)->post_heavy(h); };
	} else {
		t.post_heavy = t.post_demoted;
	}
	t.now = [](void* p) -> std::uint64_t { return static_cast<S*>(p)->now(); };
	if constexpr (requires(S& s, void (*f)(void*), void* a) { s.post_call(f, a); }) {
		t.post_call = [](void* p, void (*f)(void*), void* a) { static_cast<S*>(p)->post_call(f, a); };
	}
	if constexpr (requires(S& s) { { s.runs_here() } -> std::convertible_to<bool>; }) {
		t.runs_here = [](void* p) noexcept -> bool { return static_cast<S*>(p)->runs_here(); };
	}
	if constexpr (requires(S& s) { { s.roots() } -> std::convertible_to<root_list*>; }) {
		t.roots = [](void* p) noexcept -> root_list* { return static_cast<S*>(p)->roots(); };
	}
	if constexpr (requires(S& s, wheel::timer_entry& e, std::uint64_t at) {
		s.arm(e, at);
		{ s.cancel(e) } -> std::same_as<bool>;
	}) {
		t.arm = [](void* p, wheel::timer_entry& e, std::uint64_t at) { static_cast<S*>(p)->arm(e, at); };
		t.cancel = [](void* p, wheel::timer_entry& e) noexcept -> bool { return static_cast<S*>(p)->cancel(e); };
	}
	return t;
}

template <typename S>
inline constexpr scheduler_vtable scheduler_vtable_of = make_scheduler_vtable<S>();

}  // namespace detail

// A type-erased reference to a scheduler: the scheduler, its vtable, and a lane. A reference
// taken in a heavy scope is heavy: its posts go to post_heavy (docs/lanes.md). Equality
// ignores the lane. Optional members a scheduler (or a wrapper forwarding to one) may declare,
// found by detection:
//   void post_demoted(handle);                     queues behind ordinary posts
//   void post_heavy(handle);                       queues in the heavy lane, resumed in a heavy scope
//   bool runs_here() const noexcept;               true on the scheduler's own thread
//   void post_call(void (*fn)(void*), void* arg);  runs fn(arg) on it; dropped when closed
//   task::root_list* roots() noexcept;             spawned tasks are roots there (teardown)
//   void arm(wheel::timer_entry&, std::uint64_t);  files a timer at a now() time, on its own thread
//   bool cancel(wheel::timer_entry&) noexcept;     unfiles it, on its own thread; true if it was armed
class scheduler_ref {
public:
	scheduler_ref() = default;

	template <scheduler S>
	static scheduler_ref of(S& s) noexcept {
		scheduler_ref r;
		r.self_ = &s;
		r.vt_ = reinterpret_cast<std::uintptr_t>(&detail::scheduler_vtable_of<S>);
		return r;
	}

	void post(std::coroutine_handle<> h) const { (heavy() ? vt()->post_heavy : vt()->post)(self_, h); }
	void post_demoted(std::coroutine_handle<> h) const { (heavy() ? vt()->post_heavy : vt()->post_demoted)(self_, h); }
	// The lane: a heavy reference posts to the scheduler's heavy lane.
	bool heavy() const noexcept { return vt_ & 1; }
	// The same scheduler, in the heavy lane or the light one.
	scheduler_ref with_heavy(bool heavy) const noexcept {
		scheduler_ref r = *this;
		r.vt_ = (vt_ & ~std::uintptr_t(1)) | std::uintptr_t(heavy);
		return r;
	}
	// The scheduler's clock, in nanoseconds: what arm() takes.
	std::uint64_t now() const { return vt()->now(self_); }
	// Queues fn(arg) on the scheduler; false (nothing queued) when it declares no post_call.
	bool post_call(void (*fn)(void*), void* arg) const {
		if (!vt() || !vt()->post_call) { return false; }
		vt()->post_call(self_, fn, arg);
		return true;
	}
	// True on the scheduler's own thread; false when it cannot tell.
	bool runs_here() const noexcept { return vt() && vt()->runs_here && vt()->runs_here(self_); }
	// Where spawn links its roots; null when the scheduler owns none.
	root_list* roots() const noexcept { return vt() && vt()->roots ? vt()->roots(self_) : nullptr; }
	// On the scheduler's thread: files e to fire there at `at` (as now()), waking its loop by
	// then, or moves it if armed. False (nothing armed) when the scheduler has no timers.
	bool arm(wheel::timer_entry& e, std::uint64_t at) const {
		if (!vt() || !vt()->arm) { return false; }
		vt()->arm(self_, e, at);
		return true;
	}
	// On the scheduler's thread: takes e off its timers; true if e was armed there.
	bool cancel(wheel::timer_entry& e) const noexcept { return vt() && vt()->cancel && vt()->cancel(self_, e); }
	explicit operator bool() const noexcept { return self_ != nullptr; }
	friend bool operator==(const scheduler_ref& a, const scheduler_ref& b) noexcept { return a.self_ == b.self_; }

private:
	const detail::scheduler_vtable* vt() const noexcept { return reinterpret_cast<const detail::scheduler_vtable*>(vt_ & ~std::uintptr_t(1)); }

	void* self_ = nullptr;
	std::uintptr_t vt_ = 0;  // the vtable's address, whose low bit is free (it is aligned): the lane
};
static_assert(alignof(detail::scheduler_vtable) >= 2);
static_assert(sizeof(scheduler_ref) == 2 * sizeof(void*), "the lane is packed into the vtable pointer");

namespace detail {
inline thread_local scheduler_ref current_scheduler;
// When the current resume began, in cycle-counter ticks; budgets measure their slice from it.
inline thread_local std::uint64_t slice_start = 0;
}

// The scheduler running on this thread right now (empty outside any scheduler).
inline scheduler_ref current() noexcept { return detail::current_scheduler; }

// Marks the current scope light: what it captures from here on resumes in the light lane.
inline void enter_light() noexcept { detail::current_scheduler = detail::current_scheduler.with_heavy(false); }

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

// Gives the enclosing scope a trampoline loop of its own. Inside a running loop a transfer only
// queues, so a chain resumed in the scope would wait for that loop, which the scope may be blocking.
struct fresh_trampoline {
#if TASK_SYMMETRIC_TRANSFER
	fresh_trampoline() noexcept {}
#else
	trampoline outer;
	bool nested = local_trampoline().active;
	fresh_trampoline() noexcept {
		if (nested) { std::swap(local_trampoline(), outer); }
	}
	~fresh_trampoline() {
		if (nested) { std::swap(local_trampoline(), outer); }
	}
#endif
	fresh_trampoline(const fresh_trampoline&) = delete;
	fresh_trampoline& operator=(const fresh_trampoline&) = delete;
};

}  // namespace detail

// Marks code as running on a scheduler; schedulers wrap each resume in one. Entering a scope
// starts a time slice (see budget), and leaving it restores the enclosing scope's. Each scope
// runs transfers on a trampoline loop of its own, so a driver that blocks its thread inside
// another loop's resume (a nested sync_wait or run loop) never waits on that loop's queue.
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
	[[no_unique_address]] detail::fresh_trampoline fresh_;
	scheduler_ref previous_;
	std::uint64_t previous_start_;
};

}  // namespace task
