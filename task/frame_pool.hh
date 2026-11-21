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

// Frame allocation: the per-thread pool coroutine frames and shared state come from, its
// AddressSanitizer poisoning, and the test hooks.

#pragma once

#include "drivers.hh"

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

// Counts pooled blocks alive (frames of both pool paths, lock waiters, cancel states) when
// TASK_COUNT_FRAMES is set; tests assert it returns to its baseline.
#ifndef TASK_COUNT_FRAMES
#define TASK_COUNT_FRAMES 0
#endif
#if TASK_COUNT_FRAMES
inline std::atomic<std::ptrdiff_t> live_count{0};
#endif

inline std::ptrdiff_t live_frames() noexcept {
#if TASK_COUNT_FRAMES
	return live_count.load(std::memory_order_relaxed);
#else
	return 0;
#endif
}

// TASK_FRAME_POOL=0 sends every pooled block to operator new, so a bench can count task
// frames against another library's allocations. Every translation unit of one program must
// define it (and TASK_COUNT_FRAMES) the same: they change inline functions (ODR).
#ifndef TASK_FRAME_POOL
#define TASK_FRAME_POOL 1
#endif

struct pooled_frame {
	static void* operator new(std::size_t n) {
#if TASK_FRAME_POOL
		void* p = pool().allocate(n);
#else
		void* p = ::operator new(n);
#endif
#if TASK_COUNT_FRAMES
		live_count.fetch_add(1, std::memory_order_relaxed);
#endif
		return p;
	}
	static void operator delete(void* p, std::size_t n) noexcept {
#if TASK_COUNT_FRAMES
		live_count.fetch_sub(1, std::memory_order_relaxed);
#endif
#if TASK_FRAME_POOL
		pool().deallocate(p, n);
#else
		(void)n;
		::operator delete(p);
#endif
	}
};

}  // namespace detail

// Test-only interleaving points, compiled in when TASK_TEST_HOOKS is defined.
#ifdef TASK_TEST_HOOKS
namespace detail::test_hooks {
inline thread_local void (*after_enqueue)() = nullptr;  // async_mutex::lock(token), before registering
inline thread_local void (*before_link)() = nullptr;    // cancel_source(parent), before registering on it
inline thread_local void (*cancel_settled)() = nullptr;  // a lock(token) cancel callback, between settle and wake
inline thread_local void (*asio_registered)() = nullptr; // await_asio(init, token), between registering and the check
inline thread_local void (*sleep_claimed)() = nullptr;    // a sleep's cancel callback, between its claim and its hop
inline thread_local void (*signal_loaded)() = nullptr;    // stop_signals' handler, between loading the pipe and writing
inline thread_local void (*off_loop_start)() = nullptr;   // off_loop, before it starts its thread; may throw
inline thread_local void (*pending_waits)() = nullptr;    // debounce::fire_pending, before it waits for a fire in progress
}  // namespace detail::test_hooks
#define TASK_TEST_HOOK(name) \
	do { \
		if (auto* hook_ = ::task::detail::test_hooks::name) { hook_(); } \
	} while (0)
#else
#define TASK_TEST_HOOK(name) ((void)0)
#endif

}  // namespace task
