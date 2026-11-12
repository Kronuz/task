// No allocation per arm, re-arm, cancel, remote arm or fire on task::timer_wheel, near or far,
// nor per deadline on inline_scheduler once the frame pool is warm: a counting operator new.
// TSan's runtime replaces operator new itself, so its build only runs the operations.
#include "task.hh"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>
#include <random>
#include <thread>
#include <vector>

#if defined(__SANITIZE_THREAD__)
#define COUNTS 0
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COUNTS 0
#endif
#endif
#ifndef COUNTS
#define COUNTS 1
#endif

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

static std::atomic<std::size_t> allocations{0};
#if COUNTS
void* operator new(std::size_t n) {
	allocations.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
#endif

namespace {
using u64 = std::uint64_t;
constexpr u64 ms = 1'000'000;

void wheel() {
	constexpr std::size_t n = 1000;
	std::vector<task::timer_entry> entries(n);
	auto w = std::make_unique<task::timer_wheel>(0);
	std::mt19937_64 random(5);
	w->arm_remote(entries[0], 1);  // this thread's stripe, chosen on first use
	CHECK(w->expire(1 * ms) == 0 && w->size() == 1);
	w->cancel(entries[0]);
	const std::size_t before = allocations.load();
	std::size_t fired = 0;
	for (int rep = 0; rep < 20; ++rep) {
		const u64 base = u64(rep) * 100'000 * ms;
		for (auto& e : entries) { w->arm(e, base + 1 + random() % (2'000 * ms)); }       // arm
		for (auto& e : entries) { w->arm(e, base + 1 + random() % (90'000 * ms)); }      // re-arm, some far
		for (std::size_t i = 0; i < n; i += 2) { w->cancel(entries[i]); }                 // cancel
		for (std::size_t i = 0; i < n; i += 2) { w->arm_remote(entries[i], base + 5 * ms); }  // remote arm
		for (u64 t = base; t <= base + 99'000 * ms; t += ms) { fired += w->expire(t, [](task::timer_entry&) {}); }  // fire
	}
	const std::size_t during = allocations.load() - before;
	CHECK(fired == 20 * n && w->size() == 0);
	CHECK(!COUNTS || during == 0);
	std::printf("wheel ops=%zu allocations=%zu ok\n", std::size_t(20 * n * 5), during);
}

void deadlines() {
	using tp = task::inline_scheduler::clock::time_point;
	task::inline_scheduler s;
	(void)s.wheel();
	for (int i = 0; i < 64; ++i) { task::deadline<task::inline_scheduler> warm(s, tp(std::chrono::nanoseconds(5 * ms))); }  // the pool
	const std::size_t before = allocations.load();
	std::size_t expired = 0;
	for (int i = 0; i < 10'000; ++i) {
		task::deadline<task::inline_scheduler> d(s, tp(std::chrono::nanoseconds(s.now() + 1 + (i % 7) * ms)));
		if (i % 2) {
			s.advance(7 * ms);
			expired += d.expired();
		}
	}
	const std::size_t during = allocations.load() - before;
	CHECK(expired == 5'000);
	CHECK(!COUNTS || during == 0);
	std::printf("deadlines made=10000 expired=%zu allocations=%zu ok\n", expired, during);
}
}  // namespace

int main() {
	wheel();
	deadlines();
	std::printf("wheel_alloc_test: ok (counting=%d)\n", COUNTS);
	return 0;
}
