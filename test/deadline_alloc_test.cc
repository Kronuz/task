// No allocation per deadline on inline_scheduler's wheel once the frame pool is warm: a counting
// operator new (Kronuz/wheel's own test covers the wheel's arm, cancel and fire).
// TSan's runtime replaces operator new itself, so its build only runs the operations.
#include "task.hh"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>

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

void deadlines() {
	using tp = task::inline_scheduler::clock::time_point;
	task::inline_scheduler s;
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
	deadlines();
	std::printf("deadline_alloc_test: ok (counting=%d)\n", COUNTS);
	return 0;
}
