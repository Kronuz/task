// No allocation per deadline on a scheduler's wheel once the frame pool is warm, a counting
// operator new: inline_scheduler, blocking_scheduler, and asio_scheduler when built with
// TASK_ALLOC_ASIO, each deadline armed and unfiled on its loop (Kronuz/wheel's own test covers
// the wheel's arm, cancel and fire). TSan's runtime replaces operator new itself, so its build
// only runs the operations.
#if TASK_ALLOC_ASIO
#include "task_asio.hh"
#else
#include "task.hh"
#endif

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>

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
// The blocking entry's scheduler: deadlines made and destroyed on the driving thread.
void blocking() {
	task::blocking_scheduler s;
	const auto start = std::chrono::steady_clock::now();
	for (int i = 0; i < 64; ++i) { task::deadline<task::blocking_scheduler> warm(s, start + std::chrono::seconds(5)); }
	const std::size_t before = allocations.load();
	for (int i = 0; i < 10'000; ++i) {
		task::deadline<task::blocking_scheduler> d(s, start + std::chrono::seconds(5) + std::chrono::milliseconds(i % 7));
	}
	const std::size_t during = allocations.load() - before;
	CHECK(s.armed_timers() == 0);
	CHECK(!COUNTS || during == 0);
	std::printf("blocking made=10000 allocations=%zu ok\n", during);
}

#if TASK_ALLOC_ASIO
// asio_scheduler: each round makes a deadline on the loop (filing it arms the wake timer) and
// destroys it in the next handler (the wheel empties, the wake is cancelled and its aborted wait
// runs), so every round crosses the loop as a request does.
void asio_deadlines() {
	using dl_t = task::deadline<task::asio_scheduler>;
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	constexpr int warm = 256, rounds = 10'000;
	std::optional<dl_t> d;
	std::size_t before = 0;
	int round = 0;
	std::function<void()> step = [&] {
		if (d) {
			d.reset();
		} else {
			if (round == warm) { before = allocations.load(); }
			if (round == warm + rounds) { return; }
			++round;
			d.emplace(s, std::chrono::steady_clock::now() + std::chrono::seconds(30));
		}
		asio::post(io, [&] { step(); });
	};
	asio::post(io, [&] { step(); });
	io.run();
	const std::size_t during = allocations.load() - before;
	CHECK(!COUNTS || during == 0);
	std::printf("asio made=%d allocations=%zu ok\n", rounds, during);
}
#endif
}  // namespace

int main() {
	deadlines();
	blocking();
#if TASK_ALLOC_ASIO
	asio_deadlines();
#endif
	std::printf("deadline_alloc_test: ok (counting=%d)\n", COUNTS);
	return 0;
}
