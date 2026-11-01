// A no-op task's create/await/destroy cost and allocations, and (TASK_BENCH_ASIO) yield-to-resume
// latency through the Asio scheduler on an idle loop. With TASK_BENCH_ASIO the same two measurements
// run on Asio's own coroutine type, asio::awaitable (the Asio version CMake pins), as the comparator:
// a no-op awaitable<int> awaited in a loop, and a co_await of asio::post(executor, use_awaitable).
// Both sides return the same sums, which the program checks.
#include "task.hh"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <vector>

#if TASK_BENCH_ASIO
#include "task_asio.hh"
#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/use_awaitable.hpp>
#endif

// TSan's runtime replaces operator new itself, so its builds cannot count allocations.
#if defined(__SANITIZE_THREAD__)
#define TASK_COUNTS_ALLOCATIONS 0
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TASK_COUNTS_ALLOCATIONS 0
#endif
#endif
#ifndef TASK_COUNTS_ALLOCATIONS
#define TASK_COUNTS_ALLOCATIONS 1
#endif

static std::size_t allocations = 0;
#if TASK_COUNTS_ALLOCATIONS
void* operator new(std::size_t n) {
	++allocations;
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
#endif

task::task<int> noop() { co_return 1; }

task::task<long> loop(long n) {
	long sum = 0;
	for (long i = 0; i < n; ++i) { sum += co_await noop(); }
	co_return sum;
}

#if TASK_BENCH_ASIO
asio::awaitable<int> asio_noop() { co_return 1; }

asio::awaitable<void> asio_loop(long n, long& out) {
	long sum = 0;
	for (long i = 0; i < n; ++i) { sum += co_await asio_noop(); }
	out = sum;
}
#endif

int main() {
	task::inline_scheduler s;
	task::run(s, loop(1000));  // warm the frame pool
	constexpr long n = 20'000'000;
	double best = 1e300;
	std::size_t allocs = 0;
	for (int trial = 0; trial < 5; ++trial) {
		std::size_t before = allocations;
		auto t = std::chrono::steady_clock::now();
		long sum = task::run(s, loop(n));
		double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t).count() / double(n);
		allocs = allocations - before;
		if (sum != n) { return 1; }
		best = std::min(best, ns);
	}
	if (TASK_COUNTS_ALLOCATIONS) {
		std::printf("no-op task create+await+destroy: %.2f ns, %zu allocations for %ld tasks (outer task and run scaffolding included)\n", best, allocs, n);
	} else {
		std::printf("no-op task create+await+destroy: %.2f ns (allocations not counted under TSan)\n", best);
	}

#if TASK_BENCH_ASIO
	{
		asio::io_context loop_io;
		long warm = 0;
		asio::co_spawn(loop_io, asio_loop(1000, warm), asio::detached);
		loop_io.run();
		double asio_best = 1e300;
		std::size_t asio_allocs = 0;
		for (int trial = 0; trial < 5; ++trial) {
			loop_io.restart();
			long sum = 0;
			std::size_t before = allocations;
			auto t = std::chrono::steady_clock::now();
			asio::co_spawn(loop_io, asio_loop(n, sum), asio::detached);
			loop_io.run();
			double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t).count() / double(n);
			asio_allocs = allocations - before;
			if (sum != n) { return 1; }
			asio_best = std::min(asio_best, ns);
		}
		if (TASK_COUNTS_ALLOCATIONS) {
			std::printf("asio::awaitable no-op create+await+destroy: %.2f ns, %zu allocations for %ld awaitables\n", asio_best, asio_allocs, n);
		} else {
			std::printf("asio::awaitable no-op create+await+destroy: %.2f ns (allocations not counted under TSan)\n", asio_best);
		}
	}

	asio::io_context io;
	task::asio_scheduler as(io.get_executor());
	std::vector<double> latencies;
	latencies.reserve(200000);
	auto yields = [&]() -> task::task<void> {
		for (int i = 0; i < 200000; ++i) {
			auto t = task::cycles::read();
			co_await task::yield();
			latencies.push_back(double(task::cycles::read() - t) / task::cycles::per_ns());
		}
	};
	asio::co_spawn(io, [&]() -> asio::awaitable<void> { co_await task::as_awaitable(as, yields()); }, asio::detached);
	io.run();
	std::sort(latencies.begin(), latencies.end());
	auto at = [&](double q) { return latencies[std::size_t(q * double(latencies.size() - 1))]; };
	std::printf("task::yield through asio_scheduler, yield->resume: p50 %.0f ns, p99 %.0f ns, p99.9 %.0f ns\n", at(0.5), at(0.99), at(0.999));

	// The comparator: a co_await of asio::post(executor, use_awaitable) in an asio::awaitable.
	asio::io_context post_io;
	std::vector<double> post_latencies;
	post_latencies.reserve(200000);
	asio::co_spawn(post_io, [&]() -> asio::awaitable<void> {
		for (int i = 0; i < 200000; ++i) {
			auto t = task::cycles::read();
			co_await asio::post(post_io, asio::use_awaitable);
			post_latencies.push_back(double(task::cycles::read() - t) / task::cycles::per_ns());
		}
	}, asio::detached);
	post_io.run();
	if (post_latencies.size() != latencies.size()) { return 1; }
	std::sort(post_latencies.begin(), post_latencies.end());
	auto post_at = [&](double q) { return post_latencies[std::size_t(q * double(post_latencies.size() - 1))]; };
	std::printf("asio::post(use_awaitable) post->resume: p50 %.0f ns, p99 %.0f ns, p99.9 %.0f ns\n", post_at(0.5), post_at(0.99), post_at(0.999));
#endif
}
