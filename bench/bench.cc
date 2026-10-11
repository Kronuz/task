// task gate numbers: a no-op task's create/await/destroy cost and allocations, and
// (TASK_BENCH_ASIO) yield-to-resume latency through the Asio scheduler on an idle loop.
#include "task.hh"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <vector>

#if TASK_BENCH_ASIO
#include "task_asio.hh"
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#endif

static std::size_t allocations = 0;
void* operator new(std::size_t n) {
	++allocations;
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

task::task<int> noop() { co_return 1; }

task::task<long> loop(long n) {
	long sum = 0;
	for (long i = 0; i < n; ++i) { sum += co_await noop(); }
	co_return sum;
}

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
	std::printf("no-op task create+await+destroy: %.2f ns, %zu allocations for %ld tasks (outer task and run scaffolding included)\n", best, allocs, n);

#if TASK_BENCH_ASIO
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
	std::printf("asio yield->resume: p50 %.0f ns, p99 %.0f ns, p99.9 %.0f ns\n", at(0.5), at(0.99), at(0.999));
#endif
}
