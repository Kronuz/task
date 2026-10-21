// task_asio: an asio::awaitable awaits tasks that yield, demote, and throw; they run
// on the io_context thread and hand results back through Asio.
#include "task_asio.hh"

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <thread>

static int failures = 0;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); ++failures; } \
	} while (0)

task::task<int> work(int n, std::thread::id* where) {
	task::budget b(std::chrono::nanoseconds(1));  // every checkpoint demotes
	int sum = 0;
	for (int i = 0; i < n; ++i) {
		co_await task::yield();
		co_await b.checkpoint();
		sum += i;
	}
	*where = std::this_thread::get_id();
	co_return sum;
}

task::task<std::thread::id> on_io(task::asio_scheduler& s) {
	co_await task::schedule_on{task::scheduler_ref::of(s)};
	task::budget b(std::chrono::nanoseconds(1));
	for (int i = 0; i < 10; ++i) {
		co_await task::yield();
		co_await b.checkpoint();
	}
	co_return std::this_thread::get_id();
}

task::task<void> explode() {
	co_await task::yield();
	throw std::logic_error("nope");
}

task::task<void> explode_on(task::asio_scheduler& s) {
	co_await task::schedule_on{task::scheduler_ref::of(s)};
	co_await explode();
}

int main() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::thread::id where;
	int result = -1;
	bool caught = false;
	asio::co_spawn(io, [&]() -> asio::awaitable<void> {
		result = co_await task::as_awaitable(s, work(100, &where));
		try {
			co_await task::as_awaitable(s, explode());
		} catch (const std::logic_error&) {
			caught = true;
		}
	}, asio::detached);
	io.run();
	CHECK(result == 4950);
	CHECK(where == std::this_thread::get_id());
	CHECK(caught);

	// sync_wait from a thread outside the io_context, which runs on another thread.
	{
		asio::io_context io2;
		auto guard = asio::make_work_guard(io2);
		task::asio_scheduler s2(io2.get_executor());
		std::thread runner([&] { io2.run(); });
		for (int i = 0; i < 100; ++i) { CHECK(task::sync_wait(on_io(s2)) == runner.get_id()); }
		bool threw = false;
		try { task::sync_wait(explode_on(s2)); } catch (const std::logic_error&) { threw = true; }
		CHECK(threw);
		guard.reset();
		runner.join();
	}
	std::printf("task_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
