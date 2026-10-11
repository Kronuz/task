// task_asio: an asio::awaitable awaits tasks that yield, demote, and throw; they run
// on the io_context thread and hand results back through Asio.
#include "task_asio.hh"

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
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

task::task<void> explode() {
	co_await task::yield();
	throw std::logic_error("nope");
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
	std::printf("task_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
