// Shared by the asio_test pieces: the includes, CHECK, and the helpers more than one piece uses.

#pragma once

#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "task_asio.hh"

#include <asio/associated_cancellation_slot.hpp>
#include <asio/dispatch.hpp>
#include <asio/local/connect_pair.hpp>
#include <asio/local/stream_protocol.hpp>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <tuple>
#include <random>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#define HAVE_FORK 1
#endif

using namespace std::chrono_literals;

// Sanitizer runtimes replace operator new themselves, so only plain builds count allocations.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define TEST_COUNTS_NEW 0
#endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TEST_COUNTS_NEW 0
#endif
#ifndef TEST_COUNTS_NEW
#define TEST_COUNTS_NEW 1
#endif

extern int failures;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); ++failures; } \
	} while (0)

// ---------------------------------------------------------------------------------------------
// await_asio with a token (D5), forwarding wrappers (D7), teardown (D8), sizes (T5)

using sock = asio::local::stream_protocol::socket;
using read_sig = void(asio::error_code, std::size_t);

struct destroy_mark {
	std::atomic<int>* n;
	~destroy_mark() { ++*n; }
};

// A loop on its own thread; destroying it is phase 4 (scheduler, then io_context).
struct loop {
	asio::io_context io{1};
	asio::executor_work_guard<asio::io_context::executor_type> guard = asio::make_work_guard(io);
	std::unique_ptr<task::asio_scheduler> sched = std::make_unique<task::asio_scheduler>(io.get_executor());
	std::thread thread;
	task::scheduler_ref ref() { return task::scheduler_ref::of(*sched); }
	void start() { thread = std::thread([this] { io.run(); }); }
	void stop() {
		guard.reset();
		io.stop();
		if (thread.joinable()) { thread.join(); }
	}
};

// Defined in the piece that owns them.
task::task<void> demote_once();
void spin(std::chrono::nanoseconds d);
void wait_for(const std::atomic<bool>& flag);
void test_primitives();
void test_await_token();
void test_await_token_move_only();
void test_inline_resume();
void test_inline_top_level();
void test_wrapper();
void test_teardown();
