// task_asio: tasks on an asio_scheduler, await_asio with and without tokens, and teardown.
#include "asio_test_support.hh"

#if TEST_COUNTS_NEW
static std::atomic<std::size_t> allocations{0};
void* operator new(std::size_t n) {
	allocations.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
#endif

int failures = 0;

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

task::task<void> demote_once() {
	task::budget b(std::chrono::nanoseconds(1));
	co_await b.checkpoint();
}

#if defined(HAVE_FORK) && !defined(NDEBUG)
bool aborts(void (*f)()) {
	pid_t pid = fork();
	if (pid == 0) {
		std::freopen("/dev/null", "w", stderr);
		f();
		_exit(0);
	}
	int status = 0;
	waitpid(pid, &status, 0);
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}
void scheduler_with_demoted() {
	asio::io_context io;
	auto* s = new task::asio_scheduler(io.get_executor());
	task::spawn(task::scheduler_ref::of(*s), demote_once());
	io.run_one();  // the task demotes itself; the drain pass has not run
	delete s;
}
void scheduler_stopped_with_demoted() {  // the destructor destroys the root itself
	asio::io_context io;
	auto* s = new task::asio_scheduler(io.get_executor());
	task::spawn(task::scheduler_ref::of(*s), demote_once());
	io.run_one();
	io.stop();
	delete s;
}
void scheduler_clean() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	task::spawn(task::scheduler_ref::of(s), demote_once());
	io.run();
}
#endif

#if defined(HAVE_FORK) && !defined(NDEBUG)
// A cross-thread cancel of a token-bound await with no current scheduler is lost: Debug aborts.
void cancel_without_scheduler() {
	asio::io_context io;
	auto guard = asio::make_work_guard(io);
	std::thread loop([&] { io.run(); });
	task::cancel_source src;
	asio::steady_timer timer(io, 10s);
	std::thread requester([&] {
		std::this_thread::sleep_for(50ms);
		src.request();
	});
	task::sync_wait([&]() -> task::task<void> {
		co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, src.token());
	}());
	requester.join();
	guard.reset();
	loop.join();
}
// close() and destroy_roots() run after the loops joined: not from a root on the loop.
void close_on_loop() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	task::spawn(task::scheduler_ref::of(s), [](task::asio_scheduler& sch) -> task::task<void> {
		sch.close();
		co_return;
	}(s));
	io.run();
}
void destroy_roots_on_running_loop() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	task::spawn(task::scheduler_ref::of(s), demote_once());
	io.run_one();  // the task demotes itself; the loop is not stopped
	s.close();
	s.destroy_roots();
}
#endif

void test_sizes() {
	auto init = [](auto) {};
	using plain = decltype(task::await_asio<read_sig>(init));
	using with_token = decltype(task::await_asio<read_sig>(init, task::cancel_token{}));
	std::printf("sizes: scheduler_ref %zu, cancel_callback(lock) %zu, await_asio awaiter %zu, with a token %zu, lock(token) awaiter %zu\n",
		sizeof(task::scheduler_ref), sizeof(task::cancel_callback<task::detail::lock_cancel>), sizeof(plain), sizeof(with_token),
		sizeof(task::async_mutex::cancellable_lock_awaiter));
	std::printf("sizes: deadline<asio_scheduler> %zu, event awaiter %zu\n", sizeof(task::deadline<task::asio_scheduler>),
		sizeof(decltype(std::declval<task::event&>().operator co_await())));
	static_assert(sizeof(task::scheduler_ref) == 16);
	static_assert(sizeof(task::cancel_callback<task::detail::lock_cancel>) <= 48);
	static_assert(sizeof(plain) <= 72);
	static_assert(sizeof(with_token) <= 160);
#if TEST_COUNTS_NEW
	// Steady-state reads allocate nothing, with an empty token or an unrequested one.
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	sock a(io), b(io);
	asio::local::connect_pair(a, b);
	std::size_t allocs[2] = {99, 99};
	auto reads = [&](bool token, std::size_t* out) -> task::task<void> {
		task::cancel_source src;
		char c = 'x', buf[1];
		for (int i = 0; i < 10100; ++i) {
			if (i == 100) { *out = allocations.load(); }
			asio::write(b, asio::buffer(&c, 1));
			auto start = [&](auto h) { a.async_read_some(asio::buffer(buf), std::move(h)); };
			auto [ec, n] = token ? co_await task::await_asio<read_sig>(start, src.token()) : co_await task::await_asio<read_sig>(start);
			CHECK(!ec && n == 1);
		}
		*out = allocations.load() - *out;
	};
	task::spawn(task::scheduler_ref::of(s), reads(false, &allocs[0]));
	io.run();
	io.restart();
	task::spawn(task::scheduler_ref::of(s), reads(true, &allocs[1]));
	io.run();
	std::printf("operator new per 10,000 reads: empty token %zu, unrequested token %zu\n", allocs[0], allocs[1]);
	CHECK(allocs[0] == 0 && allocs[1] == 0);
#endif
}

int main() {
#if defined(HAVE_FORK) && !defined(NDEBUG)
	CHECK(!aborts(scheduler_clean));
	CHECK(aborts(scheduler_with_demoted));  // live roots and a loop that was not stopped
	CHECK(!aborts(scheduler_stopped_with_demoted));
	CHECK(aborts(cancel_without_scheduler));
	CHECK(aborts(close_on_loop));
	CHECK(aborts(destroy_roots_on_running_loop));
#endif
	// A task on the io_context awaits tasks that yield, demote and throw.
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::thread::id where;
	int result = -1;
	bool caught = false;
	auto body = [&]() -> task::task<void> {
		result = co_await work(100, &where);
		try {
			co_await explode();
		} catch (const std::logic_error&) {
			caught = true;
		}
	};
	task::spawn(task::scheduler_ref::of(s), body());
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
	test_primitives();
	test_await_token();
	test_await_token_move_only();
	test_inline_resume();
	test_inline_top_level();
	test_wrapper();
	test_teardown();
	test_sizes();
	std::printf("task_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
