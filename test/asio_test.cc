// task_asio: an asio::awaitable awaits tasks that yield, demote, and throw; they run
// on the io_context thread and hand results back through Asio.
#include "task_asio.hh"

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#define HAVE_FORK 1
#endif

using namespace std::chrono_literals;

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


// A scheduler that counts demotions and otherwise behaves like asio_scheduler's post().
struct counting_scheduler {
	explicit counting_scheduler(asio::any_io_executor e) : executor(std::move(e)) {}
	void post(std::coroutine_handle<> h) {
		asio::post(executor, [this, h] {
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		});
	}
	void post_demoted(std::coroutine_handle<> h) {
		++demotions;
		post(h);
	}
	std::uint64_t now() const noexcept { return 0; }
	task::scheduler_ref ref() { return task::scheduler_ref::of(*this); }
	asio::any_io_executor executor;
	int demotions = 0;
};

void spin(std::chrono::nanoseconds d) {
	auto end = std::chrono::steady_clock::now() + d;
	while (std::chrono::steady_clock::now() < end) {}
}

task::task<void> demote_once() {
	task::budget b(std::chrono::nanoseconds(1));
	co_await b.checkpoint();
}
task::task<void> observe_demoted(task::asio_scheduler& s, std::size_t* seen) {
	*seen = s.demoted();
	co_return;
}

// The slice restarts when a task resumes after an await_asio, an event or a lock.
task::task<void> slice_after_asio(counting_scheduler& cs, asio::io_context& io, int* seen) {
	co_await task::schedule_on{cs.ref()};
	task::budget b(200us);
	asio::steady_timer t(io);
	t.expires_after(3ms);
	asio::error_code ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
	int before = cs.demotions;
	co_await b.checkpoint();
	*seen = ec ? -1 : cs.demotions - before;
}
task::task<void> slice_after_event(counting_scheduler& cs, task::event& e, int* seen) {
	co_await task::schedule_on{cs.ref()};
	task::budget b(200us);
	co_await e;
	int before = cs.demotions;
	co_await b.checkpoint();
	*seen = cs.demotions - before;
}
task::task<void> slice_after_lock(counting_scheduler& cs, task::async_mutex& m, int* seen) {
	co_await task::schedule_on{cs.ref()};
	task::budget b(200us);
	auto g = co_await m.lock();
	int before = cs.demotions;
	co_await b.checkpoint();
	*seen = cs.demotions - before;
}
task::task<void> slice_after_busy(counting_scheduler& cs, int* seen) {
	co_await task::schedule_on{cs.ref()};
	task::budget b(200us);
	spin(1ms);
	int before = cs.demotions;
	co_await b.checkpoint();
	*seen = cs.demotions - before;
}

task::task<std::size_t> read_some(asio::ip::tcp::socket& s, char* buf, std::size_t n, asio::error_code* ec_out) {
	auto [ec, got] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { s.async_read_some(asio::buffer(buf, n), std::move(h)); });
	*ec_out = ec;
	co_return got;
}
task::task<void> write_after(asio::io_context& io, asio::ip::tcp::socket& s, std::string data, bool close_after) {
	asio::steady_timer t(io);
	t.expires_after(2ms);
	co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
	co_await task::await_asio<void(asio::error_code, std::size_t)>(
		[&](auto h) { asio::async_write(s, asio::buffer(data), std::move(h)); });
	if (close_after) { s.close(); }
}

struct outcome {
	int got = 0;  // 1 holds the lock, 2 timed out
	std::chrono::steady_clock::duration waited{};
};
task::task<void> timed_lock(task::asio_scheduler& s, task::async_mutex& m, std::chrono::nanoseconds timeout, outcome* out) {
	co_await task::schedule_on{task::scheduler_ref::of(s)};
	auto start = std::chrono::steady_clock::now();
	auto g = co_await m.lock_for(s, timeout);
	out->waited = std::chrono::steady_clock::now() - start;
	out->got = g ? 1 : 2;
}

// Several reactors contend for one mutex with short random deadlines; every attempt is
// accounted for as a grant or a timeout, and grants never overlap.
task::task<void> contender(task::asio_scheduler& s, asio::io_context& io, task::async_mutex& m, unsigned seed, int rounds,
	std::atomic<int>* granted, std::atomic<int>* timed_out, std::atomic<int>* inside, std::atomic<bool>* overlapped, std::atomic<int>* done) {
	co_await task::schedule_on{task::scheduler_ref::of(s)};
	std::mt19937 rng(seed);
	for (int i = 0; i < rounds; ++i) {
		auto g = co_await m.lock_for(s, std::chrono::microseconds(rng() % 300));
		if (!g) {
			++*timed_out;
			continue;
		}
		++*granted;
		if (inside->fetch_add(1) != 0) { *overlapped = true; }
		if (rng() % 2) {
			asio::steady_timer t(io);
			t.expires_after(std::chrono::microseconds(rng() % 100));
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
		} else {
			co_await task::yield();
		}
		inside->fetch_sub(1);
	}
	++*done;
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
void scheduler_clean() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	task::spawn(task::scheduler_ref::of(s), demote_once());
	io.run();
}
#endif

void test_primitives() {
	// demoted() counts the handles waiting in the demoted queue, and returns to zero.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		std::size_t seen = 99;
		for (int i = 0; i < 5; ++i) { task::spawn(r, demote_once()); }
		task::spawn(r, observe_demoted(s, &seen));  // runs before the first drain pass
		CHECK(s.demoted() == 0);
		io.run();
		CHECK(seen == 5);
		CHECK(s.demoted() == 0);
	}
	// A task resumed by an await_asio completion, an event or a lock handoff starts a fresh slice.
	{
		asio::io_context io;
		counting_scheduler cs(io.get_executor());
		int by_asio = -1, by_event = -1, by_lock = -1, by_busy = -1;
		task::event e;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::spawn(cs.ref(), slice_after_asio(cs, io, &by_asio));
		task::spawn(cs.ref(), slice_after_event(cs, e, &by_event));
		task::spawn(cs.ref(), slice_after_lock(cs, m, &by_lock));
		task::spawn(cs.ref(), slice_after_busy(cs, &by_busy));
		asio::steady_timer t(io);
		t.expires_after(4ms);
		t.async_wait([&](const asio::error_code&) {
			e.set();
			m.unlock();
		});
		io.run();
		CHECK(by_asio == 0);
		CHECK(by_event == 0);
		CHECK(by_lock == 0);
		CHECK(by_busy == 1);  // a long continuous run is still demoted
		CHECK(m.try_lock());
		m.unlock();
	}
	// await_asio: results, errors, and the completion arriving on another thread.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		asio::ip::tcp::acceptor acceptor(io, {asio::ip::address_v4::loopback(), 0});
		asio::ip::tcp::socket client(io), server(io);
		client.connect(acceptor.local_endpoint());
		acceptor.accept(server);
		char buf[16] = {};
		asio::error_code ec1, ec2;
		std::size_t n1 = 0, n2 = 99;
		auto reader = [&]() -> task::task<void> {
			n1 = co_await read_some(server, buf, sizeof buf, &ec1);
			n2 = co_await read_some(server, buf, sizeof buf, &ec2);  // the peer closed: end of file
		};
		task::spawn(task::scheduler_ref::of(s), reader());
		task::spawn(task::scheduler_ref::of(s), write_after(io, client, "hello", true));
		io.run();
		CHECK(!ec1 && n1 == 5 && std::string(buf, 5) == "hello");
		CHECK(ec2 == asio::error::eof && n2 == 0);

		asio::io_context io2;
		task::asio_scheduler s2(io2.get_executor());
		asio::steady_timer t(io2);
		t.expires_after(1h);
		asio::error_code ec3;
		auto aborted = [&]() -> task::task<void> {
			ec3 = co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
		};
		task::spawn(task::scheduler_ref::of(s2), aborted());
		io2.poll();
		t.cancel();
		io2.run();
		CHECK(ec3 == asio::error::operation_aborted);

		// A completion on another thread resumes the task on its scheduler's thread.
		asio::io_context io3;
		auto guard = asio::make_work_guard(io3);
		task::asio_scheduler s3(io3.get_executor());
		std::thread t3([&] { io3.run(); });
		std::thread::id ran_on, io3_id = t3.get_id();
		std::atomic<bool> finished{false};
		auto other_thread = [&]() -> task::task<void> {
			int v = co_await task::await_asio<void(int)>([&](auto h) {
				std::thread([h = std::move(h)]() mutable { std::this_thread::sleep_for(2ms); h(41); }).detach();
			});
			ran_on = std::this_thread::get_id();
			finished = v == 41;
		};
		task::spawn(task::scheduler_ref::of(s3), other_thread());
		for (int i = 0; i < 2000 && !finished; ++i) { std::this_thread::sleep_for(1ms); }
		CHECK(finished && ran_on == io3_id);
		guard.reset();
		t3.join();
	}
	// lock_until on real timers: a timeout, a grant before the deadline, and a free lock.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		task::async_mutex m;
		CHECK(m.try_lock());
		outcome timed, granted, free_lock;
		task::spawn(task::scheduler_ref::of(s), timed_lock(s, m, 20ms, &timed));
		io.run();  // nobody unlocks: the deadline passes
		CHECK(timed.got == 2);
		CHECK(timed.waited >= 20ms);
		asio::io_context io2;
		task::asio_scheduler s2(io2.get_executor());
		task::spawn(task::scheduler_ref::of(s2), timed_lock(s2, m, 5s, &granted));
		io2.poll();  // the waiter queues behind the first, dead entry
		m.unlock();  // skips the dead entry; the waiter gets the lock
		io2.run();
		CHECK(granted.got == 1);
		CHECK(granted.waited < 5s);
		CHECK(m.try_lock());
		m.unlock();
		task::spawn(task::scheduler_ref::of(s2), timed_lock(s2, m, 0ns, &free_lock));
		io2.restart();
		io2.run();
		CHECK(free_lock.got == 1);
	}
	// Reactors contending with random deadlines.
	{
		constexpr int reactors = 4, per_reactor = 8, rounds = 150;
		std::vector<std::unique_ptr<asio::io_context>> ios;
		std::vector<std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>> guards;
		std::vector<std::unique_ptr<task::asio_scheduler>> scheds;
		std::vector<std::thread> threads;
		for (int i = 0; i < reactors; ++i) {
			ios.push_back(std::make_unique<asio::io_context>(1));
			guards.push_back(std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(ios.back()->get_executor()));
			scheds.push_back(std::make_unique<task::asio_scheduler>(ios.back()->get_executor()));
		}
		for (int i = 0; i < reactors; ++i) { threads.emplace_back([&, i] { ios[std::size_t(i)]->run(); }); }
		{
			task::async_mutex m;
			std::atomic<int> granted{0}, timed_out{0}, inside{0}, done{0};
			std::atomic<bool> overlapped{false};
			for (int i = 0; i < reactors * per_reactor; ++i) {
				auto k = std::size_t(i % reactors);
				task::spawn(task::scheduler_ref::of(*scheds[k]),
					contender(*scheds[k], *ios[k], m, unsigned(i + 1), rounds, &granted, &timed_out, &inside, &overlapped, &done));
			}
			for (int i = 0; i < 20000 && done.load() < reactors * per_reactor; ++i) { std::this_thread::sleep_for(1ms); }
			CHECK(done.load() == reactors * per_reactor);
			CHECK(granted + timed_out == reactors * per_reactor * rounds);
			CHECK(granted > 0 && timed_out > 0);
			CHECK(!overlapped.load());
			CHECK(m.try_lock());
			m.unlock();
		}
		guards.clear();
		for (auto& t : threads) { t.join(); }
	}
}

int main() {
#if defined(HAVE_FORK) && !defined(NDEBUG)
	CHECK(!aborts(scheduler_clean));
	CHECK(aborts(scheduler_with_demoted));  // destroying a scheduler with a demoted handle queued
#endif
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
	test_primitives();
	std::printf("task_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
