// task_asio: tasks on an asio_scheduler, await_asio with and without tokens, and teardown.
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

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
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
	task::deadline dl(s, start + timeout);
	auto g = co_await m.lock(dl.token());
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
		task::deadline dl(s, std::chrono::steady_clock::now() + std::chrono::microseconds(rng() % 300));
		auto g = co_await m.lock(dl.token());
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
		std::thread completer;
		auto other_thread = [&]() -> task::task<void> {
			int v = co_await task::await_asio<void(int)>([&](auto h) {
				completer = std::thread([h = std::move(h)]() mutable { std::this_thread::sleep_for(2ms); h(41); });
			});
			ran_on = std::this_thread::get_id();
			finished = v == 41;
		};
		task::spawn(task::scheduler_ref::of(s3), other_thread());
		for (int i = 0; i < 2000 && !finished; ++i) { std::this_thread::sleep_for(1ms); }
		CHECK(finished && ran_on == io3_id);
		if (completer.joinable()) completer.join();  // its tail must end before io3 is destroyed
		guard.reset();
		t3.join();
	}
	// deadline on a real timer: requested with cancel_reason::deadline at its time.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto start = std::chrono::steady_clock::now();
		task::deadline dl(s, start + 20ms);
		auto t = dl.token();
		io.run();
		CHECK(dl.expired() && t.reason() == task::cancel_reason::deadline);
		CHECK(std::chrono::steady_clock::now() - start >= 20ms);
	}
	// A deadline may be destroyed on any thread, before or after its expiry, while its loop runs;
	// once destroyed it requests nothing more.
	{
		asio::io_context io;
		auto guard = asio::make_work_guard(io);
		task::asio_scheduler s(io.get_executor());
		std::thread runner([&] { io.run(); });
		for (int i = 0; i < 300; ++i) {
			auto dl = std::make_unique<task::deadline<task::asio_scheduler>>(s, std::chrono::steady_clock::now() + std::chrono::microseconds(i % 4 * 80));
			auto token = dl->token();
			std::this_thread::sleep_for(std::chrono::microseconds(i % 5 * 40));
			std::thread([&] { dl.reset(); }).join();
			const bool after_destroy = token.requested();
			std::this_thread::sleep_for(std::chrono::microseconds(300));
			CHECK(token.requested() == after_destroy);
		}
		guard.reset();
		runner.join();
	}
	// late_fire: a handler that runs after the deadline's expiry was queued destroys it; the
	// expiry still runs and requests nothing.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto at = std::chrono::steady_clock::now() + 5ms;
		asio::steady_timer first(io, at);
		std::unique_ptr<task::deadline<task::asio_scheduler>> dl;
		bool first_ran = false;
		first.async_wait([&](asio::error_code) {
			first_ran = true;
			dl.reset();
		});
		dl = std::make_unique<task::deadline<task::asio_scheduler>>(s, at);
		auto t = dl->token();
		std::this_thread::sleep_for(10ms);  // both expire before the loop looks
		io.run();
		CHECK(first_ran && !dl);
		CHECK(!t.requested() && t.reason() == task::cancel_reason::none);
	}
	// lock(deadline token) on real timers: a timeout, a grant before the deadline, and a free lock.
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

// Phases 1-3 across every loop.
void teardown(std::initializer_list<loop*> loops) {
	for (auto* l : loops) { l->stop(); }
	for (auto* l : loops) { l->sched->close(); }
	for (auto* l : loops) { l->sched->destroy_roots(); }
}

void wait_for(const std::atomic<bool>& flag) {
	for (int i = 0; i < 20000 && !flag; ++i) { std::this_thread::sleep_for(100us); }
}

task::task<void> sleep_on(asio::io_context& io, std::atomic<int>* destroyed, std::atomic<bool>* parked, task::cancel_token t = {}) {
	destroy_mark m{destroyed};
	asio::steady_timer timer(io, 1h);
	*parked = true;
	co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, std::move(t));
}

task::task<void> read_with(sock& s, task::cancel_token t, asio::error_code* ec, std::size_t* n, std::atomic<int>* done) {
	char b[8];
	std::tie(*ec, *n) = co_await task::await_asio<read_sig>([&](auto h) { s.async_read_some(asio::buffer(b), std::move(h)); }, std::move(t));
	++*done;
}

// A request made on the loop's thread inside init: before the operation starts or after. The
// emit has to wait until the slot holds the operation (D5.3), so the await ends aborted.
task::task<void> request_in_init(asio::steady_timer& timer, task::cancel_source& src, bool before, bool posted, asio::error_code* ec,
	std::atomic<int>* done) {
	auto request = [&] {
		if (posted) {
			std::thread([&] { src.request(); }).join();  // from another thread: a posted emit
		} else {
			src.request();  // on the loop: pending until the operation is in the slot
		}
	};
	*ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) {
		if (before) { request(); }
		timer.async_wait(std::move(h));
		if (!before) { request(); }
	}, src.token());
	++*done;
}

// A cross-thread request that races the operation's own completion. kind 0: a timer; 1: the
// handler runs inside init through a dispatch; 2: init throws.
struct race_stats {
	std::atomic<int> aborted{0}, ok{0}, thrown{0}, other{0}, finished{0};
};
task::task<void> race_one(asio::io_context& io, task::cancel_token t, int kind, race_stats* st) {
	try {
		asio::steady_timer timer(io, std::chrono::microseconds(100 + (kind % 8) * 50));
		asio::error_code ec;
		if (kind % 3 == 1) {
			ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) {
				asio::dispatch(io, [h = std::move(h)]() mutable { std::move(h)(asio::error_code{}); });
			}, t);
		} else if (kind % 3 == 2) {
			co_await task::await_asio<void(asio::error_code)>([&](auto) { throw std::runtime_error("init"); }, t);
		} else {
			ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, t);
		}
		if (!ec) {
			++st->ok;
		} else if (ec == asio::error::operation_aborted) {
			++st->aborted;
		} else {
			++st->other;
		}
	} catch (const std::runtime_error&) {
		++st->thrown;
	}
	++st->finished;
}

void test_await_token() {
	const auto base = task::detail::live_frames();
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	auto r = task::scheduler_ref::of(s);
	// A requested token never calls init: before the await, and between registration and the
	// re-check, on the loop or from another thread (NF1: the posted emit outlives nothing).
	for (int mode = 0; mode < 3; ++mode) {
		task::cancel_source src;
		if (mode == 0) { src.request(); }
		bool called = false;
		asio::error_code ec;
		std::size_t n = 7;
		std::atomic<int> done{0};
		auto body = [&]() -> task::task<void> {
			std::tie(ec, n) = co_await task::await_asio<read_sig>([&](auto) { called = true; }, src.token());
			++done;
		};
		static task::cancel_source* hooked;
		hooked = &src;
		if (mode == 1) { task::detail::test_hooks::asio_registered = [] { hooked->request(); }; }
		if (mode == 2) { task::detail::test_hooks::asio_registered = [] { std::thread([] { hooked->request(); }).join(); }; }
		task::spawn(r, body());
		io.restart();
		io.run();
		task::detail::test_hooks::asio_registered = nullptr;
		CHECK(!called && done == 1 && ec == asio::error::operation_aborted && n == 0);
	}
	// init throws after a request from another thread posted its emit: the exception is
	// rethrown once the emit has run, and without a request at once.
	for (bool request : {false, true}) {
		task::cancel_source src;
		bool caught = false;
		auto body = [&]() -> task::task<void> {
			try {
				co_await task::await_asio<void(asio::error_code)>([&](auto) {
					if (request) { std::thread([&] { src.request(); }).join(); }
					throw std::runtime_error("init");
				}, src.token());
			} catch (const std::runtime_error&) {
				caught = true;
			}
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(caught);
	}
	// A request inside init, before and after the operation starts, on the loop's thread (emitted
	// once init returns) and from another thread (a posted emit): the operation ends aborted.
	for (int mode = 0; mode < 4; ++mode) {
		task::cancel_source src;
		asio::steady_timer timer(io, 1h);
		asio::error_code ec;
		std::atomic<int> done{0};
		task::spawn(r, request_in_init(timer, src, mode & 1, mode & 2, &ec, &done));
		io.restart();
		io.run_for(10s);
		CHECK(done == 1 && ec == asio::error::operation_aborted);
	}
	// A handler run inside init (a dispatch on the loop, a direct call), with and without a
	// token: the continuation runs after init returned, and the root ends there.
	for (int mode = 0; mode < 4; ++mode) {
		const bool token = mode & 1, dispatch = mode & 2;
		task::cancel_source src;
		bool returned = false, ok = false;
		auto init = [&](auto h) {
			if (dispatch) {
				asio::dispatch(io.get_executor(), [h = std::move(h)]() mutable { h(asio::error_code{}); });
			} else {
				h(asio::error_code{});
			}
			returned = true;
		};
		auto body = [&]() -> task::task<void> {
			if (token) {
				co_await task::await_asio<void(asio::error_code)>(init, src.token());
			} else {
				co_await task::await_asio<void(asio::error_code)>(init);
			}
			ok = returned;
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(ok);
		if (!dispatch) {  // no scheduler at all: the handler cannot post, it only drops its count
			ok = returned = false;
			task::sync_wait(body());
			CHECK(ok);
		}
	}
	sock a(io), b(io);
	asio::local::connect_pair(a, b);
	// Requested on the loop while the read is pending: operation_aborted.
	{
		task::cancel_source src;
		asio::error_code ec;
		std::size_t n = 7;
		std::atomic<int> done{0};
		task::spawn(r, read_with(a, src.token(), &ec, &n, &done));
		io.restart();
		io.poll();
		CHECK(done == 0);
		asio::post(io, [&] { src.request(); });
		int ran = 0;
		while (done == 0 && ran < 10) { ran += int(io.run_one()); }
		CHECK(done == 1 && ec == asio::error::operation_aborted && n == 0);
		CHECK(ran <= 3);  // the request, the handler, the resume (C3's inline resume drops the last)
	}
	// A cancel that loses the race: the data was there, the read completed during initiation,
	// and the request inside init is emitted after: the real result comes back.
	{
		asio::write(b, asio::buffer("abcd", 4));
		task::cancel_source src;
		asio::error_code ec = asio::error::would_block;
		std::size_t n = 0;
		auto body = [&]() -> task::task<void> {
			char buf[8];
			std::tie(ec, n) = co_await task::await_asio<read_sig>([&](auto h) {
				a.async_read_some(asio::buffer(buf), std::move(h));
				src.request();
			}, src.token());
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(!ec && n == 4 && src.requested());
	}
	// An empty token binds no slot; a live one does.
	for (bool live : {false, true}) {
		task::cancel_source src;
		int connected = -1;
		auto body = [&]() -> task::task<void> {
			co_await task::await_asio<void(asio::error_code)>([&](auto h) {
				connected = asio::get_associated_cancellation_slot(h).is_connected();
				h(asio::error_code{});
			}, live ? src.token() : task::cancel_token{});
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(connected == int(live));
	}
	// Requested from another thread while the read is pending on a running loop: the frame
	// ends once, after the posted emit (TSan, ASan).
	{
		loop l;
		sock c(l.io), d(l.io);
		asio::local::connect_pair(c, d);
		l.start();
		constexpr int n = 10000;
		std::atomic<int> done{0};
		int aborted = 0;
		for (int i = 0; i < n; ++i) {
			task::cancel_source src;
			asio::error_code ec;
			std::size_t got = 0;
			task::spawn(l.ref(), read_with(c, src.token(), &ec, &got, &done));
			for (int k = 0; k < i % 3; ++k) { std::this_thread::yield(); }
			src.request();
			while (done.load() != i + 1) { std::this_thread::yield(); }
			aborted += ec == asio::error::operation_aborted;
		}
		CHECK(aborted == n);
		l.stop();
	}
	// Cross-thread requests race the operation's completion (random spins), with a timer, a
	// handler inside init and a throwing init: every task ends once, only as ok, aborted or
	// thrown, and no frame outlives its await (the handler's cb_.reset(), ASan).
	{
		loop l;
		l.start();
		constexpr int rounds = 20000;
		race_stats st;
		std::mt19937 rng(42);
		for (int i = 0; i < rounds; ++i) {
			task::cancel_source src;
			task::spawn(l.ref(), race_one(l.io, src.token(), i, &st));
			int spin = int(rng() % 400);
			for (volatile int k = 0; k < spin * 20; k = k + 1) {}
			src.request(task::cancel_reason::shutdown);
			if ((i & 63) == 0) { std::this_thread::yield(); }
		}
		while (st.finished.load() < rounds) { std::this_thread::sleep_for(1ms); }
		l.stop();
		CHECK(st.other == 0 && st.finished == rounds && st.ok + st.aborted + st.thrown == rounds);
		std::printf("race: ok %d aborted %d thrown %d\n", st.ok.load(), st.aborted.load(), st.thrown.load());
	}
	CHECK(task::detail::live_frames() == base);
}

using tcp = asio::ip::tcp;
using accept_sig = void(asio::error_code, tcp::socket);

struct accepted {
	asio::error_code ec = asio::error::would_block;
	bool engaged = false, open = false, called = false;
	std::uint16_t peer_port = 0;
};
task::task<void> accept_with(tcp::acceptor& a, task::cancel_token t, accepted* out, std::atomic<int>* done) {
	auto [ec, peer] = co_await task::await_asio<accept_sig>([&](auto h) {
		out->called = true;
		a.async_accept(std::move(h));
	}, std::move(t));
	out->ec = ec;
	out->engaged = peer.has_value();
	out->open = peer && peer->is_open();
	if (out->open) { out->peer_port = peer->remote_endpoint().port(); }
	++*done;
}

// A completion argument without a default constructor (a socket): await_asio returns it as a
// std::optional with or without a token, empty only when a request came before the start.
void test_await_token_move_only() {
	const auto base = task::detail::live_frames();
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<accept_sig>(0, task::cancel_token{}))&>().await_resume()),
		std::tuple<asio::error_code, std::optional<tcp::socket>>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<accept_sig>(0))&>().await_resume()),
		std::tuple<asio::error_code, std::optional<tcp::socket>>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<read_sig>(0, task::cancel_token{}))&>().await_resume()),
		std::tuple<asio::error_code, std::size_t>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<read_sig>(0))&>().await_resume()),
		std::tuple<asio::error_code, std::size_t>>);
	std::printf("sizes: token awaiter, socket argument %zu\n", sizeof(decltype(task::await_asio<accept_sig>(0, task::cancel_token{}))));
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	auto r = task::scheduler_ref::of(s);
	tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
	// Requested before the await: init is not called, there is no socket.
	{
		task::cancel_source src;
		src.request();
		accepted out;
		std::atomic<int> done{0};
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.run();
		CHECK(done == 1 && !out.called && out.ec == asio::error::operation_aborted && !out.engaged);
	}
	// Requested while the accept is pending, on the loop and from another thread: asio completes
	// it with operation_aborted and a socket that is not open.
	for (bool other_thread : {false, true}) {
		task::cancel_source src;
		accepted out;
		std::atomic<int> done{0};
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.poll();
		CHECK(done == 0 && out.called);
		if (other_thread) {
			std::thread([&] { src.request(); }).join();
		} else {
			asio::post(io, [&] { src.request(); });
		}
		io.run_for(10s);
		CHECK(done == 1 && out.ec == asio::error::operation_aborted && out.engaged && !out.open);
	}
	// A connection arrives: the socket is handed over, open, connected to the client.
	{
		task::cancel_source src;
		accepted out;
		std::atomic<int> done{0};
		tcp::socket client(io);
		client.connect(acceptor.local_endpoint());
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.run_for(10s);
		CHECK(done == 1 && !out.ec && out.engaged && out.open && out.peer_port == client.local_endpoint().port());
	}
	CHECK(task::detail::live_frames() == base);
}

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

// Xapiand's Host shape: a virtual base that spawns through scheduler_ref::of(*this) and
// forwards the optional members to the reactor's scheduler (D7).
struct host {
	virtual ~host() = default;
	virtual void post(std::coroutine_handle<> h) = 0;
	virtual std::uint64_t now() const noexcept = 0;
	virtual bool runs_here() const noexcept = 0;
	virtual void post_call(void (*fn)(void*), void* arg) = 0;
	virtual task::root_list* roots() noexcept = 0;
};
struct asio_host final : host {
	explicit asio_host(task::asio_scheduler& s) : s_(s) {}
	void post(std::coroutine_handle<> h) override {
		if (s_.closed()) { return; }
		asio::post(s_.executor(), [this, h] {
			task::running_on scope(task::scheduler_ref::of(static_cast<host&>(*this)));
			h.resume();
		});
	}
	std::uint64_t now() const noexcept override { return s_.now(); }
	bool runs_here() const noexcept override { return s_.runs_here(); }
	void post_call(void (*fn)(void*), void* arg) override { s_.post_call(fn, arg); }
	task::root_list* roots() noexcept override { return s_.roots(); }
	task::asio_scheduler& s_;
};

void test_wrapper() {
	const auto base = task::detail::live_frames();
	loop l;
	asio_host h(*l.sched);
	auto hr = task::scheduler_ref::of(static_cast<host&>(h));
	l.start();
	std::atomic<int> destroyed{0}, done{0};
	std::atomic<bool> parked{false}, parked2{false};
	// A cancel from another thread reaches the operation through the wrapper's post_call.
	task::cancel_source src;
	auto cancelled = [&]() -> task::task<void> {
		co_await sleep_on(l.io, &destroyed, &parked, src.token());
		CHECK(task::current() == hr);
		++done;
	};
	task::spawn(hr, cancelled());
	wait_for(parked);
	std::this_thread::sleep_for(5ms);
	src.request();
	for (int i = 0; i < 2000 && done == 0; ++i) { std::this_thread::sleep_for(1ms); }
	CHECK(done == 1 && destroyed == 1);
	// A task spawned through the wrapper is a root of the reactor's scheduler.
	task::spawn(hr, sleep_on(l.io, &destroyed, &parked2));
	wait_for(parked2);
	teardown({&l});
	CHECK(destroyed == 2);
	CHECK(task::detail::live_frames() == base + 1);  // src's state
}

struct sets_on_death {
	task::event* e;
	~sets_on_death() { e->set(); }
};
struct spawns_on_death {
	task::scheduler_ref s;
	std::atomic<int>* destroyed;
	std::atomic<int>* done_calls;
	~spawns_on_death() {
		auto late = [](std::unique_ptr<destroy_mark>) -> task::task<void> { co_return; };  // the mark dies with the frame
		std::atomic<int>* calls = done_calls;
		task::spawn(s, late(std::unique_ptr<destroy_mark>(new destroy_mark{destroyed})), [calls](std::exception_ptr) { ++*calls; });
	}
};

void test_teardown() {
	const auto base = task::detail::live_frames();
	// One loop with roots suspended in await_asio (with and without a token), an event, a lock
	// wait, a demoted checkpoint and a when_all; a connection-sized frame (over 2 KiB) counted.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		task::event e;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source src;
		std::atomic<int> destroyed{0};
		std::atomic<bool> p1{false}, p2{false};
		task::spawn(r, sleep_on(io, &destroyed, &p1));
		task::spawn(r, sleep_on(io, &destroyed, &p2, src.token()));
		auto on_event = [](task::event& ev, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			co_await ev;
		};
		auto on_lock = [](task::async_mutex& mu, task::cancel_token t, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			auto g = co_await mu.lock(std::move(t));
		};
		auto demoter = [](std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			task::budget b(1ns);
			for (;;) { co_await b.checkpoint(); }
		};
		auto joiner = [](task::event& ev, std::atomic<int>* d) -> task::task<void> {
			auto child = [](task::event& x, std::atomic<int>* dd) -> task::task<int> {
				destroy_mark mk{dd};
				co_await x;
				co_return 1;
			};
			co_await task::when_all(child(ev, d), child(ev, d));
		};
		auto big = [](asio::io_context& c, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			volatile char tmp[64 * 1024];
			tmp[0] = 1;
			asio::steady_timer t(c, 1h);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			tmp[1] = tmp[0];
		};
		task::spawn(r, on_event(e, &destroyed));
		task::spawn(r, on_lock(m, {}, &destroyed));
		task::spawn(r, on_lock(m, src.token(), &destroyed));
		task::spawn(r, demoter(&destroyed));
		task::spawn(r, joiner(e, &destroyed));
		const auto before_big = task::detail::live_frames();
		task::spawn(r, big(io, &destroyed));
		CHECK(task::detail::live_frames() > before_big);
		for (int i = 0; i < 50; ++i) { io.poll_one(); }
		CHECK(s.demoted() == 1);
		io.stop();
		s.close();
		s.destroy_roots();
		CHECK(destroyed == 9);
		CHECK(s.demoted() == 0);
		m.unlock();  // sheds the two waiters the destroyed frames left timed out
		CHECK(m.try_lock());
		m.unlock();
		CHECK(task::detail::live_frames() == base + 1);  // src's state
		e.set();
	}
	// crossroot: a root on A suspended on B's timer.
	{
		loop a, b;
		a.start();
		b.start();
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false};
		auto cross = [](loop& to, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
			co_await task::schedule_on{to.ref()};
			co_await sleep_on(to.io, d, p);
		};
		task::spawn(a.ref(), cross(b, &destroyed, &parked));
		wait_for(parked);
		teardown({&a, &b});
		CHECK(destroyed == 1);
	}
	// holder: a guard held by a root on A, a waiter rooted on B; destroyed in both orders. The
	// holder's unlock grants the waiter, whose wake B drops; the waiter's destructor unlocks.
	for (bool a_first : {true, false}) {
		task::async_mutex m;  // owned outside every root
		{
			loop a, b;
			a.start();
			b.start();
			std::atomic<int> destroyed{0};
			std::atomic<bool> held{false}, queued{false};
			auto holder = [](task::async_mutex& mu, loop& l, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
				auto g = co_await mu.lock();
				co_await sleep_on(l.io, d, p);
			};
			auto waiter = [](task::async_mutex& mu, std::atomic<int>* d, std::atomic<bool>* q) -> task::task<void> {
				destroy_mark mk{d};
				*q = true;
				auto g = co_await mu.lock();
			};
			task::spawn(a.ref(), holder(m, a, &destroyed, &held));
			wait_for(held);
			task::spawn(b.ref(), waiter(m, &destroyed, &queued));
			wait_for(queued);
			std::this_thread::sleep_for(5ms);
			a.stop();
			b.stop();
			a.sched->close();
			b.sched->close();
			if (a_first) {
				a.sched->destroy_roots();
				b.sched->destroy_roots();
			} else {
				b.sched->destroy_roots();
				a.sched->destroy_roots();
			}
			CHECK(destroyed == 2);
		}
		CHECK(m.try_lock());
		m.unlock();
	}
	// An event set by a dying frame on A, its waiter rooted on B; both orders.
	for (bool a_first : {true, false}) {
		task::event e;
		loop a, b;
		a.start();
		b.start();
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false}, waiting{false};
		auto setter = [](task::event& ev, loop& l, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
			sets_on_death s{&ev};
			co_await sleep_on(l.io, d, p);
		};
		auto waiter = [](task::event& ev, std::atomic<int>* d, std::atomic<bool>* w) -> task::task<void> {
			destroy_mark mk{d};
			*w = true;
			co_await ev;
		};
		task::spawn(a.ref(), setter(e, a, &destroyed, &parked));
		task::spawn(b.ref(), waiter(e, &destroyed, &waiting));
		wait_for(parked);
		wait_for(waiting);
		std::this_thread::sleep_for(5ms);
		a.stop();
		b.stop();
		a.sched->close();
		b.sched->close();
		if (a_first) {
			a.sched->destroy_roots();
			b.sched->destroy_roots();
		} else {
			b.sched->destroy_roots();
			a.sched->destroy_roots();
		}
		CHECK(destroyed == 2 && e.is_set());
	}
	// A destructor that spawns during phase 3: the task is destroyed at once, unrun, and its
	// done is never called; a frame destroyed with a cross-thread emit pending.
	{
		loop l;
		l.start();
		std::atomic<int> destroyed{0}, late{0}, done_calls{0};
		std::atomic<bool> parked{false}, parked2{false};
		auto spawner = [](loop& lp, std::atomic<int>* d, std::atomic<int>* lt, std::atomic<int>* calls, std::atomic<bool>* p) -> task::task<void> {
			spawns_on_death sp{lp.ref(), lt, calls};
			co_await sleep_on(lp.io, d, p);
		};
		task::cancel_source src;
		task::spawn(l.ref(), spawner(l, &destroyed, &late, &done_calls, &parked));
		task::spawn(l.ref(), sleep_on(l.io, &destroyed, &parked2, src.token()));
		wait_for(parked);
		wait_for(parked2);
		l.stop();
		src.request();  // off the loop: the emit is posted to the stopped loop
		l.sched->close();
		l.sched->destroy_roots();
		CHECK(destroyed == 2 && late == 1 && done_calls == 0);
		CHECK(l.sched->roots()->empty());
	}
	// spawn onto a closed scheduler destroys the task before returning; a blocking caller whose
	// done owns a promise returns (broken_promise) instead of hanging. post, post_demoted and
	// post_call drop.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		s.close();
		std::atomic<int> destroyed{0};
		auto quick = [](std::unique_ptr<destroy_mark>) -> task::task<void> { co_return; };
		std::promise<void> p;
		auto f = p.get_future();
		task::spawn(task::scheduler_ref::of(s), quick(std::unique_ptr<destroy_mark>(new destroy_mark{&destroyed})), [p = std::move(p)](std::exception_ptr) mutable { p.set_value(); });
		CHECK(destroyed == 1);
		bool broken = false;
		try {
			f.get();
		} catch (const std::future_error& err) {
			broken = err.code() == std::future_errc::broken_promise;
		}
		CHECK(broken);
		s.post(std::noop_coroutine());
		s.post_demoted(std::noop_coroutine());
		s.post_call([](void*) {}, nullptr);
		CHECK(io.poll() == 0);
		s.close();  // idempotent
		s.destroy_roots();
		s.destroy_roots();
	}
	// The io_context run again after phase 3 (io.restart(); io.poll()): a resume or a cancel emit
	// queued before close() is dropped, not run against a destroyed frame.
	for (bool emit : {false, true}) {
		std::atomic<int> destroyed{0};
		asio::io_context io;
		asio::steady_timer timer(io, 1h);  // outside the frame: its pending operation outlives it
		{
			task::asio_scheduler s(io.get_executor());
			task::event ev;
			task::cancel_source src;
			auto frame = [&]() -> task::task<void> {
				destroy_mark mark{&destroyed};
				if (emit) {
					co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, src.token());
				} else {
					co_await ev;
				}
			};
			task::spawn(task::scheduler_ref::of(s), frame());
			io.poll();
			if (emit) {
				std::thread([&] { src.request(); }).join();  // queues the emit, which has not run
			} else {
				ev.set();  // queues the resume
			}
			io.stop();
			s.close();
			s.destroy_roots();
			CHECK(destroyed == 1);
			io.restart();
			CHECK(io.poll() >= 1);  // the queued handler runs, and does nothing
		}
		CHECK(task::detail::live_frames() == base);
	}
	// The destructor runs the phases itself when the owner did not (a stopped loop).
	{
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false};
		asio::io_context io;
		{
			task::asio_scheduler s(io.get_executor());
			task::spawn(task::scheduler_ref::of(s), sleep_on(io, &destroyed, &parked));
			io.poll();
			io.stop();
		}
		CHECK(destroyed == 1);
	}
	CHECK(task::detail::live_frames() == base);
}

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
	test_wrapper();
	test_teardown();
	test_sizes();
	std::printf("task_asio: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
