// task::lifecycle on inline_scheduler's manual clock, times in simulated milliseconds:
//   signals  a child process: a dump signal, then three SIGTERMs: stop, hurry, flush then die
//   order    every stop() in reverse, then each drain in reverse, then each flush in reverse
//   bound    a drain past its bound: cancelled at bound - grace, abandoned grace after; what it
//            left is reported and the earlier components only flush
//   hurry    a hurry cancels at once, and abandons grace after
//   failure  a start that throws: the started ones stop, drain and flush in reverse; named
//   start-token  a start or ready failure requests the start token, so work a started
//            component runs on it ends and its drain returns
//   stop-throws  a stop() that throws: every other stop, drain and flush runs, the throw is
//            reported, and the bound's timer ends with the lifecycle
//   nested   a set of components inside a lifecycle, in its parent's order; a nested failure
//   parent   a parent component's stop is a nested lifecycle's stop source; its flush()
//            forwards to the inner set, so the third signal flushes the children
//   flush-off-loop  the flushes run off the loop: a flush() that needs the loop returns
//   flush-inline  with no thread to start, every flush still runs, inline on the loop
//   flush-fail  a flush the third signal runs may call fail() on its source
//   signal-close  ~stop_signals waits for a handler that loaded the pipe before it closes it
//   tool     a tool's end stops the lifecycle; fail() is reported
//   asio     service-shape's main on asio_scheduler: io.run() returns only after the flushes
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "lifecycle.hh"
#if TASK_LIFECYCLE_ASIO
#include <asio/io_context.hpp>

#include "task_asio.hh"
#endif

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <poll.h>
#include <pthread.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(c)                                                              \
	do {                                                                      \
		if (!(c)) {                                                           \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); \
			std::exit(1);                                                     \
		}                                                                     \
	} while (0)

namespace {
using namespace std::chrono_literals;
using u64 = std::uint64_t;
using lines = std::vector<std::string>;
constexpr u64 ms = 1'000'000;

lines g_log;
task::inline_scheduler* g_sched = nullptr;
std::string at() { return "@" + std::to_string(g_sched->now() / ms); }

// A component that logs each call; its drain returns at once, or waits for b.cancel, or for
// b.abandon and then reports what it left.
struct probe {
	enum mode { quick, until_cancel, until_abandon };
	std::string id;
	mode drains = quick;
	bool throws = false;

	std::string_view name() const { return id; }
	task::task<void> start(task::cancel_token) {
		g_log.push_back("start " + id);
		if (throws) { throw std::runtime_error(id); }
		co_return;
	}
	void stop() { g_log.push_back("stop " + id); }
	task::task<lines> drain(const task::stop_bound& b) {
		g_log.push_back("drain " + id);
		if (drains != quick) {
			co_await task::sleep_until(task::never, b.cancel);
			g_log.push_back("cancel " + id + " " + at());
		}
		if (drains == until_abandon) {
			co_await task::sleep_until(task::never, b.abandon);
			g_log.push_back("abandon " + id + " " + at());
			co_return lines{"left " + id};
		}
		co_return lines{};
	}
	void flush() { g_log.push_back("flush " + id); }
};
static_assert(task::component<probe>);

// Runs t on s, calling act(ms) before each simulated millisecond, until it ends or `limit`.
// With nothing queued or armed it yields, so the flush thread runs.
std::optional<task::stopped> drive(task::inline_scheduler& s, task::task<task::stopped> t, const std::function<void(u64)>& act, u64 limit = 1'000'000) {
	std::optional<task::stopped> result;
	task::spawn(task::scheduler_ref::of(s), std::move(t), [&](std::exception_ptr e, std::optional<task::stopped> r) {
		CHECK(!e);
		result = std::move(r);
	});
	s.run();
	while (!result && s.now() / ms < limit) {
		act(s.now() / ms);
		s.run();
		s.advance(ms);
		s.run();
		if (!s.queued() && !s.armed_timers()) { std::this_thread::yield(); }
	}
	return result;
}

struct fixture {
	task::inline_scheduler s;
	task::stop_source source;
	fixture() {
		g_log.clear();
		g_sched = &s;
	}
	std::function<void()> ready() {
		return [] { g_log.push_back("ready " + at()); };
	}
};

void order() {
	fixture f;
	probe a{"a"}, b{"b"}, c{"c"};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, b, c), [&](u64 t) {
		if (t == 1'000) { f.source.request_stop(); }
	});
	CHECK(r && r->ready && !r->failure && r->unfinished.empty());
	CHECK((g_log == lines{"start a", "start b", "start c", "ready @0", "stop c", "stop b", "stop a", "drain c", "drain b", "drain a",
	                      "flush c", "flush b", "flush a"}));
	CHECK(f.s.armed_timers() == 0);  // the bound's timers end with the drains
}

void bound() {
	fixture f;
	probe a{"a"}, b{"b", probe::until_abandon}, c{"c", probe::until_cancel};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {35s, 5s}, f.ready(), a, b, c), [&](u64 t) {
		if (t == 1'000) { f.source.request_stop(); }
	});
	CHECK(r && r->ready && !r->failure);
	CHECK((r->unfinished == lines{"left b"}));
	// c's drain ends at the cancel, b's at the abandon; a is never drained, only flushed.
	CHECK((g_log == lines{"start a", "start b", "start c", "ready @0", "stop c", "stop b", "stop a", "drain c", "cancel c @31000", "drain b",
	                      "cancel b @31000", "abandon b @36000", "flush c", "flush b", "flush a"}));
}

void hurry() {
	fixture f;
	probe a{"a", probe::until_abandon};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {35s, 5s}, f.ready(), a), [&](u64 t) {
		if (t == 1'000) { f.source.request_stop(); }
		if (t == 2'000) { f.source.request_hurry(); }
	});
	CHECK(r && (r->unfinished == lines{"left a"}));
	CHECK((g_log == lines{"start a", "ready @0", "stop a", "drain a", "cancel a @2000", "abandon a @7000", "flush a"}));
}

void failure() {
	fixture f;
	probe a{"a"}, b{"b", probe::quick, true}, c{"c"};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, b, c), [](u64) {});
	CHECK(r && !r->ready && r->failure && r->failed == "b");
	try {
		std::rethrow_exception(r->failure);
	} catch (const std::runtime_error& e) {
		CHECK(std::string(e.what()) == "b");
	}
	CHECK((g_log == lines{"start a", "start b", "stop a", "drain a", "flush a"}));
}

// A component whose housekeeping runs on the token start(stop) gets; its drain awaits it.
struct housekeeper {
	task::event ended;
	task::task<void> work(task::cancel_token stop) {
		co_await task::sleep_until(task::never, stop);
		g_log.push_back("housekeeping ended");
		ended.set();
	}
	task::task<void> start(task::cancel_token stop) {
		g_log.push_back("start housekeeper");
		task::spawn(task::current(), work(stop));
		co_return;
	}
	void stop() { g_log.push_back("stop housekeeper"); }
	task::task<lines> drain(const task::stop_bound&) {
		co_await ended;
		co_return lines{};
	}
	void flush() { g_log.push_back("flush housekeeper"); }
};

void start_token() {
	{
		fixture f;  // a later start fails
		housekeeper h;
		probe b{"b", probe::quick, true};
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), h, b), [](u64) {}, 5'000);
		CHECK(r && !r->ready && r->failure && r->failed == "b");
		CHECK((g_log == lines{"start housekeeper", "start b", "stop housekeeper", "housekeeping ended", "flush housekeeper"}));
	}
	{
		fixture f;  // ready() throws
		housekeeper h;
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, [] { throw std::runtime_error("ready"); }, h), [](u64) {}, 5'000);
		CHECK(r && !r->ready && r->failure);
		CHECK((g_log == lines{"start housekeeper", "stop housekeeper", "housekeeping ended", "flush housekeeper"}));
	}
}

struct throwing_stop : probe {
	void stop() {
		probe::stop();
		throw std::runtime_error("stop " + id);
	}
};
static_assert(task::component<throwing_stop>);

void stop_throws() {
	fixture f;
	probe a{"a"}, c{"c"};
	throwing_stop b{{"b"}};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, b, c), [&](u64 t) {
		if (t == 10) { f.source.request_stop(); }
	});
	CHECK(r && r->ready && r->failure);
	try {
		std::rethrow_exception(r->failure);
	} catch (const std::runtime_error& e) {
		CHECK(std::string(e.what()) == "stop b");
	}
	CHECK((g_log == lines{"start a", "start b", "start c", "ready @0", "stop c", "stop b", "stop a", "drain c", "drain b", "drain a", "flush c",
	                      "flush b", "flush a"}));
	CHECK(f.s.armed_timers() == 0);  // the bound's timer ended with the drains
	for (int i = 0; i < 40'000; ++i) {  // past bound and grace: nothing of the lifecycle fires
		f.s.advance(ms);
		f.s.run();
	}
}

void nested() {
	{
		fixture f;
		probe a{"a"}, b{"b"}, c{"c"}, d{"d"};
		task::components inner(b, c);
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, inner, d), [&](u64 t) {
			if (t == 10) { f.source.request_stop(); }
		});
		CHECK(r && r->ready && !r->failure);
		CHECK((g_log == lines{"start a", "start b", "start c", "start d", "ready @0", "stop d", "stop c", "stop b", "stop a", "drain d", "drain c",
		                      "drain b", "drain a", "flush d", "flush c", "flush b", "flush a"}));
	}
	{
		fixture f;  // a nested start failure: the inner set's started child unwinds in the parent's order
		probe a{"a"}, b{"b"}, c{"c", probe::quick, true}, d{"d"};
		task::components inner(b, c);
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, inner, d), [](u64) {});
		CHECK(r && !r->ready && r->failure && r->failed == "1/c");
		CHECK((g_log == lines{"start a", "start b", "start c", "stop b", "stop a", "drain b", "drain a", "flush b", "flush a"}));
	}
	{
		fixture f;  // a nested drain that leaves work: the earlier components only flush
		probe a{"a"}, b{"b", probe::until_abandon}, c{"c"};
		task::components inner(a, b);
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {2s, 1s}, f.ready(), inner, c), [&](u64 t) {
			if (t == 10) { f.source.request_stop(); }
		});
		CHECK(r && (r->unfinished == lines{"left b"}));
		CHECK((g_log == lines{"start a", "start b", "start c", "ready @0", "stop c", "stop b", "stop a", "drain c", "drain b", "cancel b @1010",
		                      "abandon b @2010", "flush c", "flush b", "flush a"}));
	}
}

// A component that runs its own lifecycle over children, its stop as their stop source. It
// holds the children as one set, so its flush() forwards to them (service-shape's pattern).
struct parent_component {
	probe x{"x"}, y{"y"};
	task::components children{x, y};
	std::optional<task::stop_source> inner;
	std::optional<task::stopped> result;
	task::event ended;

	task::task<void> start(task::cancel_token stop) {
		g_log.push_back("start parent");
		inner.emplace(stop);
		task::spawn(task::current(), run(), [](std::exception_ptr e) { CHECK(!e); });
		co_return;
	}
	task::task<void> run() {
		result = co_await task::lifecycle(*g_sched, *inner, {}, {}, children);
		ended.set();
	}
	void stop() { g_log.push_back("stop parent"); }
	task::task<lines> drain(const task::stop_bound&) {
		co_await ended;
		g_log.push_back("drained parent");
		co_return result->unfinished;
	}
	void flush() {
		g_log.push_back("flush parent");
		children.flush();
	}
};

void parent() {
	{
		fixture f;
		probe a{"a"};
		parent_component p;
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, p), [&](u64 t) {
			if (t == 10) { f.source.request_stop(); }
		});
		CHECK(r && r->ready && !r->failure && p.result && p.result->ready);
		// The inner lifecycle sees the stop through a hop, so a stops before x and y.
		CHECK((g_log == lines{"start a", "start parent", "ready @0", "start x", "start y", "stop parent", "stop a", "stop y", "stop x", "drain y",
		                      "drain x", "flush y", "flush x", "drained parent", "drain a", "flush parent", "flush y", "flush x", "flush a"}));
	}
	{
		fixture f;  // the third signal while the inner drain waits: the children flush too
		probe a{"a"};
		parent_component p;
		p.x.drains = probe::until_abandon;
		lines at_third;
		auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, p), [&](u64 t) {
			if (t == 10) { f.source.request_stop(); }
			if (t == 20) {
				const auto before = g_log.size();
				f.source.run_flushes();
				at_third.assign(g_log.begin() + long(before), g_log.end());
			}
		});
		CHECK(r);
		CHECK((at_third == lines{"flush parent", "flush y", "flush x", "flush a"}));
	}
}

// A flush() that needs the loop: it queues a call there and waits for it to run.
struct loop_flush : probe {
	void flush() {
		probe::flush();
		std::atomic<bool> ran{false};
		CHECK(task::scheduler_ref::of(*g_sched).post_call([](void* p) { static_cast<std::atomic<bool>*>(p)->store(true); }, &ran));
		const auto until = std::chrono::steady_clock::now() + 5s;
		while (!ran.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
		CHECK(ran.load());  // on the loop it would wait forever
	}
};
static_assert(task::component<loop_flush>);

void flush_off_loop() {
	fixture f;
	loop_flush a{{"a"}};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a), [&](u64 t) {
		if (t == 10) { f.source.request_stop(); }
	});
	CHECK(r && r->ready && !r->failure);
	CHECK((g_log == lines{"start a", "ready @0", "stop a", "drain a", "flush a"}));
}

// A component whose flush records the thread it ran on.
struct thread_flush : probe {
	std::thread::id* flushed_on;
	void flush() {
		probe::flush();
		*flushed_on = std::this_thread::get_id();
	}
};
static_assert(task::component<thread_flush>);

void flush_inline() {
	fixture f;
	task::detail::test_hooks::off_loop_start = [] { throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again)); };
	std::thread::id on_a, on_b;
	thread_flush a{{"a"}, &on_a}, b{{"b"}, &on_b};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, b), [&](u64 t) {
		if (t == 10) { f.source.request_stop(); }
	});
	task::detail::test_hooks::off_loop_start = nullptr;
	CHECK(r && r->ready && !r->failure);
	CHECK((g_log == lines{"start a", "start b", "ready @0", "stop b", "stop a", "drain b", "drain a", "flush b", "flush a"}));
	CHECK(on_a == std::this_thread::get_id() && on_b == std::this_thread::get_id());
}

void flush_fail() {
	task::stop_source source;
	std::atomic<bool> flushed{false};
	const auto registration = source.add_flush([&] {
		source.fail(std::make_exception_ptr(std::runtime_error("flush")));
		CHECK(source.failure());
		flushed.store(true);
	});
	std::thread([&] { source.run_flushes(); }).detach();  // the third signal's thread
	const auto until = std::chrono::steady_clock::now() + 5s;
	while (!flushed.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
	CHECK(flushed.load());  // under the source's lock it would deadlock
	CHECK(source.stop().requested());
}

std::atomic<bool> g_loaded{false}, g_closed{false};

void signal_close() {
	std::optional<task::stop_signals> sig;
	sig.emplace(std::initializer_list<int>{SIGINT, SIGTERM}, std::initializer_list<int>{SIGUSR1}, [](int) {});
	std::thread handler([] {  // a handler preempted between loading the pipe and writing to it
		task::detail::test_hooks::signal_loaded = [] {
			g_loaded.store(true);
			const auto until = std::chrono::steady_clock::now() + 300ms;
			while (!g_closed.load() && std::chrono::steady_clock::now() < until) {}
		};
		::pthread_kill(::pthread_self(), SIGUSR1);
		task::detail::test_hooks::signal_loaded = nullptr;
	});
	while (!g_loaded.load()) { std::this_thread::yield(); }
	sig.reset();  // waits for the handler's write before it closes the pipe
	int fresh[2];  // the closed descriptors' numbers, reused
	CHECK(::pipe(fresh) == 0);
	g_closed.store(true);
	handler.join();
	pollfd p{fresh[0], POLLIN, 0};
	CHECK(::poll(&p, 1, 0) == 0);  // the late write did not land in the new pipe
	::close(fresh[0]);
	::close(fresh[1]);
}

void tool() {
	fixture f;
	probe a{"a"};
	// The tool's work starts at ready and stops the lifecycle when it ends.
	auto work = [&]() -> task::task<void> {
		co_await task::sleep_for(500ms);
		g_log.push_back("tool done " + at());
		f.source.request_stop();
	};
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, [&] { task::spawn(task::current(), work()); }, a), [](u64) {});
	CHECK(r && r->ready && !r->failure);
	CHECK((g_log == lines{"start a", "tool done @500", "stop a", "drain a", "flush a"}));

	fixture g;  // fail() stops and is reported
	probe b{"b"};
	auto r2 = drive(g.s, task::lifecycle(g.s, g.source, {}, g.ready(), b), [&](u64 t) {
		if (t == 7) { g.source.fail(std::make_exception_ptr(std::runtime_error("failed"))); }
	});
	CHECK(r2 && r2->failure && r2->failed.empty());
	CHECK((g_log == lines{"start b", "ready @0", "stop b", "drain b", "flush b"}));
}

// The child's component writes a byte to the parent at each step.
struct signal_probe {
	int fd;
	void say(char c) const { CHECK(::write(fd, &c, 1) == 1); }
	task::task<void> start(task::cancel_token) { co_return; }
	void stop() { say('S'); }
	task::task<lines> drain(const task::stop_bound& b) {
		co_await task::sleep_until(task::never, b.cancel);
		say('H');
		co_await task::sleep_until(task::never, b.abandon);  // an hour: the third signal comes first
		co_return lines{};
	}
	void flush() { say('F'); }
};

char expect(int fd) {
	pollfd p{fd, POLLIN, 0};
	CHECK(::poll(&p, 1, 10'000) == 1);
	char c = 0;
	CHECK(::read(fd, &c, 1) == 1);
	return c;
}

void signals() {
	int out[2];
	CHECK(::pipe(out) == 0);
	const pid_t pid = ::fork();
	CHECK(pid >= 0);
	if (pid == 0) {
		::close(out[0]);
		signal_probe child{out[1]};
		task::stop_signals sig({SIGINT, SIGTERM}, {SIGUSR1}, [&](int) { child.say('D'); });
		task::blocking_scheduler loop;
		bool done = false;
		task::spawn(task::scheduler_ref::of(loop), task::lifecycle(loop, sig, {2h, 1h}, [&] { child.say('R'); }, child),
		            [&](std::exception_ptr, std::optional<task::stopped>) { done = true; });
		loop.drive([&] { return done; }, [] { return true; });
		std::_Exit(3);  // the third signal kills before the lifecycle ends
	}
	::close(out[1]);
	CHECK(expect(out[0]) == 'R');
	CHECK(::kill(pid, SIGUSR1) == 0);
	CHECK(expect(out[0]) == 'D');
	CHECK(::kill(pid, SIGTERM) == 0);
	CHECK(expect(out[0]) == 'S');  // the first stops
	CHECK(::kill(pid, SIGTERM) == 0);
	CHECK(expect(out[0]) == 'H');  // the second hurries: b.cancel at once
	CHECK(::kill(pid, SIGTERM) == 0);
	CHECK(expect(out[0]) == 'F');  // the third flushes, then dies by the signal
	int status = 0;
	CHECK(::waitpid(pid, &status, 0) == pid);
	CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM);
	::close(out[0]);
}

#if TASK_LIFECYCLE_ASIO
// service-shape's main: io.run() with no work guard of its own, stopped from another thread.
void asio_main() {
	asio::io_context io(1);
	std::atomic<bool> flushed{false};
	struct quick {
		std::atomic<bool>* flushed;
		task::task<void> start(task::cancel_token) { co_return; }
		void stop() {}
		task::task<lines> drain(const task::stop_bound&) { co_return lines{}; }
		void flush() { flushed->store(true); }
	} a{&flushed};
	task::stop_source source;
	bool ended = false;
	std::thread stopper([&] {
		std::this_thread::sleep_for(100ms);
		source.request_stop();
	});
	{
		task::asio_scheduler loop(io.get_executor());
		task::spawn(task::scheduler_ref::of(loop), task::lifecycle(loop, source, {}, {}, a), [&](std::exception_ptr e, std::optional<task::stopped> r) {
			CHECK(!e && r && !r->failure);
			ended = true;
			io.stop();
		});
		io.run();
		CHECK(ended && flushed.load());  // without the lifecycle's work guard, run() returns at once
		stopper.join();
		loop.close();
		loop.destroy_roots();
	}
}
#endif

}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	if (only == "all" || only == "signals") { signals(); }  // first: it forks a single-threaded process
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "order") { order(); }
	if (only == "all" || only == "bound") { bound(); }
	if (only == "all" || only == "hurry") { hurry(); }
	if (only == "all" || only == "failure") { failure(); }
	if (only == "all" || only == "start-token") { start_token(); }
	if (only == "all" || only == "stop-throws") { stop_throws(); }
	if (only == "all" || only == "nested") { nested(); }
	if (only == "all" || only == "parent") { parent(); }
	if (only == "all" || only == "flush-off-loop") { flush_off_loop(); }
	if (only == "all" || only == "flush-inline") { flush_inline(); }
	if (only == "all" || only == "flush-fail") { flush_fail(); }
	if (only == "all" || only == "signal-close") { signal_close(); }
	if (only == "all" || only == "tool") { tool(); }
#if TASK_LIFECYCLE_ASIO
	if (only == "all" || only == "asio") { asio_main(); }
#endif
	CHECK(task::detail::live_frames() == baseline);
	std::printf("lifecycle_test: ok\n");
	return 0;
}
