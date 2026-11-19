// task::lifecycle on inline_scheduler's manual clock, times in simulated milliseconds:
//   signals  a child process: a dump signal, then three SIGTERMs: stop, hurry, flush then die
//   order    every stop() in reverse, then each drain in reverse, then each flush in reverse
//   bound    a drain past its bound: cancelled at bound - grace, abandoned grace after; what it
//            left is reported and the earlier components only flush
//   hurry    a hurry cancels at once, and abandons grace after
//   failure  a start that throws: the started ones stop, drain and flush in reverse; named
//   nested   a set of components inside a lifecycle, in its parent's order; a nested failure
//   parent   a parent component's stop is a nested lifecycle's stop source
//   tool     a tool's end stops the lifecycle; fail() is reported
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "lifecycle.hh"

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <poll.h>
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
std::optional<task::stopped> drive(task::inline_scheduler& s, task::task<task::stopped> t, const std::function<void(u64)>& act, u64 limit = 100'000) {
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

// A component that runs its own lifecycle over children, its stop as their stop source.
struct parent_component {
	probe x{"x"}, y{"y"};
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
		result = co_await task::lifecycle(*g_sched, *inner, {}, {}, x, y);
		ended.set();
	}
	void stop() { g_log.push_back("stop parent"); }
	task::task<lines> drain(const task::stop_bound&) {
		co_await ended;
		g_log.push_back("drained parent");
		co_return result->unfinished;
	}
	void flush() { g_log.push_back("flush parent"); }
};

void parent() {
	fixture f;
	probe a{"a"};
	parent_component p;
	auto r = drive(f.s, task::lifecycle(f.s, f.source, {}, f.ready(), a, p), [&](u64 t) {
		if (t == 10) { f.source.request_stop(); }
	});
	CHECK(r && r->ready && !r->failure && p.result && p.result->ready);
	CHECK((g_log == lines{"start a", "start parent", "ready @0", "start x", "start y", "stop parent", "stop a", "stop y", "stop x", "drain y",
	                      "drain x", "flush y", "flush x", "drained parent", "drain a", "flush parent", "flush a"}));
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

}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	if (only == "all" || only == "signals") { signals(); }  // first: it forks a single-threaded process
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "order") { order(); }
	if (only == "all" || only == "bound") { bound(); }
	if (only == "all" || only == "hurry") { hurry(); }
	if (only == "all" || only == "failure") { failure(); }
	if (only == "all" || only == "nested") { nested(); }
	if (only == "all" || only == "parent") { parent(); }
	if (only == "all" || only == "tool") { tool(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("lifecycle_test: ok\n");
	return 0;
}
