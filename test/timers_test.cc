// sleep_until and sleep_for, group, periodic and debounce, on inline_scheduler's manual clock
// (firing times in simulated milliseconds) and on an asio_scheduler (a cross-thread cancel):
//   sleep     fires at its time, a token ends it early and unfiles the entry, never waits for the token
//   sleep-forced  on blocking_scheduler, a cross-thread cancel held between its claim and its hop
//             while the entry fires: the fire waits for the hop, drive() never stalls
//   sleep-no-post-call  the control, not in `all`: a cancellable sleep on a scheduler without
//             post_call aborts on the assertion (Debug); it exits 1 without one
//   group     join wakes at zero, a token ends it first, an escaping exception goes to failed
//   periodic  ticks a period after the previous one ends; a throw ticks again; stop ends it
//   debounce  quiet, busy and force targets; stop drops pending fires; touch then returns false
//   asio      sleep on asio_scheduler's wheel, cancelled from another thread
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "task_asio.hh"
#include "task_timers.hh"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

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
constexpr u64 ms = 1'000'000;

// Advances the manual clock 1 ms at a time to `until`, running what each step queues.
void step_to(task::inline_scheduler& s, u64 until) {
	s.run();
	while (s.now() < until) {
		s.advance(ms);
		s.run();
	}
}

void sleep() {
	task::inline_scheduler s;
	const auto ref = task::scheduler_ref::of(s);
	std::vector<std::pair<u64, bool>> woke;
	auto sleeper = [&](std::chrono::nanoseconds d, task::cancel_token t) -> task::task<void> {
		const bool fired = co_await task::sleep_for(d, std::move(t));
		woke.emplace_back(s.now() / ms, fired);
	};
	task::spawn(ref, sleeper(10ms, {}));
	step_to(s, 20 * ms);
	CHECK(woke.size() == 1 && woke[0] == std::make_pair(u64(10), true));
	CHECK(s.armed_timers() == 0);

	task::cancel_source early;
	task::spawn(ref, sleeper(100ms, early.token()));
	step_to(s, 25 * ms);
	CHECK(s.armed_timers() == 1);
	early.request();
	s.run();
	CHECK(woke.size() == 2 && woke[1] == std::make_pair(u64(25), false));
	CHECK(s.armed_timers() == 0);  // the hop unfiled it

	task::spawn(ref, sleeper(5ms, early.token()));  // requested already: no wait
	s.run();
	CHECK(woke.size() == 3 && woke[2] == std::make_pair(u64(25), false));

	task::cancel_source later;
	auto waiter = [&](task::cancel_token t) -> task::task<void> {  // named: a coroutine lambda's captures live in it
		const bool fired = co_await task::sleep_until(task::never, std::move(t));
		woke.emplace_back(s.now() / ms, fired);
	};
	task::spawn(ref, waiter(later.token()));
	step_to(s, 5'000 * ms);
	CHECK(woke.size() == 3 && s.armed_timers() == 0);  // no entry for never
	later.request();
	s.run();
	CHECK(woke.size() == 4 && woke[3] == std::make_pair(u64(5'000), false));
}

std::atomic<bool> g_release{false};

void sleep_forced() {
	task::blocking_scheduler s;
	task::cancel_source cancel;
	std::optional<bool> fired;
	std::atomic<bool> waiting{false};
	auto sleeper = [&](task::cancel_token t) -> task::task<void> {
		waiting.store(true);  // its await_suspend registers the callback in this same resume
		fired = co_await task::sleep_for(50ms, std::move(t));
	};
	task::spawn(task::scheduler_ref::of(s), sleeper(cancel.token()));
	std::thread other([&] {
		task::detail::test_hooks::sleep_claimed = [] {  // preempted between the claim and the hop
			const auto until = std::chrono::steady_clock::now() + 300ms;
			while (!g_release.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
		};
		while (!waiting.load()) { std::this_thread::yield(); }
		std::this_thread::sleep_for(5ms);
		cancel.request();
		task::detail::test_hooks::sleep_claimed = nullptr;
	});
	const bool done = s.drive([&] { return fired.has_value(); });  // the entry fires at 50 ms, mid-hold
	g_release.store(true);
	other.join();
	if (!done) { CHECK(s.drive([&] { return fired.has_value(); }, [] { return true; })); }  // the hop came after the stall
	std::printf("sleep-forced drive=%s %s\n", done ? "cancelled" : "stalled", done ? "ok" : "FAILS");
	CHECK(done && fired == false && s.armed_timers() == 0);
}

// Timers and posts, but no post_call.
struct no_call_scheduler {
	std::deque<std::coroutine_handle<>> ready;
	wheel::timer_wheel wheel{0};
	void post(std::coroutine_handle<> h) { ready.push_back(h); }
	std::uint64_t now() const noexcept { return 0; }
	void arm(wheel::timer_entry& e, std::uint64_t at) { wheel.arm(e, at); }
	bool cancel(wheel::timer_entry& e) noexcept {
		if (!e.armed()) { return false; }
		wheel.cancel(e);
		return true;
	}
	void run() {
		task::running_on scope(task::scheduler_ref::of(*this));
		while (!ready.empty()) {
			const auto h = ready.front();
			ready.pop_front();
			h.resume();
		}
	}
};

int sleep_no_post_call() {
	no_call_scheduler s;
	task::cancel_source cancel;
	auto sleeper = [](task::cancel_token t) -> task::task<void> { co_await task::sleep_for(1h, std::move(t)); };
	task::spawn(task::scheduler_ref::of(s), sleeper(cancel.token()));
	s.run();
	std::printf("sleep-no-post-call: cancelling (expected: the assertion aborts)\n");
	std::fflush(stdout);
	cancel.request();
	std::printf("sleep-no-post-call: no assertion, the cancel is lost\n");
	std::_Exit(1);
}

void group() {
	task::inline_scheduler s;
	const auto ref = task::scheduler_ref::of(s);
	task::group g(ref);
	std::vector<std::exception_ptr> failures;
	g.failed = [&](std::exception_ptr e) { failures.push_back(e); };
	g.spawn([]() -> task::task<void> { co_await task::sleep_for(30ms); }());
	g.spawn([]() -> task::task<void> {
		co_await task::sleep_for(10ms);
		throw std::runtime_error("boom");
	}());
	CHECK(g.running() == 2);
	u64 joined_at = 0;
	bool joined = false;
	task::cancel_source give_up;
	bool gave_up = true;
	auto impatient = [&]() -> task::task<void> { gave_up = co_await g.join(give_up.token()); };
	auto patient = [&]() -> task::task<void> {
		joined = co_await g.join();
		joined_at = s.now() / ms;
	};
	task::spawn(ref, impatient());
	task::spawn(ref, patient());
	step_to(s, 5 * ms);
	give_up.request();
	s.run();
	CHECK(!gave_up);
	step_to(s, 50 * ms);
	CHECK(joined && joined_at == 30 && g.running() == 0);
	CHECK(failures.size() == 1);
	bool at_zero = false;
	auto late = [&]() -> task::task<void> { at_zero = co_await g.join(); };
	task::spawn(ref, late());
	s.run();
	CHECK(at_zero);
}

void periodic() {
	task::inline_scheduler s;
	const auto ref = task::scheduler_ref::of(s);
	task::cancel_source stop;
	std::vector<u64> ticks;
	int throws = 0;
	task::group g(ref);
	g.spawn(task::periodic(
		100ms, stop.token(),
		[&] {
			ticks.push_back(s.now() / ms);
			if (ticks.size() == 2) { throw std::runtime_error("tick"); }
		},
		[&](std::exception_ptr) { ++throws; }));
	step_to(s, 350 * ms);
	CHECK((ticks == std::vector<u64>{100, 200, 300}) && throws == 1);  // the throw ticked again
	stop.request();
	s.run();
	CHECK(g.running() == 0 && s.armed_timers() == 0);  // stop ended the pending wait at once
	step_to(s, 1'000 * ms);
	CHECK(ticks.size() == 3);

	// A coroutine tick: the period runs from its end.
	task::cancel_source stop2;
	std::vector<u64> starts;
	g.spawn(task::periodic(
		100ms, stop2.token(),
		[&]() -> task::task<void> {
			starts.push_back(s.now() / ms);
			co_await task::sleep_for(50ms);
		},
		[](std::exception_ptr) {}));
	step_to(s, 1'450 * ms);
	CHECK((starts == std::vector<u64>{1'100, 1'250, 1'400}));
	stop2.request();
	step_to(s, 1'500 * ms);  // the tick in flight ends; the next wait sees the stop
	CHECK(g.running() == 0 && s.armed_timers() == 0);
}

void debounce() {
	task::inline_scheduler s;
	const auto ref = task::scheduler_ref::of(s);
	task::group g(ref);
	task::cancel_source stop;
	std::vector<std::pair<int, u64>> fires;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms}, stop.token(), [&](const int& k) { fires.emplace_back(k, s.now() / ms); });

	CHECK(d.touch(1));  // quiet: 100 ms after the first touch
	step_to(s, 60 * ms);
	CHECK(d.touch(1));  // busy: 60 + 50 = 110 > 100
	step_to(s, 200 * ms);
	CHECK((fires == std::vector<std::pair<int, u64>>{{1, 110}}));
	CHECK(d.pending_keys() == 0);

	// Touched every 10 ms, a key fires at its force, 1,000 ms after its first touch.
	for (u64 t = 200; t < 1'400; t += 10) {
		step_to(s, t * ms);
		CHECK(d.touch(2));
	}
	CHECK((fires.size() == 2 && fires[1] == std::pair<int, u64>{2, 1'200}));
	step_to(s, 1'500 * ms);  // touches after the forced fire started another: busy past the last, at 1,390
	CHECK((fires.size() == 3 && fires[2] == std::pair<int, u64>{2, 1'440}));

	// Stop drops what is pending, at once, and touch then returns false.
	const auto before = fires.size();
	CHECK(d.touch(3) && d.touch(4));
	step_to(s, 1'550 * ms);
	stop.request();
	s.run();
	CHECK(g.running() == 0 && s.armed_timers() == 0 && d.pending_keys() == 0);
	CHECK(!d.touch(3));
	step_to(s, 3'000 * ms);
	CHECK(fires.size() == before);
}

void asio_sleep() {
	asio::io_context io(1);
	{
		task::asio_scheduler loop(io.get_executor());
		const auto ref = task::scheduler_ref::of(loop);
		task::cancel_source cancel;
		bool timed = false, cancelled = true;
		int done = 0;
		auto short_sleep = [&]() -> task::task<void> {
			timed = co_await task::sleep_for(5ms);
			if (++done == 2) { io.stop(); }
		};
		auto long_sleep = [&]() -> task::task<void> {
			cancelled = co_await task::sleep_for(1h, cancel.token());
			if (++done == 2) { io.stop(); }
		};
		task::spawn(ref, short_sleep());
		task::spawn(ref, long_sleep());
		std::thread other([&] {
			std::this_thread::sleep_for(20ms);
			cancel.request();  // off the loop: the hop unfiles the entry there
		});
		auto work = asio::make_work_guard(io);
		io.run();
		other.join();
		CHECK(timed && !cancelled && done == 2);
		loop.close();
		loop.destroy_roots();
	}
}

}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	if (only == "sleep-no-post-call") { return sleep_no_post_call(); }
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "sleep") { sleep(); }
	if (only == "all" || only == "sleep-forced") { sleep_forced(); }
	if (only == "all" || only == "group") { group(); }
	if (only == "all" || only == "periodic") { periodic(); }
	if (only == "all" || only == "debounce") { debounce(); }
	if (only == "all" || only == "asio") { asio_sleep(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("timers_test: ok\n");
	return 0;
}
