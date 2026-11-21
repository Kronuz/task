// sleep_until and sleep_for, group, periodic and debounce, on inline_scheduler's manual clock
// (firing times in simulated milliseconds) and on an asio_scheduler (a cross-thread cancel):
//   sleep     fires at its time, a token ends it early and unfiles the entry, never waits for the token
//   sleep-forced  on blocking_scheduler, a cross-thread cancel held between its claim and its hop
//             while the entry fires: the fire waits for the hop, drive() never stalls
//   sleep-no-post-call  the control, not in `all`: a cancellable sleep on a scheduler without
//             post_call aborts on the assertion (Debug); it exits 1 without one
//   group     join wakes at zero, a token ends it first, an escaping exception goes to failed
//   periodic  ticks a period after the previous one ends; a throw ticks again; stop ends it
//   debounce  quiet, busy and force targets; stop ends the waits without firing and keeps the
//             pending keys, which fire_pending() fires; touch then returns false
//   debounce-flags  a fire gets the OR of its touches' flags; a touch during a fire goes to the next
//   debounce-throttle  a fired key keeps its entry for `throttle`; a touch meanwhile fires at its
//             end; then the entry and its watch go, and a stop ends the wait at once
//   debounce-throw  a throwing fire goes to the group's `failed`; a touch during it still fires, at
//             the throttle's end, and without one the entry and its watch go at once
//   debounce-mid-fire  fire_pending() on another thread while the loop fires a key: that key fires
//             once, and fire_pending() returns only after its fire ended
//   debounce-overlap  before a stop, fire_pending() waits for the fires in progress at the call,
//             not for one the loop starts after it
//   debounce-held  a touch of a key fire_pending() is firing fires on the loop after that fire,
//             never with it
//   debounce-held-twice  two fire_pending() calls taking the same key fire it one after the other
//   debounce-cross  two calls on two threads, each call's fire calling the other debounce's
//             fire_pending(): both return (a deadlock fails after 10 s)
//   debounce-lock  a call while an earlier call's fire waits for a mutex the caller holds, with
//             no key in common: it returns (a deadlock fails after 10 s)
//   debounce-reentry  fire_pending() inside a fire of the same debounce, the loop's or its own,
//             aborts (Debug); another debounce's fire may call it
//   debounce-after-stop  a touch from another thread while fire_pending() fires returns false
//   asio      sleep on asio_scheduler's wheel, cancelled from another thread
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "task_asio.hh"
#include "task_timers.hh"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#define HAVE_FORK 1
#endif

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
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms}, stop.token(), [&](const int& k, unsigned) { fires.emplace_back(k, s.now() / ms); });

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

	// Stop ends every wait at once without firing; the pending keys wait for fire_pending(), and
	// touch then returns false.
	const auto before = fires.size();
	CHECK(d.touch(3) && d.touch(4));
	step_to(s, 1'550 * ms);
	stop.request();
	s.run();
	CHECK(g.running() == 0 && s.armed_timers() == 0 && d.pending_keys() == 2);
	CHECK(!d.touch(3));
	step_to(s, 3'000 * ms);
	CHECK(fires.size() == before);
	CHECK(d.fire_pending() == 2 && d.pending_keys() == 0 && d.fired() == before + 2);
	CHECK(fires.size() == before + 2 && fires[before].first + fires[before + 1].first == 7 && fires[before].second == 3'000);
	CHECK(d.fire_pending() == 0 && fires.size() == before + 2);
}

using fire_log = std::vector<std::tuple<int, u64, unsigned>>;

void debounce_flags() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	fire_log fires;
	task::debounce<int>* self = nullptr;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms}, stop.token(), [&](const int& k, unsigned f) {
		fires.emplace_back(k, s.now() / ms, f);
		if (fires.size() == 1) { CHECK(self->touch(k, 4)); }  // during the fire: the next one's
	});
	self = &d;
	CHECK(d.touch(1, 1));
	step_to(s, 10 * ms);
	CHECK(d.touch(1, 2) && d.touch(2));
	step_to(s, 400 * ms);
	CHECK((fires == fire_log{{1, 100, 3}, {2, 110, 0}, {1, 200, 4}}));
	CHECK(d.touch(1));  // the flags reset after each fire
	step_to(s, 600 * ms);
	CHECK((fires.size() == 4 && fires[3] == std::tuple<int, u64, unsigned>{1, 500, 0}));
	stop.request();
	s.run();
	CHECK(g.running() == 0);
}

void debounce_throttle() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	fire_log fires;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms, 500ms}, stop.token(), [&](const int& k, unsigned f) { fires.emplace_back(k, s.now() / ms, f); });
	CHECK(d.touch(1, 1));
	step_to(s, 150 * ms);
	CHECK((fires == fire_log{{1, 100, 1}}));
	CHECK(d.touch(1, 2));  // quiet would fire it at 250; the throttle holds it to 600
	CHECK(d.pending_keys() == 1);
	step_to(s, 599 * ms);
	CHECK(fires.size() == 1);
	step_to(s, 600 * ms);
	CHECK((fires.size() == 2 && fires[1] == std::tuple<int, u64, unsigned>{1, 600, 2}));
	step_to(s, 1'099 * ms);
	CHECK(g.running() == 1 && d.pending_keys() == 0);  // the entry stays until 600 + 500
	step_to(s, 1'100 * ms);
	CHECK(g.running() == 0 && s.armed_timers() == 0);  // bounded by the keys fired in the last throttle
	CHECK(d.touch(1));  // past the throttle: quiet again
	step_to(s, 1'300 * ms);
	CHECK((fires.size() == 3 && fires[2] == std::tuple<int, u64, unsigned>{1, 1'200, 0}));
	CHECK(g.running() == 1);
	stop.request();  // ends the throttle's wait at once
	s.run();
	CHECK(g.running() == 0 && s.armed_timers() == 0 && d.pending_keys() == 0 && d.fire_pending() == 0);
}

void debounce_throw() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	int failures = 0;
	g.failed = [&](std::exception_ptr) { ++failures; };
	task::cancel_source stop;
	fire_log fires;
	task::debounce<int>* self = nullptr;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms, 500ms}, stop.token(), [&](const int& k, unsigned f) {
		fires.emplace_back(k, s.now() / ms, f);
		if (fires.size() == 1) { CHECK(self->touch(k, 8)); }
		if (fires.size() <= 2) { throw std::runtime_error("fire"); }
	});
	self = &d;
	CHECK(d.touch(1));
	step_to(s, 150 * ms);
	CHECK(failures == 1 && g.running() == 1 && d.pending_keys() == 1);  // the touch's watch
	step_to(s, 700 * ms);
	CHECK((fires == fire_log{{1, 100, 0}, {1, 600, 8}}));  // at the throttle's end
	CHECK(failures == 2 && g.running() == 0 && s.armed_timers() == 0);  // no touch: the entry went
	CHECK(d.touch(1));
	step_to(s, 900 * ms);
	CHECK((fires.size() == 3 && fires[2] == std::tuple<int, u64, unsigned>{1, 800, 0}));
	stop.request();
	s.run();
	CHECK(g.running() == 0);
}

std::atomic<bool> g_pending_waits{false};

void debounce_mid_fire() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	fire_log fires;
	std::atomic<bool> in_fire{false}, fire_ended{false}, returned{false};
	const auto loop_thread = std::this_thread::get_id();
	std::vector<std::thread::id> fired_on;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms}, stop.token(), [&](const int& k, unsigned f) {
		fires.emplace_back(k, s.now() / ms, f);
		fired_on.push_back(std::this_thread::get_id());
		if (k == 1) {  // the loop's fire, held until fire_pending() waits for it or returns without
			in_fire.store(true);
			while (!g_pending_waits.load() && !returned.load()) { std::this_thread::yield(); }
			fire_ended.store(true);
		}
	});
	CHECK(d.touch(1, 1));
	step_to(s, 50 * ms);
	CHECK(d.touch(2, 2));
	std::size_t taken = 0;
	bool ended_first = false;
	std::thread other([&] {
		while (!in_fire.load()) { std::this_thread::yield(); }
		task::detail::test_hooks::pending_waits = [] { g_pending_waits.store(true); };
		stop.request();  // the third signal's order: stop, then fire_pending()
		taken = d.fire_pending();
		ended_first = fire_ended.load();
		returned.store(true);
		task::detail::test_hooks::pending_waits = nullptr;
	});
	step_to(s, 100 * ms);  // fires key 1 on this thread
	other.join();
	s.run();
	step_to(s, 1'000 * ms);
	CHECK(taken == 1 && ended_first && g_pending_waits.load());
	CHECK((fires == fire_log{{1, 100, 1}, {2, 100, 2}}));  // each once
	CHECK(fired_on[0] == loop_thread && fired_on[1] != loop_thread);
	CHECK(g.running() == 0 && d.pending_keys() == 0 && d.fired() == 2);
}

// Posts queue here; the test resumes each on the thread it picks, so two fires can overlap.
struct manual_scheduler {
	std::mutex m;
	std::deque<std::coroutine_handle<>> ready;
	void post(std::coroutine_handle<> h) {
		std::lock_guard lock(m);
		ready.push_back(h);
	}
	std::uint64_t now() const noexcept { return 0; }
	std::size_t queued() {
		std::lock_guard lock(m);
		return ready.size();
	}
	void drain() {
		for (;;) {
			std::coroutine_handle<> h;
			{
				std::lock_guard lock(m);
				if (ready.empty()) { return; }
				h = ready.front();
				ready.pop_front();
			}
			resume(h);
		}
	}
	std::coroutine_handle<> take() {
		std::lock_guard lock(m);
		CHECK(ready.size() == 1);
		const auto h = ready.front();
		ready.pop_front();
		return h;
	}
	void resume(std::coroutine_handle<> h) {
		task::running_on scope(task::scheduler_ref::of(*this));
		h.resume();
	}
};

std::atomic<bool> g_overlap_waits{false};

void debounce_overlap() {
	manual_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	std::atomic<bool> in_one{false}, release_one{false}, in_two{false}, returned{false}, two_saw_return{false};
	std::atomic<int> fires{0};
	task::debounce<int> d(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), [&](const int& k, unsigned) {
		++fires;
		if (k == 1) {
			in_one.store(true);
			while (!release_one.load()) { std::this_thread::yield(); }
		} else {  // held until fire_pending() returns; the deadline only turns a starved wait into a failure
			in_two.store(true);
			const auto until = std::chrono::steady_clock::now() + 10s;
			while (!returned.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
			two_saw_return.store(returned.load());
		}
	});
	CHECK(d.touch(1));
	std::thread one([&, h = s.take()] { s.resume(h); });
	while (!in_one.load()) { std::this_thread::yield(); }
	std::size_t taken = 1;
	std::thread waiter([&] {
		task::detail::test_hooks::pending_waits = [] { g_overlap_waits.store(true); };
		taken = d.fire_pending();  // no stop: the loop keeps firing
		returned.store(true);
		task::detail::test_hooks::pending_waits = nullptr;
	});
	while (!g_overlap_waits.load()) { std::this_thread::yield(); }
	CHECK(d.touch(2));  // after the take: the loop fires it, overlapping key 1's fire
	std::thread two([&, h = s.take()] { s.resume(h); });
	while (!in_two.load()) { std::this_thread::yield(); }
	release_one.store(true);  // the only fire in progress at the call ends
	one.join();
	waiter.join();
	two.join();
	CHECK(two_saw_return.load() && taken == 0 && fires == 2 && d.fired() == 2);
	CHECK(g.running() == 0 && d.pending_keys() == 0);
}

// Counts the fires running at once; the first is held until `release` (the deadline only turns a
// wait that never ends into a failure).
struct overlap_probe {
	std::atomic<int> active{0}, most{0}, fires{0};
	std::atomic<bool> in_first{false}, release{false};
	std::atomic<unsigned> first_flags{0}, last_flags{0};
	void operator()(const int&, unsigned f) {
		const int now = ++active;
		for (int m = most.load(); now > m && !most.compare_exchange_weak(m, now);) {}
		if (fires++ == 0) {
			first_flags.store(f);
			in_first.store(true);
			const auto until = std::chrono::steady_clock::now() + 10s;
			while (!release.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
		} else {
			last_flags.store(f);
		}
		--active;
	}
};

void debounce_held() {
	manual_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	overlap_probe p;
	task::debounce<int> d(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), std::ref(p));
	CHECK(d.touch(1, 1));
	const auto watch = s.take();
	std::size_t taken = 0;
	std::thread other([&] { taken = d.fire_pending(); });  // no stop: the loop keeps firing
	while (!p.in_first.load()) { std::this_thread::yield(); }
	s.resume(watch);  // its key is taken: it ends without firing
	CHECK(d.touch(1, 2));  // mid fire: pending again
	const std::size_t early = s.queued();
	if (early) { s.resume(s.take()); }  // a watch now fires key 1 on the loop, during the held fire
	p.release.store(true);
	other.join();
	if (!early) { s.resume(s.take()); }  // the watch the fire's end handed the key to
	std::printf("debounce-held: watches before the fire ended %zu, fires at once %d\n", early, p.most.load());
	CHECK(early == 0 && p.most == 1 && taken == 1 && p.fires == 2 && d.fired() == 2);
	CHECK(p.first_flags == 1 && p.last_flags == 2);
	CHECK(g.running() == 0 && d.pending_keys() == 0 && s.queued() == 0);
}

std::atomic<bool> g_twice_waits{false};

void debounce_held_twice() {
	manual_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	overlap_probe p;
	task::debounce<int> d(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), std::ref(p));
	CHECK(d.touch(1, 1));
	const auto watch = s.take();
	std::size_t a = 0, b = 0;
	std::thread first([&] { a = d.fire_pending(); });
	while (!p.in_first.load()) { std::this_thread::yield(); }
	s.resume(watch);
	CHECK(d.touch(1, 2));
	std::thread second([&] {
		task::detail::test_hooks::pending_waits = [] { g_twice_waits.store(true); };
		b = d.fire_pending();  // takes key 1 again, mid the first call's fire of it
		task::detail::test_hooks::pending_waits = nullptr;
	});
	while (!g_twice_waits.load() && p.fires.load() < 2) { std::this_thread::yield(); }
	p.release.store(true);
	first.join();
	second.join();
	std::printf("debounce-held-twice: second call waited %d, fires at once %d\n", int(g_twice_waits.load()), p.most.load());
	CHECK(g_twice_waits.load() && p.most == 1 && a == 1 && b == 1 && p.fires == 2);
	CHECK(p.first_flags == 1 && p.last_flags == 2);
	CHECK(g.running() == 0 && d.pending_keys() == 0 && s.queued() == 0);
}

void spin(const std::atomic<bool>& b) {
	while (!b.load()) { std::this_thread::yield(); }
}

// Runs f on its own thread; one that never returns fails the test after 10 s rather than hanging.
template <typename F>
void returns_within(const char* what, F f) {
	std::atomic<bool> done{false};
	std::thread t([&] {
		f();
		done.store(true);
	});
	const auto until = std::chrono::steady_clock::now() + 10s;
	while (!done.load() && std::chrono::steady_clock::now() < until) { std::this_thread::sleep_for(1ms); }
	if (!done.load()) {
		std::printf("%s: DEADLOCK (no return in 10 s)\n", what);
		std::fflush(stdout);
		std::_Exit(1);
	}
	t.join();
}

void debounce_cross() {
	returns_within("debounce-cross", [] {
		manual_scheduler s;
		task::group g(task::scheduler_ref::of(s));
		task::cancel_source stop;
		std::atomic<bool> in1{false}, in2{false};
		std::atomic<std::size_t> nested{0};
		task::debounce<int>* p1 = nullptr;
		task::debounce<int>* p2 = nullptr;
		task::debounce<int> d1(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), [&](const int&, unsigned) {
			in1.store(true);
			spin(in2);
			nested += p2->fire_pending();  // its key is held by d2's call: nothing to take
		});
		task::debounce<int> d2(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), [&](const int&, unsigned) {
			in2.store(true);
			spin(in1);
			nested += p1->fire_pending();
		});
		p1 = &d1;
		p2 = &d2;
		CHECK(d1.touch(1) && d2.touch(1));
		std::size_t a = 0, b = 0;
		std::thread t1([&] { a = d1.fire_pending(); });
		std::thread t2([&] { b = d2.fire_pending(); });
		t1.join();
		t2.join();
		CHECK(a == 1 && b == 1 && nested == 0);
		stop.request();
		s.drain();
		CHECK(g.running() == 0 && d1.pending_keys() == 0 && d2.pending_keys() == 0);
	});
	std::printf("debounce-cross: both calls returned\n");
}

void debounce_lock() {
	returns_within("debounce-lock", [] {
		manual_scheduler s;
		task::group g(task::scheduler_ref::of(s));
		task::cancel_source stop;
		std::mutex user;
		std::atomic<bool> in_fire{false}, locked{false};
		task::debounce<int> d(g, {0ms, 0ms, 0ms, 0ms}, stop.token(), [&](const int&, unsigned) {
			in_fire.store(true);
			spin(locked);
			std::lock_guard l(user);  // what the later caller holds
		});
		CHECK(d.touch(1));
		std::size_t a = 0, b = 1;
		std::thread t1([&] { a = d.fire_pending(); });
		spin(in_fire);
		std::thread t2([&] {
			std::lock_guard l(user);
			locked.store(true);
			b = d.fire_pending();  // nothing pending: no key in common with the earlier call
		});
		t1.join();
		t2.join();
		CHECK(a == 1 && b == 0);
		stop.request();
		s.drain();
		CHECK(g.running() == 0 && d.pending_keys() == 0);
	});
	std::printf("debounce-lock: both calls returned\n");
}

#if defined(HAVE_FORK) && !defined(NDEBUG)
// Runs f in a child: true when it aborts (an assert), false when it exits or hangs (SIGALRM).
template <typename F>
bool aborts(const char* what, F f) {
	const pid_t pid = fork();
	if (pid == 0) {
		std::freopen("/dev/null", "w", stderr);
		alarm(10);
		f();
		_exit(0);
	}
	int status = 0;
	CHECK(waitpid(pid, &status, 0) == pid);
	std::printf("debounce-reentry %s: %s\n", what, WIFSIGNALED(status) ? strsignal(WTERMSIG(status)) : "exited");
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}
#endif

void debounce_reentry() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	std::vector<int> fires;
	task::debounce<int> inner(g, {1'000ms, 1'000ms, 2'000ms, 2'000ms}, stop.token(), [&](const int& k, unsigned) { fires.push_back(k); });
	task::debounce<int> outer(g, {10ms, 10ms, 100ms, 100ms}, stop.token(), [&](const int& k, unsigned) {
		fires.push_back(k);
		CHECK(inner.fire_pending() == 1);  // the loop's fire of another debounce of the same type: no assert
	});
	CHECK(inner.touch(2) && outer.touch(1));
	step_to(s, 50 * ms);
	CHECK((fires == std::vector<int>{1, 2}));
	stop.request();
	s.run();
	CHECK(g.running() == 0 && inner.pending_keys() == 0 && outer.pending_keys() == 0);
#if defined(HAVE_FORK) && !defined(NDEBUG)
	auto self_calling = [](bool on_loop) {  // its fire calls fire_pending(): it would wait for itself
		task::inline_scheduler s2;
		task::group g2(task::scheduler_ref::of(s2));
		task::cancel_source stop2;
		task::debounce<int>* self = nullptr;
		task::debounce<int> d(g2, {10ms, 10ms, 100ms, 100ms}, stop2.token(), [&](const int&, unsigned) { self->fire_pending(); });
		self = &d;
		d.touch(1);
		if (on_loop) {
			step_to(s2, 50 * ms);
		} else {
			d.fire_pending();
		}
		stop2.request();  // reached only without the assert
		s2.run();
	};
	CHECK(aborts("loop", [&] { self_calling(true); }));
	CHECK(aborts("fire_pending", [&] { self_calling(false); }));
#else
	std::printf("debounce-reentry: the abort is Debug-only (NDEBUG or no fork)\n");
#endif
}

void debounce_after_stop() {
	task::inline_scheduler s;
	task::group g(task::scheduler_ref::of(s));
	task::cancel_source stop;
	std::atomic<bool> in_fire{false}, touched{false};
	std::optional<bool> late;
	std::vector<int> fires;
	task::debounce<int> d(g, {100ms, 50ms, 1'000ms, 1'000ms}, stop.token(), [&](const int& k, unsigned) {
		fires.push_back(k);
		in_fire.store(true);
		const auto until = std::chrono::steady_clock::now() + 5s;
		while (!touched.load() && std::chrono::steady_clock::now() < until) { std::this_thread::yield(); }
	});
	CHECK(d.touch(1));
	s.run();
	stop.request();
	s.run();
	CHECK(g.running() == 0 && d.pending_keys() == 1);
	std::thread other([&] { CHECK(d.fire_pending() == 1); });  // the third signal's thread
	while (!in_fire.load()) { std::this_thread::yield(); }
	late = d.touch(5);  // mid fire_pending(), on another thread
	touched.store(true);
	other.join();
	CHECK(late == false && d.pending_keys() == 0 && d.fire_pending() == 0);
	CHECK((fires == std::vector<int>{1}));
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
	if (only == "all" || only == "debounce-flags") { debounce_flags(); }
	if (only == "all" || only == "debounce-throttle") { debounce_throttle(); }
	if (only == "all" || only == "debounce-throw") { debounce_throw(); }
	if (only == "all" || only == "debounce-mid-fire") { debounce_mid_fire(); }
	if (only == "all" || only == "debounce-overlap") { debounce_overlap(); }
	if (only == "all" || only == "debounce-held") { debounce_held(); }
	if (only == "all" || only == "debounce-held-twice") { debounce_held_twice(); }
	if (only == "all" || only == "debounce-cross") { debounce_cross(); }
	if (only == "all" || only == "debounce-lock") { debounce_lock(); }
	if (only == "all" || only == "debounce-reentry") { debounce_reentry(); }
	if (only == "all" || only == "debounce-after-stop") { debounce_after_stop(); }
	if (only == "all" || only == "asio") { asio_sleep(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("timers_test: ok\n");
	return 0;
}
