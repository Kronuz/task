// task::blocking_scheduler, the blocking entry's scheduler (Detent's async-drivers D5, review X3;
// the cases of its bench's `blocking_entry threads`). A caller awaits an operation that never
// completes on its own, through D8's waiter: the token's callback, on any thread, claims the
// waiter and hops to the scheduler with post_call, which resumes the caller.
//   cancel-thread         a cancel requested on another thread at 50 ms wakes a drive asleep
//                         toward a 30 s deadline; it returns cancelled at once
//   deadline-steady       a 200 ms deadline on the steady clock wakes it and cancels
//   lost-wake             no deadline: drive() returns false without sleeping
//   lost-wake-live-token  the same under a token another thread requests only after drive()
//                         returned (review R3): a live token does not hold off the stall
//   rounds                300 cross-thread cancels at 0-2 ms, each cancelled, none dropped
//   wakes                 post and post_call from another thread cut a sleep toward a far timer
//                         short; drive(done) with nothing armed, or may_wait() false, never sleeps
//   wakes-unarmed         with nothing armed and may_wait() true, drive() sleeps until a post
//                         or post_call from another thread wakes it; 5,000 rounds with the post
//                         landing around drive()'s decision to sleep, none slept through
//   starve                a call re-posting itself does not hold off a due timer
//   late                  a deadline outliving its scheduler is freed without touching it
//   offthread-cancel      the control, not in `all`: cancel off the driving thread aborts on
//                         the scheduler's assertion (Debug); it exits 0 only without one
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES). Run under TSan.
#define TASK_COUNT_FRAMES 1
#include "task.hh"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <random>
#include <string_view>
#include <thread>

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

namespace {
using namespace std::chrono_literals;
using steady = std::chrono::steady_clock;

enum outcome : int { pending = 0, cancelled = 1, given_up = 2 };

// D8's waiter, its group and caller on one scheduler: one claim word decides who resumes.
struct waiter {
	std::atomic<int> state{pending};
	std::coroutine_handle<> handle;
	task::scheduler_ref where;
	std::atomic<int>* dropped = nullptr;  // hops the scheduler refused

	bool claim(outcome o) noexcept {
		int expected = pending;
		return state.compare_exchange_strong(expected, o, std::memory_order_acq_rel);
	}
	static void hop(void* p) {  // on the scheduler: the group's last touch, then the resume
		auto* self = static_cast<waiter*>(p);
		self->where.post(self->handle);
	}
	struct on_cancel {
		waiter* self;
		void operator()() noexcept {
			if (!self->claim(cancelled)) { return; }
			if (!self->where.post_call(&hop, self)) { ++*self->dropped; }
		}
	};
};

struct await_operation {
	waiter& w;
	task::cancel_token token;
	std::optional<task::cancel_callback<waiter::on_cancel>> callback;
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) {
		w.handle = h;
		w.where = task::current();
		callback.emplace(token, waiter::on_cancel{&w});
	}
	int await_resume() noexcept {
		callback.reset();
		return w.state.load();
	}
};

// The caller's frame: the operation under a deadline on the scheduler's wheel (none for 0)
// linked to the caller's token.
task::task<int> operation(task::blocking_scheduler& s, waiter& w, task::cancel_token caller, steady::duration deadline) {
	std::optional<task::deadline<task::blocking_scheduler>> d;
	task::cancel_token token = caller;
	if (deadline != steady::duration::zero()) {
		d.emplace(s, steady::now() + deadline, caller);
		token = d->token();
	}
	co_return co_await await_operation{w, token, {}};
}

struct observed {
	std::string_view how;  // "cancelled" or "stalled"
	double elapsed_ms;
	int dropped;
};

// Drives the operation on this thread. With `live_token` another thread holds the caller's
// token: it requests it after `cancel_after_us`, or, when negative, once drive() has returned.
observed blocking(steady::duration deadline, int cancel_after_us, bool live_token) {
	task::blocking_scheduler s;
	waiter w;
	std::atomic<int> dropped{0};
	w.dropped = &dropped;
	task::cancel_source request;
	std::optional<int> state;
	std::atomic<bool> returned{false};
	const auto start = steady::now();
	task::spawn(task::scheduler_ref::of(s), operation(s, w, live_token ? request.token() : task::cancel_token{}, deadline),
		[&](std::exception_ptr e, std::optional<int> v) {
			CHECK(!e);
			state = v;
		});
	std::thread other;
	if (live_token) {
		other = std::thread([&, cancel_after_us] {
			if (cancel_after_us >= 0) {
				std::this_thread::sleep_for(std::chrono::microseconds(cancel_after_us));
			} else {
				returned.wait(false);
			}
			request.request();
		});
	}
	const bool done = s.drive([&] { return state.has_value(); });
	const double elapsed = std::chrono::duration<double, std::milli>(steady::now() - start).count();
	if (!done) {  // the entry gives up its operation: the frame runs to its end
		CHECK(w.claim(given_up));
		s.post(w.handle);
		CHECK(s.drive([&] { return state.has_value(); }) && state == given_up);
	}
	returned = true;
	returned.notify_one();
	if (other.joinable()) { other.join(); }
	CHECK(s.armed_timers() == 0 && state.has_value());
	return {done ? "cancelled" : "stalled", elapsed, dropped.load()};
}

bool report(std::string_view name, const observed& o, std::string_view want, double min_ms, double max_ms) {
	const bool ok = o.how == want && o.dropped == 0 && o.elapsed_ms >= min_ms && o.elapsed_ms < max_ms;
	std::printf("case=%.*s how=%.*s elapsed_ms=%.1f dropped=%d want=%.*s in [%.0f, %.0f) ms %s\n", int(name.size()), name.data(),
		int(o.how.size()), o.how.data(), o.elapsed_ms, o.dropped, int(want.size()), want.data(), min_ms, max_ms, ok ? "ok" : "FAILS");
	return ok;
}

void rounds(int n) {
	std::mt19937 random(7);
	double worst = 0;
	for (int r = 0; r < n; ++r) {
		const auto o = blocking(30s, int(random() % 2'000), true);
		CHECK(o.how == "cancelled" && o.dropped == 0);
		worst = std::max(worst, o.elapsed_ms);
	}
	std::printf("rounds=%d cross-thread cancels all cancelled, slowest %.1f ms ok\n", n, worst);
}

void wakes() {
	wheel::timer_entry far;
	far.fire = [](wheel::timer_entry&, bool) noexcept {};
	for (const bool by_call : {false, true}) {  // a far timer armed; another thread posts at 20 ms
		task::blocking_scheduler s;
		CHECK(task::scheduler_ref::of(s).arm(far, s.now() + 30'000'000'000ull));
		std::atomic<bool> ran{false};
		struct call {
			static void run(void* p) { static_cast<std::atomic<bool>*>(p)->store(true); }
		};
		const auto start = steady::now();
		std::thread other([&] {
			std::this_thread::sleep_for(20ms);
			if (by_call) {
				s.post_call(&call::run, &ran);
			} else {
				task::spawn(task::scheduler_ref::of(s), [](std::atomic<bool>& r) -> task::task<void> {
					r = true;
					co_return;
				}(ran));
			}
		});
		CHECK(s.drive([&] { return ran.load(); }));
		const auto elapsed = steady::now() - start;
		other.join();
		CHECK(elapsed >= 20ms && elapsed < 1s);
		CHECK(s.cancel(far));
		std::printf("wakes %s elapsed_ms=%.1f ok\n", by_call ? "post_call" : "post", std::chrono::duration<double, std::milli>(elapsed).count());
	}
	{  // nothing armed: no sleep, though another thread would post in 50 ms
		task::blocking_scheduler s;
		std::atomic<bool> ran{false};
		struct call {
			static void run(void* p) { static_cast<std::atomic<bool>*>(p)->store(true); }
		};
		std::thread other([&] {
			std::this_thread::sleep_for(50ms);
			s.post_call(&call::run, &ran);
		});
		const auto start = steady::now();
		CHECK(!s.drive([&] { return ran.load(); }));
		CHECK(steady::now() - start < 50ms && !ran);
		other.join();
		CHECK(s.drive([&] { return ran.load(); }) && ran);  // the late post is still queued
		std::printf("wakes none-armed ok\n");
	}
	{  // a timer armed but may_wait() false: no sleep
		task::blocking_scheduler s;
		CHECK(task::scheduler_ref::of(s).arm(far, s.now() + 30'000'000'000ull));
		const auto start = steady::now();
		CHECK(!s.drive([] { return false; }, [] { return false; }));
		CHECK(steady::now() - start < 50ms);
		CHECK(s.cancel(far));
		std::printf("wakes may-wait-false ok\n");
	}
}

void spin(int n) {
	for (int i = 0; i < n; ++i) { std::atomic_signal_fence(std::memory_order_seq_cst); }
}

void flag(void* p) { static_cast<std::atomic<bool>*>(p)->store(true); }

void unarmed() {
	const auto always = [] { return true; };
	for (const bool by_call : {false, true}) {  // nothing armed; another thread posts at 20 ms
		task::blocking_scheduler s;
		std::atomic<bool> ran{false};
		const auto start = steady::now();
		std::thread other([&] {
			std::this_thread::sleep_for(20ms);
			if (by_call) {
				s.post_call(&flag, &ran);
			} else {
				task::spawn(task::scheduler_ref::of(s), [](std::atomic<bool>& r) -> task::task<void> {
					r = true;
					co_return;
				}(ran));
			}
		});
		CHECK(s.drive([&] { return ran.load(); }, always));
		const auto elapsed = steady::now() - start;
		other.join();
		CHECK(elapsed >= 20ms && elapsed < 1s && s.armed_timers() == 0);
		std::printf("wakes-unarmed %s elapsed_ms=%.1f ok\n", by_call ? "post_call" : "post", std::chrono::duration<double, std::milli>(elapsed).count());
	}
	// A slept-through post never wakes: the watchdog fails a round stuck for 10 s instead of hanging.
	constexpr int rounds = 5'000;
	std::atomic<int> round{0};
	std::thread watchdog([&] {
		int seen = -1;
		auto since = steady::now();
		while (round.load() < rounds) {
			std::this_thread::sleep_for(10ms);
			if (const int now = round.load(); now != seen) {
				seen = now;
				since = steady::now();
			} else if (steady::now() - since > 10s) {
				std::fprintf(stderr, "FAIL %s:%d: round %d: a drive() slept through a post\n", __FILE__, __LINE__, now);
				std::_Exit(1);
			}
		}
	});
	std::mt19937 random(11);
	for (int r = 0; r < rounds; ++r) {
		task::blocking_scheduler s;
		std::atomic<bool> ran{false};
		const int before_post = int(random() % 2'000), in_window = int(random() % 2'000);
		std::thread other([&] {
			spin(before_post);
			s.post_call(&flag, &ran);
		});
		CHECK(s.drive([&] { return ran.load(); }, [&] {
			spin(in_window);  // between drive()'s check of the queue and its sleep
			return true;
		}));
		other.join();
		++round;
	}
	watchdog.join();
	std::printf("wakes-unarmed rounds=%d none slept through ok\n", rounds);
}

// A timer entry whose fire stops a call that keeps re-posting itself.
struct reposting : wheel::timer_entry {
	task::blocking_scheduler* s;
	steady::time_point start = steady::now();
	bool fired = false;
	std::size_t runs = 0;
	static void on_fire(wheel::timer_entry& e, bool) noexcept { static_cast<reposting&>(e).fired = true; }
	static void again(void* p) {
		auto* self = static_cast<reposting*>(p);
		++self->runs;
		if (self->fired) { return; }
		CHECK(steady::now() - self->start < 2s);  // the timer is held off
		self->s->post_call(&again, self);
	}
};

void starve() {
	task::blocking_scheduler s;
	reposting r;
	r.s = &s;
	r.fire = &reposting::on_fire;
	CHECK(task::scheduler_ref::of(s).arm(r, s.now() + 20'000'000ull));
	s.post_call(&reposting::again, &r);
	CHECK(s.drive([&] { return r.fired; }));
	const auto elapsed = steady::now() - r.start;
	CHECK(elapsed >= 20ms && elapsed < 1s && s.armed_timers() == 0);
	std::printf("starve runs=%zu elapsed_ms=%.1f ok\n", r.runs, std::chrono::duration<double, std::milli>(elapsed).count());
}

void late() {
	auto s = std::make_unique<task::blocking_scheduler>();
	std::optional<task::deadline<task::blocking_scheduler>> d;
	d.emplace(*s, steady::now() + 60s);
	const task::cancel_token token = d->token();
	CHECK(s->armed_timers() == 1);
	s.reset();  // fires the entry off the wheel with expired false
	d.reset();  // touches nothing of the scheduler
	CHECK(!token.requested());
	std::printf("late ok\n");
}

int offthread_cancel() {
	task::blocking_scheduler s;
	wheel::timer_entry e;
	e.fire = [](wheel::timer_entry&, bool) noexcept {};
	std::printf("offthread-cancel: cancelling off the driving thread (expected: the assertion aborts)\n");
	std::fflush(stdout);
	std::thread([&] { s.cancel(e); }).join();
	std::printf("offthread-cancel: no assertion\n");
	return 0;
}
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	if (only == "offthread-cancel") { return offthread_cancel(); }
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "cancel-thread") { CHECK(report("cancel-thread", blocking(30s, 50'000, true), "cancelled", 50, 1'000)); }
	if (only == "all" || only == "deadline-steady") { CHECK(report("deadline-steady", blocking(200ms, -1, false), "cancelled", 200, 1'200)); }
	if (only == "all" || only == "lost-wake") { CHECK(report("lost-wake", blocking(steady::duration::zero(), -1, false), "stalled", 0, 100)); }
	if (only == "all" || only == "lost-wake-live-token") {
		CHECK(report("lost-wake-live-token", blocking(steady::duration::zero(), -1, true), "stalled", 0, 100));
	}
	if (only == "all" || only == "rounds") { rounds(300); }
	if (only == "all" || only == "wakes") { wakes(); }
	if (only == "all" || only == "wakes-unarmed") { unarmed(); }
	if (only == "all" || only == "starve") { starve(); }
	if (only == "all" || only == "late") { late(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("blocking_test: ok\n");
	return 0;
}
