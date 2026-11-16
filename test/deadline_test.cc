// task::deadline on inline_scheduler's wheel, and timers armed through scheduler_ref
// (Kronuz/wheel's own tests cover the wheel):
//   manual    deadline<inline_scheduler> on its manual clock, advance() fires it, no real time
//   chain     a fired parent deadline cancels a linked child whose callback destroys it
//   ref       scheduler_ref::arm and cancel on inline_scheduler; false without timers
//   reentry   a fire cancels or re-arms another due entry, or itself, or arms one already due
//   model     random arm, cancel and re-arm, from fires too, against a reference model
//   long      deadlines beyond a 4,096 ms round and up to days: never early, at most a step late
//   post_call inline_scheduler's same-thread post_call, in order with post, and a cancel's hop
// The timers are armed through scheduler_ref and fired by advance(), as a simulator drives them.
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "task.hh"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string_view>
#include <vector>

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

namespace {
using u64 = std::uint64_t;
constexpr u64 ms = 1'000'000;

// Waits for the token; returns its reason as an int.
task::task<int> wait_for_cancel(task::cancel_token token) {
	struct awaiter {
		task::cancel_token token;
		std::coroutine_handle<> handle;
		task::scheduler_ref where;
		struct resume {
			awaiter* self;
			void operator()() noexcept { self->where.post(self->handle); }
		};
		std::optional<task::cancel_callback<resume>> callback;
		bool await_ready() const noexcept { return token.requested(); }
		void await_suspend(std::coroutine_handle<> h) {
			handle = h;
			where = task::current();
			callback.emplace(token, resume{this});
		}
		int await_resume() const noexcept { return int(token.reason()); }
	};
	co_return co_await awaiter{token, {}, {}, {}};
}

void manual() {
	using namespace std::chrono_literals;
	task::inline_scheduler s;
	const auto start = std::chrono::steady_clock::now();
	{
		task::deadline d(s, task::inline_scheduler::clock::time_point(5s));  // five seconds of simulated time
		int reason = -1;
		task::spawn(task::scheduler_ref::of(s), wait_for_cancel(d.token()), [&](std::exception_ptr, std::optional<int> r) { reason = *r; });
		s.run();
		CHECK(reason == -1 && !d.expired());
		s.advance(4'999 * ms);
		s.run();
		CHECK(reason == -1 && !d.expired());
		s.advance(1 * ms);
		s.run();
		CHECK(d.expired() && reason == int(task::cancel_reason::deadline));
		// one destroyed before its time never fires and leaves the wheel
		auto early = std::make_unique<task::deadline<task::inline_scheduler>>(s, task::inline_scheduler::clock::time_point(60s));
		const auto token = early->token();
		CHECK(s.armed_timers() == 1);
		early.reset();
		CHECK(s.armed_timers() == 0);
		s.advance(120'000 * ms);
		CHECK(!token.requested());
	}
	const auto real = std::chrono::steady_clock::now() - start;
	CHECK(real < 1s);
	std::printf("manual simulated_ms=%llu real_us=%lld ok\n", static_cast<unsigned long long>(s.now() / ms),
		static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(real).count()));
}

// A parent deadline and a linked child's, both due in one advance. The parent's fire requests
// the child, whose callback destroys the child deadline while its entry waits on the due list.
void chain() {
	for (int order = 0; order < 2; ++order) {
		task::inline_scheduler s;
		using tp = task::inline_scheduler::clock::time_point;
		auto parent = std::make_unique<task::deadline<task::inline_scheduler>>(s, tp(std::chrono::nanoseconds(order ? 3 * ms : 2 * ms)));
		auto child = std::make_unique<task::deadline<task::inline_scheduler>>(s, tp(std::chrono::nanoseconds(order ? 2 * ms : 3 * ms)), parent->token());
		const auto child_token = child->token();
		bool destroyed = false;
		task::cancel_callback on_cancel(parent->token(), [&] {
			child.reset();
			destroyed = true;
		});
		s.advance(4 * ms);
		CHECK(destroyed && child_token.requested() && s.armed_timers() == 0);
		CHECK(order ? child_token.reason() == task::cancel_reason::deadline : true);
		parent.reset();
		std::printf("chain order=%s ok\n", order ? "child-first" : "parent-first");
	}
}

struct no_wheel {
	void post(std::coroutine_handle<>) {}
	std::uint64_t now() const noexcept { return 0; }
};

void ref() {
	static int fired = 0;
	wheel::timer_entry e;
	e.fire = [](wheel::timer_entry&, bool expired) noexcept { fired += expired; };
	task::inline_scheduler s;
	const auto r = task::scheduler_ref::of(s);
	s.advance(3 * ms);
	CHECK(r.now() == 3 * ms);
	CHECK(r.arm(e, r.now() + 2 * ms) && s.armed_timers() == 1);
	CHECK(r.cancel(e) && !r.cancel(e) && s.armed_timers() == 0);
	CHECK(r.arm(e, r.now() + 2 * ms));
	s.advance(1 * ms);
	CHECK(fired == 0);
	s.advance(1 * ms);
	CHECK(fired == 1 && !e.armed());
	no_wheel plain;
	CHECK(!task::scheduler_ref::of(plain).arm(e, 0) && !e.armed() && !task::scheduler_ref::of(plain).cancel(e));
	CHECK(!task::scheduler_ref{}.arm(e, 0) && !task::scheduler_ref{}.cancel(e));
	std::printf("ref ok\n");
}
// Entries whose fire calls `on_fire`, the case's observer.
std::function<void(wheel::timer_entry&)> on_fire;
void observed(wheel::timer_entry& e, bool expired) noexcept {
	if (expired) { on_fire(e); }
}

// The reviewer's probe (Detent async-drivers-bench, review F1.3, F2) and its variants, on one slot.
void reentry() {
	for (const std::string_view mode : {"cancel", "rearm", "self", "arm-due"}) {
		task::inline_scheduler s;
		const auto r = task::scheduler_ref::of(s);
		wheel::timer_entry a, b, c;
		a.fire = b.fire = c.fire = &observed;
		CHECK(r.arm(a, 500'000) && r.arm(b, 600'000));  // both due in slot 0; b is the list head
		int fired_a = 0, fired_b = 0, fired_c = 0, fired = 0;
		on_fire = [&](wheel::timer_entry& e) {
			++fired;
			if (&e == &b) {
				++fired_b;
				if (mode == "cancel") { CHECK(r.cancel(a)); }
				if (mode == "rearm") { r.arm(a, 50 * ms); }
				if (mode == "self" && fired_b == 1) { r.arm(b, 1 * ms); }  // due now: fires on the next expire
				if (mode == "arm-due") { r.arm(c, 0); }                      // a new entry already due
			} else if (&e == &a) {
				++fired_a;
			} else {
				++fired_c;
			}
		};
		s.advance(1 * ms);
		const int first = std::exchange(fired, 0);
		if (mode == "cancel") { CHECK(first == 1 && fired_a == 0 && !a.armed() && s.armed_timers() == 0); }
		if (mode == "rearm") {
			CHECK(first == 1 && fired_a == 0 && a.armed() && s.armed_timers() == 1);
			s.advance(48 * ms);
			CHECK(fired == 0);
			s.advance(1 * ms);
			CHECK(fired == 1 && fired_a == 1);
		}
		if (mode == "self") {
			CHECK(first == 2 && fired_b == 1 && b.armed());
			s.advance(0);
			CHECK(fired == 1 && fired_b == 2 && s.armed_timers() == 0);
		}
		if (mode == "arm-due") {
			CHECK(first == 2 && fired_c == 0 && c.armed());
			s.advance(0);
			CHECK(fired == 1 && fired_c == 1);
		}
		std::printf("reentry mode=%.*s ok\n", int(mode.size()), mode.data());
	}
	on_fire = nullptr;
}

// Random operations against a reference: an entry fires only when due, once per arm, and every
// entry due when advance() began fires in it unless a fire cancelled or re-armed it first.
void model() {
	constexpr std::size_t n = 512;
	constexpr int rounds = 20'000;
	std::mt19937_64 random(20261109);
	std::vector<wheel::timer_entry> entries(n);
	for (auto& e : entries) { e.fire = &observed; }
	std::map<const wheel::timer_entry*, u64> armed;  // the reference: entry -> deadline
	std::set<const wheel::timer_entry*> due;
	task::inline_scheduler s;
	const auto r = task::scheduler_ref::of(s);
	u64 fires = 0, mutations = 0;
	on_fire = [&](wheel::timer_entry& e) {
		CHECK(due.count(&e) == 1);  // due when advance began, never fired twice
		CHECK(armed.at(&e) <= s.now());
		due.erase(&e);
		armed.erase(&e);
		++fires;
		if (random() % 3 == 0) {  // a fire mutates another entry, perhaps itself
			auto& other = entries[random() % n];
			due.erase(&other);
			++mutations;
			if (random() % 2) {
				r.cancel(other);
				armed.erase(&other);
			} else {
				const u64 at = s.now() + random() % (5'000 * ms);
				r.arm(other, at);
				armed[&other] = at;
			}
		}
	};
	for (int round = 0; round < rounds; ++round) {
		for (int k = 0; k < 8; ++k) {
			auto& e = entries[random() % n];
			if (random() % 4 == 0) {
				CHECK(r.cancel(e) == (armed.erase(&e) == 1));
			} else {
				const u64 at = s.now() + random() % (12'000 * ms);  // up to three rounds ahead
				r.arm(e, at);
				armed[&e] = at;
			}
		}
		u64 step = random() % (3 * ms);
		if (random() % 500 == 0) { step += 9'000 * ms; }  // a gap longer than a round
		due.clear();
		for (const auto& [e, at] : armed) {
			if (at <= s.now() + step) { due.insert(e); }
		}
		s.advance(step);
		CHECK(due.empty());
		CHECK(s.armed_timers() == armed.size());
		for (const auto& [e, at] : armed) { CHECK(e->armed() && e->deadline == at); }
	}
	on_fire = nullptr;
	std::printf("model rounds=%d fires=%llu mutations_from_fires=%llu ok\n", rounds, static_cast<unsigned long long>(fires),
		static_cast<unsigned long long>(mutations));
}

// A deadline beyond one round waits in an upper level and moves down as the clock passes; it
// fires in the first advance() that reaches it, never before. Steps of up to 2 ms walk the clock.
void long_deadlines() {
	using tp = task::inline_scheduler::clock::time_point;
	std::mt19937_64 random(4096);
	for (const u64 d : {4'095ull, 4'096ull, 10'000ull, 30'000ull, 300'000ull}) {
		task::inline_scheduler s;
		const u64 at = d * ms + 123;
		task::deadline<task::inline_scheduler> dl(s, tp(std::chrono::nanoseconds(at)));
		u64 steps = 0;
		while (!dl.expired()) {
			const u64 before = s.now();
			s.advance(1 + random() % (2 * ms));
			++steps;
			CHECK(dl.expired() == (s.now() >= at));
			if (dl.expired()) { CHECK(before < at); }  // the first advance past it
		}
		std::printf("long deadline_ms=%llu steps=%llu ok\n", static_cast<unsigned long long>(d), static_cast<unsigned long long>(steps));
	}
	for (const u64 days : {1ull, 40ull}) {  // clock jumps: one advance short of it, then past it
		task::inline_scheduler s;
		const u64 at = days * 86'400'000 * ms + 7;
		task::deadline<task::inline_scheduler> dl(s, tp(std::chrono::nanoseconds(at)));
		s.advance(at - 1);
		CHECK(!dl.expired() && s.armed_timers() == 1);
		s.advance(1);
		CHECK(dl.expired() && s.armed_timers() == 0);
		std::printf("long days=%llu ok\n", static_cast<unsigned long long>(days));
	}
}

// A cancel's hop (async-drivers D8): the token's callback posts a call to the scheduler, which
// posts the waiting frame. On inline_scheduler both run on its thread, in post order.
task::task<int> hop_waiter(task::cancel_token token) {
	struct awaiter {
		task::cancel_token token;
		std::coroutine_handle<> handle;
		task::scheduler_ref where;
		bool hopped = false;
		static void hop(void* p) {
			auto* self = static_cast<awaiter*>(p);
			CHECK(task::current() == self->where);  // the call runs in the scheduler's scope
			self->hopped = true;
			self->where.post(self->handle);
		}
		struct on_cancel {
			awaiter* self;
			void operator()() noexcept { CHECK(self->where.post_call(&hop, self)); }
		};
		std::optional<task::cancel_callback<on_cancel>> callback;
		bool await_ready() const noexcept { return token.requested(); }
		void await_suspend(std::coroutine_handle<> h) {
			handle = h;
			where = task::current();
			callback.emplace(token, on_cancel{this});
		}
		int await_resume() const noexcept { return hopped ? int(token.reason()) : -1; }
	};
	co_return co_await awaiter{token, {}, {}, false, {}};
}

void post_call() {
	task::inline_scheduler s;
	std::vector<int> order;
	struct call {
		std::vector<int>* order;
		int value;
		static void run(void* p) {
			auto* c = static_cast<call*>(p);
			c->order->push_back(c->value);
		}
	} one{&order, 1}, three{&order, 3};
	const auto r = task::scheduler_ref::of(s);
	CHECK(r.post_call(&call::run, &one));
	task::spawn(r, [](std::vector<int>& o) -> task::task<void> {
		o.push_back(2);
		co_return;
	}(order));
	CHECK(r.post_call(&call::run, &three));
	CHECK(s.queued() == 3);
	CHECK(s.run() == 3 && order == std::vector<int>({1, 2, 3}));

	using tp = task::inline_scheduler::clock::time_point;
	task::deadline<task::inline_scheduler> d(s, tp(std::chrono::nanoseconds(5 * ms)));
	int reason = -2;
	task::spawn(r, hop_waiter(d.token()), [&](std::exception_ptr, std::optional<int> v) { reason = *v; });
	s.run();
	CHECK(reason == -2);
	s.advance(5 * ms);  // the fire's callback posts the hop; run() runs it, then the frame
	CHECK(reason == -2 && s.queued() == 1);
	s.run();
	CHECK(reason == int(task::cancel_reason::deadline));
	std::printf("post_call ok\n");
}
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "manual") { manual(); }
	if (only == "all" || only == "chain") { chain(); }
	if (only == "all" || only == "ref") { ref(); }
	if (only == "all" || only == "reentry") { reentry(); }
	if (only == "all" || only == "model") { model(); }
	if (only == "all" || only == "long") { long_deadlines(); }
	if (only == "all" || only == "post_call") { post_call(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("deadline_test: ok\n");
	return 0;
}
