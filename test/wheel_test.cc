// task::timer_wheel and the deadlines inline_scheduler files on it:
//   reentry   a fire cancels or re-arms another due entry, or itself
//   model     random arm, cancel and re-arm, from fires too, against a reference, with
//             deadlines up to 40 days ahead and clock jumps longer than a round
//   far       deadlines beyond a round: never early, at most a tick late, a few wakes each;
//             a million far entries leave level 0 empty and cost no tick a walk
//   cross     arm_remote of unarmed entries from 1, 4 and 14 threads: each fires once
//   owner     (Debug) a cancel and a re-arm from another thread abort on the owner assertion
//   manual    deadline<inline_scheduler> on its manual clock, advance() fires it, no real time
//   chain     a fired parent deadline cancels a linked child whose callback destroys it
//   ref       scheduler_ref::wheel()
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "task.hh"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string_view>
#include <thread>
#include <vector>

#ifndef NDEBUG
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

namespace {
using task::timer_entry;
using task::timer_wheel;
using u64 = std::uint64_t;
constexpr u64 ms = 1'000'000;
constexpr u64 day = 86'400'000 * ms;

void reentry() {
	for (const std::string_view mode : {"cancel", "rearm", "self", "arm-due"}) {
		timer_wheel w(0);
		timer_entry a, b, c;
		w.arm(a, 500'000);  // both due in slot 0; b is the list head, a follows
		w.arm(b, 600'000);
		int fired_a = 0, fired_b = 0, fired_c = 0;
		auto fire = [&](timer_entry& e) {
			if (&e == &b) {
				++fired_b;
				if (mode == "cancel") { w.cancel(a); }
				if (mode == "rearm") { w.arm(a, 50 * ms); }
				if (mode == "self" && fired_b == 1) { w.arm(b, 1 * ms); }  // due now: fires on the next expire
				if (mode == "arm-due") { w.arm(c, 0); }
			} else if (&e == &a) {
				++fired_a;
			} else {
				++fired_c;
			}
		};
		const auto first = w.expire(1 * ms, fire);
		if (mode == "cancel") { CHECK(first == 1 && fired_a == 0 && !a.armed() && w.size() == 0); }
		if (mode == "rearm") {
			CHECK(first == 1 && fired_a == 0 && a.armed() && w.size() == 1);
			CHECK(w.expire(49 * ms, fire) == 0);
			CHECK(w.expire(50 * ms, fire) == 1 && fired_a == 1);
		}
		if (mode == "self") {
			CHECK(first == 2 && fired_b == 1 && b.armed());
			CHECK(w.expire(1 * ms, fire) == 1 && fired_b == 2 && w.size() == 0);
		}
		if (mode == "arm-due") {
			CHECK(first == 2 && fired_c == 0 && c.armed());
			CHECK(w.expire(1 * ms, fire) == 1 && fired_c == 1);
		}
		std::printf("reentry mode=%.*s ok\n", int(mode.size()), mode.data());
	}
}

// An entry fires only when due, at most a tick plus a step late, exactly once per arm, and
// every entry due when expire() began fires in it unless a fire cancelled or re-armed it.
void model() {
	constexpr std::size_t n = 512;
	std::mt19937_64 random(20261111);
	std::vector<timer_entry> entries(n);
	std::map<const timer_entry*, u64> armed;  // the reference: entry -> deadline
	timer_wheel w(7'000 * ms);
	u64 now = 7'000 * ms, fires = 0, mutations = 0, far = 0;
	auto pick = [&]() -> u64 {
		switch (random() % 8) {
		case 0: return random() % (40 * day);           // the upper levels
		case 1: return random() % (600'000 * ms);       // levels 1 and 2
		case 2: return random() % (20'000 * ms);        // a few rounds
		default: return random() % (3'000 * ms);        // level 0
		}
	};
	for (int round = 0; round < 40'000; ++round) {
		for (int k = 0; k < 8; ++k) {
			auto& e = entries[random() % n];
			if (random() % 4 == 0) {
				w.cancel(e);
				armed.erase(&e);
			} else {
				const u64 at = now + pick();
				far += at - now >= timer_wheel::slots * ms;
				w.arm(e, at);
				armed[&e] = at;
			}
		}
		u64 step = random() % (3 * ms);
		if (random() % 500 == 0) { step += random() % (2 * day); }  // a gap of many rounds
		now += step;
		std::set<const timer_entry*> due;
		for (const auto& [e, at] : armed) {
			if (at <= now) { due.insert(e); }
		}
		w.expire(now, [&](timer_entry& e) {
			CHECK(due.count(&e) == 1);  // due when expire began, never fired twice
			CHECK(armed.at(&e) <= now && armed.at(&e) == e.deadline);
			due.erase(&e);
			armed.erase(&e);
			++fires;
			if (random() % 3 == 0) {  // a fire mutates an entry, itself included
				auto& other = random() % 4 == 0 ? e : entries[random() % n];
				due.erase(&other);
				++mutations;
				if (random() % 2) {
					w.cancel(other);
					armed.erase(&other);
				} else {
					const u64 at = now + 1 + pick();
					w.arm(other, at);
					armed[&other] = at;
				}
			}
		});
		CHECK(due.empty());
		CHECK(w.size() == armed.size());
		for (const auto& [e, at] : armed) { CHECK(e->armed() && e->deadline == at && at > now); }
		const u64 wake = w.next();
		CHECK(armed.empty() ? wake == timer_wheel::never : wake > now);
		for (const auto& [e, at] : armed) { CHECK(wake <= (at / timer_wheel::tick + 1) * timer_wheel::tick); }  // never wakes past a deadline's slot
	}
	std::printf("model rounds=40000 fires=%llu far_arms=%llu mutations_from_fires=%llu ok\n", static_cast<unsigned long long>(fires),
		static_cast<unsigned long long>(far), static_cast<unsigned long long>(mutations));
}

// The loop wakes at next() and expires: a far deadline fires in its own tick, never before it,
// after at most one early wake per level it moves through.
void far_deadlines() {
	for (const u64 d : {4'095 * ms, 4'096 * ms, 10'000 * ms, 30'000 * ms, 300'000 * ms, day, 30 * day, 1000 * day}) {
		timer_wheel w(0);
		timer_entry e;
		bool fired = false;
		u64 fired_at = 0, wakes = 0;
		w.arm(e, d + 123);
		for (u64 now = 0; !fired;) {
			now = w.next();
			CHECK(now != timer_wheel::never);
			++wakes;
			w.expire(now, [&](timer_entry&) { fired = true; fired_at = now; });
		}
		CHECK(fired_at >= d + 123 && fired_at - (d + 123) <= timer_wheel::tick);
		CHECK(wakes <= timer_wheel::levels + 1);
		std::printf("far deadline_ms=%llu wakes=%llu late_ns=%llu ok\n", static_cast<unsigned long long>(d / ms),
			static_cast<unsigned long long>(wakes), static_cast<unsigned long long>(fired_at - (d + 123)));
	}
	// A million entries a day out stay above level 0: the next wake is the span of their level-2
	// slot, not a 1 ms slot, and a run of 1 ms ticks walks none of them.
	constexpr std::size_t n = 1'000'000;
	std::vector<timer_entry> entries(n);
	std::mt19937_64 random(24);
	timer_wheel w(0);
	for (auto& e : entries) { w.arm(e, day + random() % (60'000 * ms)); }
	CHECK(w.next() > timer_wheel::slots * ms);
	const auto start = std::chrono::steady_clock::now();
	std::size_t fired = 0;
	for (u64 t = 0; t < 100'000 * ms; t += ms) { fired += w.expire(t); }
	const auto carry = std::chrono::steady_clock::now() - start;
	CHECK(fired == 0 && w.size() == n);
	std::size_t total = 0;
	for (u64 t = day; t <= day + 61'000 * ms; t += ms) {
		total += w.expire(t, [&](timer_entry& e) { CHECK(e.deadline <= t); });
	}
	CHECK(total == n && w.size() == 0);
	std::printf("far carry entries=%zu ticks=100000 us=%lld ok\n", n,
		static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(carry).count()));
}

// Unarmed entries armed from P threads while the owner expires: each fires once, never early.
void cross() {
	for (const std::size_t p : {1, 4, 14}) {
		constexpr std::size_t per = 20'000;
		std::vector<timer_entry> entries(p * per);
		std::vector<int> fires(entries.size(), 0);
		timer_wheel w(0);
		std::atomic<u64> clock{0};
		std::atomic<std::size_t> done{0};
		std::vector<std::thread> threads;
		for (std::size_t k = 0; k < p; ++k) {
			threads.emplace_back([&, k] {
				std::mt19937_64 random(k);
				for (std::size_t i = 0; i < per; ++i) {
					const u64 at = clock.load(std::memory_order_relaxed) + random() % (200 * ms) + (i % 97 == 0 ? 10'000 * ms : 0);
					w.arm_remote(entries[k * per + i], at);
				}
				done.fetch_add(1);
			});
		}
		std::size_t fired = 0;
		u64 now = 0;
		while (fired < entries.size()) {
			now += ms / 2;
			clock.store(now, std::memory_order_relaxed);
			fired += w.expire(now, [&](timer_entry& e) {
				CHECK(e.deadline <= now);
				++fires[static_cast<std::size_t>(&e - entries.data())];
			});
			if (done.load() == p && now > 60'000 * ms) { break; }
		}
		for (auto& t : threads) { t.join(); }
		for (int f : fires) { CHECK(f == 1); }
		CHECK(w.size() == 0);
		std::printf("cross producers=%zu arms=%zu ok\n", p, entries.size());
	}
}

#ifndef NDEBUG
// Binds the wheel to this thread, then runs `op` on another; the child must die on SIGABRT.
template <class Op>
bool aborts_off_owner(Op op) {
	std::fflush(stdout);
	const pid_t pid = fork();
	if (pid == 0) {
		timer_wheel w(0);
		timer_entry e;
		w.arm(e, 5 * ms);
		std::thread([&] { op(w, e); }).join();
		std::_Exit(0);
	}
	int status = 0;
	waitpid(pid, &status, 0);
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}
void owner() {
	CHECK(aborts_off_owner([](timer_wheel& w, timer_entry& e) { w.cancel(e); }));
	CHECK(aborts_off_owner([](timer_wheel& w, timer_entry& e) { w.arm(e, 9 * ms); }));
	CHECK(aborts_off_owner([](timer_wheel& w, timer_entry&) { w.expire(1 * ms); }));
	std::printf("owner cancel, re-arm and expire off the owner abort ok\n");
}
#endif

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
		CHECK(s.wheel().size() == 1);
		early.reset();
		CHECK(s.wheel().size() == 0);
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
		CHECK(destroyed && child_token.requested() && s.wheel().size() == 0);
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
	task::inline_scheduler s;
	CHECK(task::scheduler_ref::of(s).wheel() == &s.wheel());
	no_wheel plain;
	CHECK(task::scheduler_ref::of(plain).wheel() == nullptr);
	CHECK(task::scheduler_ref{}.wheel() == nullptr);
	std::printf("ref ok\n");
}
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "reentry") { reentry(); }
	if (only == "all" || only == "model") { model(); }
	if (only == "all" || only == "far") { far_deadlines(); }
	if (only == "all" || only == "cross") { cross(); }
#ifndef NDEBUG
	if (only == "all" || only == "owner") { owner(); }
#endif
	if (only == "all" || only == "manual") { manual(); }
	if (only == "all" || only == "chain") { chain(); }
	if (only == "all" || only == "ref") { ref(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("wheel_test: ok\n");
	return 0;
}
