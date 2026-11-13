/*
 * Copyright © 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The heavy lane (docs/lanes.md): budget's light_slices, the lane in scheduler_ref, and
// asio_scheduler's heavy queue. `task_lanes_test NAME` runs one test; no argument runs all.
#include "task_asio.hh"

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
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

namespace {

constexpr std::chrono::nanoseconds spent_slice{1};  // every checkpoint finds it spent

// A completion another thread (or a later turn) delivers through the reference captured at
// suspension, as a chore, an event or a body read does.
struct parked {
	task::scheduler_ref ref;
	std::coroutine_handle<> h;
	std::atomic<bool> ready{false};
	void wait() const {
		while (!ready.load(std::memory_order_acquire)) { std::this_thread::yield(); }
	}
	void wake() {
		ready.store(false, std::memory_order_relaxed);
		ref.post(h);
	}
};
struct park_on {
	parked& p;
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) noexcept {
		p.ref = task::current();
		p.h = h;
		p.ready.store(true, std::memory_order_release);
	}
	void await_resume() const noexcept {}
};

// A loop on its own thread, kept running until finish().
struct loop_thread {
	asio::io_context io;
	asio::executor_work_guard<asio::io_context::executor_type> guard{io.get_executor()};
	task::asio_scheduler s{io.get_executor()};
	std::thread t{[this] { io.run(); }};
	void finish() {
		guard.reset();
		t.join();
	}
};

bool heavy_now() { return task::current().heavy(); }

void spin(std::chrono::nanoseconds d) {
	auto end = std::chrono::steady_clock::now() + d;
	while (std::chrono::steady_clock::now() < end) {}
}

// Between two consecutive light turns at most one heavy turn runs: a light task's spent slice
// requeues it with the ordinary posts, not in the heavy round robin.
void light_overtakes_heavy() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::string trace;
	auto worker = [&](char id, unsigned light_slices) -> task::task<void> {
		task::budget b(spent_slice, {}, light_slices);
		for (int i = 0; i < 20; ++i) {
			co_await b.checkpoint();
			trace += id;
		}
	};
	auto r = task::scheduler_ref::of(s);
	task::spawn(r, worker('a', 1));  // heavy at its first spent slice
	task::spawn(r, worker('b', 1));
	task::spawn(r, worker('L', 1000));
	io.run();
	const auto first = trace.find('L'), last = trace.rfind('L');
	CHECK(first != std::string::npos);
	int heavy_between = 0, worst = 0;
	for (auto i = first; i <= last; ++i) {
		if (trace[i] == 'L') {
			worst = std::max(worst, heavy_between);
			heavy_between = 0;
		} else {
			++heavy_between;
		}
	}
	if (worst > 1) { std::fprintf(stderr, "trace %s\n", trace.c_str()); }
	CHECK(worst == 1);
}

// A timer that expires while heavy turns run waits for at most two heavy turns: the drainer
// takes one turn per pass, and the loop polls its timers between passes.
void timer_within_two_heavy_turns() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	using clock = std::chrono::steady_clock;
	std::vector<clock::time_point> turns;
	auto heavy = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 1);
		for (int i = 0; i < 25; ++i) {
			co_await b.checkpoint();
			turns.push_back(clock::now());
			spin(200us);
		}
	};
	for (int i = 0; i < 4; ++i) { task::spawn(task::scheduler_ref::of(s), heavy()); }
	asio::steady_timer t(io);
	clock::time_point expiry, fired;
	asio::post(io, [&] {
		expiry = clock::now() + 3ms;
		t.expires_at(expiry);
		t.async_wait([&](const asio::error_code&) { fired = clock::now(); });
	});
	io.run();
	int late = 0;
	for (auto at : turns) { late += at >= expiry && at < fired; }
	if (late > 2) { std::fprintf(stderr, "heavy turns between the expiry and the timer: %d\n", late); }
	CHECK(fired >= expiry);
	CHECK(late <= 2);
}

// A heavy task beside a light task that reposts itself runs one turn per pass: it finishes
// while the flood still runs, within about one light turn per heavy turn.
void heavy_progresses_under_flood() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	constexpr int flood = 3000, heavy_turns = 20;
	int light_turns = 0, heavy_done_at = -1;
	auto light = [&]() -> task::task<void> {
		for (; light_turns < flood; ++light_turns) { co_await task::yield(); }
	};
	auto heavy = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 1);
		for (int i = 0; i < heavy_turns; ++i) { co_await b.checkpoint(); }
		heavy_done_at = light_turns;
	};
	task::spawn(task::scheduler_ref::of(s), light());
	task::spawn(task::scheduler_ref::of(s), heavy());
	io.run();
	if (heavy_done_at > 2 * heavy_turns) { std::fprintf(stderr, "the heavy task finished at light turn %d\n", heavy_done_at); }
	CHECK(heavy_done_at >= 0 && heavy_done_at <= 2 * heavy_turns);
}

// A heavy task's completion, posted from another thread through its captured reference,
// resumes from the heavy lane: a light completion posted after it runs first.
void heavy_sticky_across_chore() {
	loop_thread l;
	parked ph, pl;
	std::string order;
	std::size_t demoted_at_light = 99;
	bool resumed_heavy = false;
	std::atomic<int> done{0};
	auto heavy = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 1);
		co_await b.checkpoint();  // the first spent slice: heavy from here on
		co_await park_on{ph};
		resumed_heavy = heavy_now();
		order += 'H';
		++done;
	};
	auto light = [&]() -> task::task<void> {
		co_await park_on{pl};
		demoted_at_light = l.s.demoted();
		order += 'L';
		++done;
	};
	task::spawn(task::scheduler_ref::of(l.s), heavy());
	task::spawn(task::scheduler_ref::of(l.s), light());
	ph.wait();
	pl.wait();
	CHECK(ph.ref.heavy() && !pl.ref.heavy());
	ph.wake();
	pl.wake();
	while (done.load() < 2) { std::this_thread::yield(); }
	l.finish();
	CHECK(order == "LH");
	CHECK(demoted_at_light == 1);
	CHECK(resumed_heavy);
}

// post_heavy and post_demoted from another thread hop to the loop: the handle is queued (and
// counted) only once the loop runs the hop. Then eight threads each post 10,000 resumes
// across, half of them heavy, and every one runs once (run it under TSan).
void cross_thread_hop() {
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		parked p;
		bool resumed_heavy = false;
		auto t = [&]() -> task::task<void> {
			co_await park_on{p};
			resumed_heavy = heavy_now();
		};
		task::spawn(task::scheduler_ref::of(s), t());
		io.poll();
		CHECK(p.ready.load());
		std::thread([&] { p.ref.with_heavy(true).post(p.h); }).join();
		CHECK(s.demoted() == 0);  // the hop waits in the loop's queue
		io.poll();
		CHECK(resumed_heavy);
		CHECK(s.demoted() == 0);
	}
	if (failures) { return; }  // the stress below assumes the hop
	loop_thread l;
	constexpr int threads = 8, rounds = 10'000;
	std::vector<parked> slots(threads);
	std::vector<int> resumes(threads, 0);
	std::vector<int> heavy_resumes(threads, 0);
	std::atomic<int> finished{0};
	auto t = [&](int i) -> task::task<void> {
		for (int r = 0; r < rounds; ++r) {
			co_await park_on{slots[i]};
			++resumes[i];
			heavy_resumes[i] += heavy_now();
		}
		++finished;
	};
	for (int i = 0; i < threads; ++i) { task::spawn(task::scheduler_ref::of(l.s), t(i)); }
	std::vector<std::thread> posters;
	for (int i = 0; i < threads; ++i) {
		posters.emplace_back([&, i] {
			for (int r = 0; r < rounds; ++r) {
				parked& p = slots[i];
				p.wait();
				p.ready.store(false, std::memory_order_relaxed);
				if (i % 2) {
					l.s.post_heavy(p.h);
				} else {
					l.s.post_demoted(p.h);
				}
			}
		});
	}
	for (auto& p : posters) { p.join(); }
	while (finished.load() < threads) { std::this_thread::yield(); }
	l.finish();
	for (int i = 0; i < threads; ++i) {
		CHECK(resumes[i] == rounds);
		CHECK(heavy_resumes[i] == (i % 2 ? rounds : 0));
	}
	CHECK(l.s.demoted() == 0);
}

// light_slices 0 is today's policy: every spent slice demoted, and no scope turns heavy, so a
// completion delivered after a demotion returns to the ordinary posts. The trace is today's.
void default_unchanged() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::string trace;
	int heavy_seen = 0;
	parked p1, p2;
	auto mark = [&](char id, char step) {
		trace += id;
		trace += step;
		heavy_seen += heavy_now();
	};
	auto d = [&](char id, parked& p) -> task::task<void> {
		task::budget b(spent_slice);
		co_await b.checkpoint();
		mark(id, '1');
		co_await park_on{p};
		mark(id, '2');
		co_await b.checkpoint();
		mark(id, '3');
		CHECK(b.spent() == 2);
	};
	auto l = [&]() -> task::task<void> {
		co_await task::yield();
		while (!p1.ready || !p2.ready) { co_await task::yield(); }
		mark('l', '1');
		p1.wake();
		p2.wake();
		for (char c : {'2', '3', '4'}) {
			co_await task::yield();
			mark('l', c);
		}
	};
	auto r = task::scheduler_ref::of(s);
	task::spawn(r, d('a', p1));
	task::spawn(r, d('b', p2));
	task::spawn(r, l());
	io.run();
	if (trace != "a1b1l1a2b2l2a3l3b3l4") { std::fprintf(stderr, "trace %s\n", trace.c_str()); }
	CHECK(trace == "a1b1l1a2b2l2a3l3b3l4");
	CHECK(heavy_seen == 0);
}

// A child that turned heavy completes into its parent by symmetric transfer: the parent's light
// reference and the heavy scope are the same scheduler, so the parent continues in the scope.
void equality_ignores_lane() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	CHECK(task::scheduler_ref::of(s) == task::scheduler_ref::of(s).with_heavy(true));
	int turns = 0, child_turn = -1, parent_turn = -1;
	bool parent_heavy = false;
	auto child = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 1);
		co_await b.checkpoint();
		child_turn = turns;
	};
	auto parent = [&]() -> task::task<void> {
		co_await child();
		parent_turn = turns;
		parent_heavy = heavy_now();
	};
	auto counter = [&]() -> task::task<void> {
		for (int i = 0; i < 10; ++i) {
			++turns;
			co_await task::yield();
		}
	};
	task::spawn(task::scheduler_ref::of(s), parent());
	task::spawn(task::scheduler_ref::of(s), counter());
	io.run();
	CHECK(child_turn >= 0 && parent_turn == child_turn);
	CHECK(parent_heavy);
}

// rearm_light() makes the scope light: a task past light_slices that rearms and then awaits a
// completion from another thread resumes with the ordinary posts.
void rearm_light_resumes_light() {
	loop_thread l;
	parked ph, pl;
	std::string order;
	bool heavy_before = false, heavy_after_rearm = true, resumed_heavy = true;
	std::size_t demoted_at_light = 99;
	std::atomic<int> done{0};
	auto rearmed = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 2);
		co_await b.checkpoint();
		co_await b.checkpoint();  // the second spent slice: heavy
		heavy_before = heavy_now();
		b.rearm_light();
		heavy_after_rearm = heavy_now();
		co_await park_on{ph};
		resumed_heavy = heavy_now();
		order += 'R';
		++done;
	};
	auto light = [&]() -> task::task<void> {
		co_await park_on{pl};
		demoted_at_light = l.s.demoted();
		order += 'L';
		++done;
	};
	task::spawn(task::scheduler_ref::of(l.s), rearmed());
	task::spawn(task::scheduler_ref::of(l.s), light());
	ph.wait();
	pl.wait();
	ph.wake();
	pl.wake();
	while (done.load() < 2) { std::this_thread::yield(); }
	l.finish();
	CHECK(heavy_before && !heavy_after_rearm && !resumed_heavy);
	CHECK(order == "RL");
	CHECK(demoted_at_light == 0);
}

// F12. when_all continues its parent in the scope of the child that finishes last, as any
// completion does: after a heavy child, the parent is heavy. A rearm_light() in the parent
// before the next when_all starts every child of the next chunk light, while a sibling's
// rearm_light() reaches neither the child already running nor the next chunk.
void when_all_join_lane() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	constexpr unsigned light_slices = 4, allowance = 2;
	struct chunk {
		bool first_heavy = true;   // the apply child's lane after its first spent slice
		bool joined_heavy = false; // the parent's lane after the join
	};
	auto apply_chunk = [&](task::budget& b, chunk& c) -> task::task<void> {
		for (int i = 0; i < 4; ++i) {  // spends past the allowance and finishes last
			co_await b.checkpoint();
			if (i == 0) { c.first_heavy = heavy_now(); }
		}
	};
	auto next_read = [&](task::budget& b, bool rearm) -> task::task<void> {
		co_await task::yield();
		if (rearm) { b.rearm_light(allowance); }
	};
	chunk in_parent[3], in_sibling[3];
	auto stream = [&](chunk* chunks, bool parent_rearms) -> task::task<void> {
		task::budget b(spent_slice, {}, light_slices);
		b.rearm_light(allowance);
		for (int i = 0; i < 3; ++i) {
			if (parent_rearms) { b.rearm_light(allowance); }
			co_await task::when_all(apply_chunk(b, chunks[i]), next_read(b, !parent_rearms));
			chunks[i].joined_heavy = heavy_now();
		}
	};
	task::spawn(task::scheduler_ref::of(s), stream(in_parent, true));
	io.run();
	io.restart();
	task::spawn(task::scheduler_ref::of(s), stream(in_sibling, false));
	io.run();
	for (const chunk& c : in_parent) {
		CHECK(!c.first_heavy);
		CHECK(c.joined_heavy);
	}
	CHECK(!in_sibling[0].first_heavy);
	CHECK(in_sibling[1].first_heavy && in_sibling[2].first_heavy);
}

// The scope turns heavy at the light_slices-th spent slice and stays heavy; rearm_light(n)
// gives n more light slices, and rearm_light() all of them.
void turns_heavy_at_light_slices() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::string lanes;
	std::vector<unsigned> spent;
	auto t = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 3);
		auto step = [&]() -> task::task<void> {
			co_await b.checkpoint();
			lanes += heavy_now() ? 'H' : 'l';
			spent.push_back(b.spent());
		};
		for (int i = 0; i < 4; ++i) { co_await step(); }
		b.rearm_light(1);
		co_await step();
		b.rearm_light(0);
		co_await step();
		b.rearm_light();
		for (int i = 0; i < 3; ++i) { co_await step(); }
	};
	task::spawn(task::scheduler_ref::of(s), t());
	io.run();
	if (lanes != "llHHHHllH") { std::fprintf(stderr, "lanes %s\n", lanes.c_str()); }
	CHECK(lanes == "llHHHHllH");
	CHECK((spent == std::vector<unsigned>{1, 2, 3, 4, 3, 4, 1, 2, 3}));
}

// await_asio resumes inline only for a light reference: a heavy task's completion goes to the
// heavy lane even from a top-level handler on the loop's thread.
void await_asio_heavy_posts() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	int depth_light = -1, depth_heavy = -1;
	bool heavy_after = false;
	auto timer_wait = [&]() -> task::task<void> {
		asio::steady_timer t(io);
		t.expires_after(1ms);
		co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
	};
	auto light = [&]() -> task::task<void> {
		co_await timer_wait();
		depth_light = task::detail::inline_resume_depth;
	};
	auto heavy = [&]() -> task::task<void> {
		task::budget b(spent_slice, {}, 1);
		co_await b.checkpoint();
		co_await timer_wait();
		depth_heavy = task::detail::inline_resume_depth;
		heavy_after = heavy_now();
	};
	task::spawn(task::scheduler_ref::of(s), light());
	task::spawn(task::scheduler_ref::of(s), heavy());
	io.run();
	CHECK(depth_light == 1);  // resumed inside the timer's handler
	CHECK(depth_heavy == 0 && heavy_after);
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
// Over a strand (no io_context executor) the scheduler cannot hop, so a post_demoted from a
// task on another scheduler asserts.
void foreign_post_demoted() {
	asio::io_context io;
	task::asio_scheduler s(asio::make_strand(io));
	task::inline_scheduler other;
	task::run(other, [&]() -> task::task<void> {
		s.post_demoted(std::noop_coroutine());
		co_return;
	}());
}
void unsupported_hop_asserts() { CHECK(aborts(foreign_post_demoted)); }
#endif

struct test_case {
	const char* name;
	void (*run)();
};
constexpr test_case tests[] = {
#if defined(HAVE_FORK) && !defined(NDEBUG)
	{"unsupported_hop_asserts", unsupported_hop_asserts},  // first: it forks
#endif
	{"light_overtakes_heavy", light_overtakes_heavy},
	{"timer_within_two_heavy_turns", timer_within_two_heavy_turns},
	{"heavy_progresses_under_flood", heavy_progresses_under_flood},
	{"heavy_sticky_across_chore", heavy_sticky_across_chore},
	{"cross_thread_hop", cross_thread_hop},
	{"default_unchanged", default_unchanged},
	{"equality_ignores_lane", equality_ignores_lane},
	{"rearm_light_resumes_light", rearm_light_resumes_light},
	{"when_all_join_lane", when_all_join_lane},
	{"turns_heavy_at_light_slices", turns_heavy_at_light_slices},
	{"await_asio_heavy_posts", await_asio_heavy_posts},
};

}  // namespace

int main(int argc, char** argv) {
	bool ran = false;
	for (const test_case& t : tests) {
		if (argc > 1 && std::strcmp(argv[1], t.name) != 0) { continue; }
		t.run();
		ran = true;
	}
	if (!ran) {
		std::fprintf(stderr, "no test named %s\n", argv[1]);
		return 2;
	}
	std::printf("task_lanes: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
