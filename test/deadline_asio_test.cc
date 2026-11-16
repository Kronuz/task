// asio_scheduler's deadlines on its wheel:
//   offloop    deadlines created on the loop or off it and destroyed on another thread or on
//              the loop while it fires, 300 rounds: none requests after its destructor returns
//   teardown   the phased teardown with deadlines armed: one destroyed off the stopped loop
//              (its posted unfile dropped), the rest on teardown's thread after close; the
//              destroyed scheduler's wheel releases what it still holds
//   idle       a far deadline destroyed on the loop cancels the wake, so run() returns
//   direct     an entry armed through scheduler_ref::arm, with no deadline armed, wakes the
//              loop and fires on time; one cancelled through it never fires
//   far        an entry armed past the clock's range (the wheel's never, 2^63 ns + 1 ms) does
//              not spin the loop
//   loops      a parent deadline on loop A, linked children made and destroyed on loop B while
//              A fires it: each entry stays on its own loop's wheel, the link crosses threads
//   roots      teardown with roots holding deadlines: destroy_roots() destroys the frames on
//              teardown's thread, a group's entry is cancelled there after them, and the
//              destroyed scheduler's wheel releases the deadlines' states
//   offloop-arm   the control, not in `all`: an entry cancelled through scheduler_ref off a
//              running loop; the owner assertion aborts (Debug); it exits 0 only without one
//   closed-arm    the control, not in `all`: an entry armed on a closed scheduler off its
//              loop; the scheduler's assertion aborts (Debug); it exits 0 only without one
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "task_asio.hh"

#include <asio/executor_work_guard.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <string_view>
#include <thread>
#include <vector>

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

namespace {
using namespace std::chrono_literals;
using dl_t = task::deadline<task::asio_scheduler>;

template <class F>
void on(asio::io_context& io, F f) {
	std::atomic<bool> done{false};
	asio::post(io, [&] {
		f();
		done = true;
	});
	while (!done) { std::this_thread::yield(); }
}

void offloop() {
	constexpr int rounds = 300;
	asio::io_context io;
	auto guard = asio::make_work_guard(io);
	task::asio_scheduler s(io.get_executor());
	std::thread runner([&] { io.run(); });
	int requested = 0, created_off = 0, destroyed_off = 0;
	for (int i = 0; i < rounds; ++i) {
		const auto at = std::chrono::steady_clock::now() + std::chrono::microseconds(i % 4 * 400);
		std::unique_ptr<dl_t> d;
		if (i % 2) {
			d = std::make_unique<dl_t>(s, at);  // posts its filing
			++created_off;
		} else {
			on(io, [&] { d = std::make_unique<dl_t>(s, at); });
		}
		const auto token = d->token();
		std::this_thread::sleep_for(std::chrono::microseconds(i % 5 * 300));
		if (i % 3 == 2) {
			on(io, [&] { d.reset(); });
		} else {
			std::thread([&] { d.reset(); }).join();  // posts its unfile
			++destroyed_off;
		}
		const bool after_destroy = token.requested();
		std::this_thread::sleep_for(3ms);  // a wake due by now runs
		CHECK(token.requested() == after_destroy);
		requested += after_destroy;
	}
	on(io, [&] { CHECK(s.armed_timers() == 0); });
	guard.reset();
	runner.join();
	CHECK(requested > 0 && requested < rounds);  // both sides of the race were reached
	std::printf("offloop rounds=%d created_off_loop=%d destroyed_off_loop=%d requested_before_destroy=%d ok\n", rounds, created_off,
		destroyed_off, requested);
}

void teardown() {
	for (int i = 0; i < 100; ++i) {
		auto io = std::make_unique<asio::io_context>();
		auto guard = asio::make_work_guard(*io);
		auto s = std::make_unique<task::asio_scheduler>(io->get_executor());
		std::thread runner([&] { io->run(); });
		const auto far = std::chrono::steady_clock::now() + 60s;
		std::unique_ptr<dl_t> on_loop, posted, foreign;
		on(*io, [&] { on_loop = std::make_unique<dl_t>(*s, far); });
		posted = std::make_unique<dl_t>(*s, far);
		foreign = std::make_unique<dl_t>(*s, far + 1ms);
		on(*io, [&] { CHECK(s->armed_timers() == 3); });
		const auto t1 = on_loop->token(), t2 = posted->token(), t3 = foreign->token();
		io->stop();  // 1: the loop is joined
		runner.join();
		foreign.reset();  // posts its unfile to the stopped loop
		s->close();       // 2: releases the wheel
		on_loop.reset();  // 3: teardown's thread destroys the frames' deadlines
		posted.reset();
		s->destroy_roots();
		s.reset();  // 4: the wheel drops the three entries
		guard.reset();
		io.reset();  // and the unfile it never ran
		CHECK(!t1.requested() && !t2.requested() && !t3.requested());
	}
	std::printf("teardown rounds=100 ok\n");
}

void idle() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	std::unique_ptr<dl_t> d;
	asio::post(io, [&] { d = std::make_unique<dl_t>(s, std::chrono::steady_clock::now() + 30s); });
	asio::post(io, [&] { d.reset(); });
	const auto start = std::chrono::steady_clock::now();
	io.run();
	CHECK(std::chrono::steady_clock::now() - start < 5s);
	std::printf("idle ok\n");
}

std::atomic<int> direct_fired{0};
std::atomic<std::uint64_t> direct_at{0}, direct_fired_at{0};

void direct() {
	asio::io_context io;
	auto guard = asio::make_work_guard(io);
	task::asio_scheduler s(io.get_executor());
	std::thread runner([&] { io.run(); });
	wheel::timer_entry e, cancelled;
	e.fire = [](wheel::timer_entry&, bool expired) noexcept {
		if (expired) {
			direct_fired_at = std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
			++direct_fired;
		}
	};
	cancelled.fire = e.fire;
	on(io, [&] {
		const auto r = task::scheduler_ref::of(s);
		direct_at = r.now() + 20'000'000;
		CHECK(r.arm(e, direct_at) && r.arm(cancelled, direct_at));
		CHECK(r.cancel(cancelled) && s.armed_timers() == 1);
	});
	const auto start = std::chrono::steady_clock::now();
	while (!direct_fired && std::chrono::steady_clock::now() - start < 2s) { std::this_thread::sleep_for(1ms); }
	CHECK(direct_fired == 1);
	CHECK(direct_fired_at >= direct_at);
	const auto late_us = (direct_fired_at - direct_at) / 1000;
	CHECK(late_us < 200'000);  // one tick plus the loop's wake: about 1 to 2 ms
	std::this_thread::sleep_for(30ms);
	CHECK(direct_fired == 1);
	on(io, [&] { CHECK(s.armed_timers() == 0 && !e.armed() && !cancelled.armed()); });
	guard.reset();
	runner.join();
	std::printf("direct late_us=%llu ok\n", static_cast<unsigned long long>(late_us));
}

void far() {
	for (const std::uint64_t at : {wheel::timer_wheel::never, (std::uint64_t(1) << 63) + 1'000'000}) {
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		wheel::timer_entry e;
		e.fire = [](wheel::timer_entry&, bool) noexcept {};
		asio::post(io, [&] { CHECK(task::scheduler_ref::of(s).arm(e, at)); });
		const std::size_t handlers = io.run_for(100ms);
		CHECK(handlers < 10);  // a wake past the clock's range would spin the loop
		asio::post(io, [&] { CHECK(task::scheduler_ref::of(s).cancel(e)); });
		io.restart();
		io.run_for(10ms);
		std::printf("far at=%llu handlers=%zu ok\n", static_cast<unsigned long long>(at), handlers);
	}
}
void loops() {
	constexpr int iterations = 300, children = 8;
	asio::io_context io_a, io_b;
	auto guard_a = asio::make_work_guard(io_a);
	auto guard_b = asio::make_work_guard(io_b);
	task::asio_scheduler a(io_a.get_executor()), b(io_b.get_executor());
	std::thread run_a([&] { io_a.run(); }), run_b([&] { io_b.run(); });
	std::atomic<int> callbacks{0};
	int kept = 0;
	std::mt19937_64 random(7);
	struct child {
		dl_t deadline;
		task::cancel_callback<std::function<void()>> callback;
		child(task::asio_scheduler& s, const task::cancel_token& parent, std::atomic<int>& count)
			: deadline(s, std::chrono::steady_clock::now() + 60s, parent), callback(deadline.token(), std::function<void()>([&count] { ++count; })) {}
	};
	for (int i = 0; i < iterations; ++i) {
		const auto delay = std::chrono::microseconds(random() % 1500);
		const unsigned spin = unsigned(random() % 2000), keep = unsigned(random() % (children + 1));
		std::unique_ptr<dl_t> parent;
		task::cancel_token parent_token;
		on(io_a, [&] {
			parent = std::make_unique<dl_t>(a, std::chrono::steady_clock::now() + delay);
			parent_token = parent->token();
		});
		std::atomic<bool> finished{false};
		asio::post(io_b, [&, spin, keep] {
			std::vector<std::unique_ptr<child>> mine;
			for (int c = 0; c < children; ++c) { mine.push_back(std::make_unique<child>(b, parent_token, callbacks)); }
			for (unsigned k = 0; k < spin; ++k) { std::atomic_signal_fence(std::memory_order_seq_cst); }
			mine.resize(keep);  // some die while A may be firing the parent, the rest after it fired
			while (!parent_token.requested()) { std::this_thread::yield(); }
			on(io_a, [] {});  // the fire's walk over the parent's children has ended
			mine.clear();
			finished = true;
		});
		while (!finished) { std::this_thread::yield(); }
		CHECK(parent->expired());
		on(io_a, [&] { parent.reset(); });
		kept += int(keep);
	}
	on(io_a, [&] { CHECK(a.armed_timers() == 0); });
	on(io_b, [&] { CHECK(b.armed_timers() == 0); });
	guard_a.reset();
	guard_b.reset();
	run_a.join();
	run_b.join();
	CHECK(callbacks >= kept && callbacks <= iterations * children);
	std::printf("loops iterations=%d children=%d kept=%d child_callbacks=%d ok\n", iterations, children, kept, callbacks.load());
}

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

// A root's frame holding a deadline on its loop's wheel while it waits for its token.
task::task<int> hold_deadline(task::asio_scheduler& s, std::chrono::steady_clock::time_point at, task::cancel_token& out) {
	dl_t d(s, at);
	out = d.token();
	co_return co_await wait_for_cancel(d.token());
}

void roots() {
	constexpr int rounds = 100, frames = 3;
	for (int i = 0; i < rounds; ++i) {
		auto io = std::make_unique<asio::io_context>();
		auto guard = asio::make_work_guard(*io);
		auto s = std::make_unique<task::asio_scheduler>(io->get_executor());
		const auto r = task::scheduler_ref::of(*s);
		std::thread runner([&] { io->run(); });
		const auto far = std::chrono::steady_clock::now() + 60s;
		std::vector<task::cancel_token> tokens(frames);
		wheel::timer_entry group;  // a group's timer, as a driver arms it
		group.fire = [](wheel::timer_entry&, bool) noexcept {};
		for (int k = 0; k < frames; ++k) {  // spawned off the loop; each frame makes its deadline on it
			task::spawn(r, hold_deadline(*s, far + k * 1ms, tokens[k]), [](std::exception_ptr, std::optional<int>) { CHECK(!"a root ran to its end"); });
		}
		on(*io, [&] { CHECK(r.arm(group, s->now() + 60'000'000'000ull)); });
		on(*io, [&] { CHECK(s->armed_timers() == frames + 1); });
		io->stop();  // 1: the loop is joined
		runner.join();
		s->close();          // 2: releases the wheel
		s->destroy_roots();  // 3: the frames' deadlines die on this thread, their entries left filed
		CHECK(s->armed_timers() == frames + 1);
		CHECK(r.cancel(group) && s->armed_timers() == frames);  // the group's entry, after the frames
		s.reset();  // 4: the wheel drops the deadlines' states
		guard.reset();
		io.reset();
		for (const auto& t : tokens) { CHECK(!t.requested()); }
	}
	std::printf("roots rounds=%d frames=%d ok\n", rounds, frames);
}

int offloop_arm() {
	asio::io_context io;
	auto guard = asio::make_work_guard(io);
	task::asio_scheduler s(io.get_executor());
	std::thread runner([&] { io.run(); });
	wheel::timer_entry e;
	e.fire = [](wheel::timer_entry&, bool) noexcept {};
	on(io, [&] { CHECK(task::scheduler_ref::of(s).arm(e, s.now() + 60'000'000'000ull)); });
	std::printf("offloop-arm: cancelling off the running loop (expected: the owner assertion aborts)\n");
	std::fflush(stdout);
	task::scheduler_ref::of(s).cancel(e);
	std::printf("offloop-arm: no assertion\n");
	guard.reset();
	runner.join();
	return 0;
}

int closed_arm() {
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	s.close();
	wheel::timer_entry e;
	e.fire = [](wheel::timer_entry&, bool) noexcept {};
	std::printf("closed-arm: arming on a closed scheduler (expected: the assertion aborts)\n");
	std::fflush(stdout);
	task::scheduler_ref::of(s).arm(e, s.now() + 60'000'000'000ull);
	std::printf("closed-arm: no assertion\n");
	task::scheduler_ref::of(s).cancel(e);
	return 0;
}
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	if (only == "offloop-arm") { return offloop_arm(); }
	if (only == "closed-arm") { return closed_arm(); }
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "offloop") { offloop(); }
	if (only == "all" || only == "teardown") { teardown(); }
	if (only == "all" || only == "idle") { idle(); }
	if (only == "all" || only == "direct") { direct(); }
	if (only == "all" || only == "far") { far(); }
	if (only == "all" || only == "loops") { loops(); }
	if (only == "all" || only == "roots") { roots(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("deadline_asio_test: ok\n");
	return 0;
}
