// asio_scheduler's deadlines on its wheel:
//   offloop    deadlines created on the loop or off it and destroyed on another thread or on
//              the loop while it fires, 300 rounds: none requests after its destructor returns
//   teardown   the phased teardown with deadlines armed: one destroyed off the stopped loop
//              (its posted unfile dropped), the rest on teardown's thread after close; the
//              destroyed scheduler's wheel releases what it still holds
//   idle       a far deadline destroyed on the loop cancels the wake, so run() returns
//   direct     an entry armed through scheduler_ref::arm, with no deadline armed, wakes the
//              loop and fires on time; one cancelled through it never fires
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "task_asio.hh"

#include <asio/executor_work_guard.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
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
}  // namespace

int main() {
	const auto baseline = task::detail::live_frames();
	offloop();
	teardown();
	idle();
	direct();
	CHECK(task::detail::live_frames() == baseline);
	std::printf("deadline_asio_test: ok\n");
	return 0;
}
