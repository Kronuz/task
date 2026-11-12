// task::deadline on inline_scheduler's wheel, and timers armed through scheduler_ref
// (Kronuz/wheel's own tests cover the wheel):
//   manual    deadline<inline_scheduler> on its manual clock, advance() fires it, no real time
//   chain     a fired parent deadline cancels a linked child whose callback destroys it
//   ref       scheduler_ref::arm and cancel on inline_scheduler; false without timers
// Every frame and cancel state is freed at the end (TASK_COUNT_FRAMES).
#define TASK_COUNT_FRAMES 1
#include "task.hh"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string_view>

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
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	const auto baseline = task::detail::live_frames();
	if (only == "all" || only == "manual") { manual(); }
	if (only == "all" || only == "chain") { chain(); }
	if (only == "all" || only == "ref") { ref(); }
	CHECK(task::detail::live_frames() == baseline);
	std::printf("deadline_test: ok\n");
	return 0;
}
