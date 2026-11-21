/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
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

// Time: deadline, sleep_until and sleep_for, and the budget that bounds a time slice.

#pragma once

#include "cancel.hh"

namespace task {

namespace detail {

// The clock a scheduler's deadlines are on: S::clock when S declares one, else steady_clock.
template <typename S, typename = void>
struct clock_of {
	using type = std::chrono::steady_clock;
};
template <typename S>
struct clock_of<S, std::void_t<typename S::clock>> {
	using type = typename S::clock;
};

// A reference to a deadline's state, called once at the deadline: it requests the deadline
// unless disarmed. A scheduler with a wheel take()s the reference for the entry it arms (the
// state is then a timed_cancel_state).
struct deadline_fire {
	cancel_state* s;
	explicit deadline_fire(cancel_state* st) noexcept : s(st) { s->retain(); }
	deadline_fire(deadline_fire&& o) noexcept : s(std::exchange(o.s, nullptr)) {}
	deadline_fire(const deadline_fire&) = delete;
	deadline_fire& operator=(const deadline_fire&) = delete;
	~deadline_fire() {
		if (s) { s->release(); }
	}
	void operator()() noexcept { s->request(cancel_reason::deadline, cancel_state::disarmed_bit); }
	timed_cancel_state* timed() const noexcept { return static_cast<timed_cancel_state*>(s); }
	timed_cancel_state* take() noexcept { return static_cast<timed_cancel_state*>(std::exchange(s, nullptr)); }
};

// What start_timer returns on a scheduler with a wheel: destroying it hands the deadline's
// entry to the scheduler's static drop_timer, which unfiles it on the wheel's thread. On
// inline_scheduler and blocking_scheduler it touches the scheduler only while the entry is
// armed, so a deadline may outlive them (their destruction fires every entry off the wheel).
template <typename S>
class wheel_timer {
public:
	wheel_timer(S& s, timed_cancel_state* st) noexcept : s_(&s), st_(st) {}
	wheel_timer(const wheel_timer&) = delete;
	wheel_timer& operator=(const wheel_timer&) = delete;
	~wheel_timer() { S::drop_timer(s_, st_); }

private:
	S* s_;
	timed_cancel_state* st_;  // the deadline's source holds it
};

}  // namespace detail

// A cancel source that a timer requests with cancel_reason::deadline at `at`, linked to
// `parent`; `at` is on S::clock when S declares one, else on steady_clock. S provides
// start_timer(time_point, F), which runs F once at the deadline; on a scheduler with a wheel
// (inline_scheduler, blocking_scheduler, asio_scheduler) the deadline's state carries the entry, so arming
// allocates nothing more; elsewhere it carries none. The timer's callback holds the state and
// requests only while the deadline is armed, so an expiry already queued when the deadline is
// destroyed requests nothing.
template <typename S>
class deadline {
	using time_point = typename detail::clock_of<S>::type::time_point;
	using timer_type = decltype(std::declval<S&>().start_timer(time_point{}, std::declval<detail::deadline_fire>()));
	static constexpr bool on_wheel = std::is_same_v<timer_type, detail::wheel_timer<S>>;

public:
	deadline(S& s, time_point at, const cancel_token& parent = {})
		: source_(parent, cancel_source::timed_tag{on_wheel}), timer_(s.start_timer(at, detail::deadline_fire(source_.s_))) {}
	deadline(const deadline&) = delete;
	deadline& operator=(const deadline&) = delete;
	// Any thread: the disarm bit is set before the timer dies, and the timer's callback holds the state.
	~deadline() { source_.s_->disarm(); }

	cancel_token token() const noexcept { return source_.token(); }
	bool expired() const noexcept { return source_.reason() == cancel_reason::deadline; }

private:
	cancel_source source_;
	timer_type timer_;
};

template <typename F>
detail::wheel_timer<inline_scheduler> inline_scheduler::start_timer(clock::time_point at, F&& on_fire) {
	static_assert(std::is_same_v<std::remove_cvref_t<F>, detail::deadline_fire>, "a wheel's start_timer files task::deadline's entry");
	detail::timed_cancel_state* s = on_fire.take();  // the wheel's reference
	arm(*s, std::uint64_t(std::max<std::int64_t>(0, at.time_since_epoch().count())));
	return {*this, s};
}

inline void inline_scheduler::drop_timer(inline_scheduler* self, detail::timed_cancel_state* s) noexcept {
	if (!s->armed()) { return; }
	self->wheel_->cancel(*s);
	s->release();
}

template <typename F>
detail::wheel_timer<blocking_scheduler> blocking_scheduler::start_timer(clock::time_point at, F&& on_fire) {
	static_assert(std::is_same_v<std::remove_cvref_t<F>, detail::deadline_fire>, "a wheel's start_timer files task::deadline's entry");
	detail::timed_cancel_state* s = on_fire.take();  // the wheel's reference
	arm(*s, std::uint64_t(std::max<clock::rep>(0, std::chrono::duration_cast<std::chrono::nanoseconds>(at.time_since_epoch()).count())));
	return {*this, s};
}

inline void blocking_scheduler::drop_timer(blocking_scheduler* self, detail::timed_cancel_state* s) noexcept {
	if (!s->armed()) { return; }
	self->cancel(*s);
	s->release();
}

namespace detail {

// A sleep's wait: a wheel entry in the waiting frame, and a cancel callback on its token. One
// compare-exchange settles it fired or cancelled; the fire resumes through a post, the cancel
// hops to the scheduler through post_call, unfiles the entry there and posts the resume. A fire
// that loses to a cancel settles the token, as a deadline's does, so the hop is queued by then.
class sleep_awaiter : wheel::timer_entry {
public:
	static constexpr std::uint64_t never = wheel::timer_wheel::never;

	sleep_awaiter(std::uint64_t at, cancel_token token) noexcept : at_(at), token_(std::move(token)) { fire = &on_fire; }
	sleep_awaiter(const sleep_awaiter&) = delete;
	sleep_awaiter& operator=(const sleep_awaiter&) = delete;
	// A frame destroyed while sleeping (teardown): deregister, then unfile.
	~sleep_awaiter() {
		callback_.reset();
		if (armed()) { sched_.cancel(*this); }
	}

	bool await_ready() noexcept {
		if (token_.requested()) { return true; }
		if (at_ != never && current() && at_ <= current().now()) {
			state_.store(fired, std::memory_order_relaxed);
			return true;
		}
		return false;
	}
	void await_suspend(std::coroutine_handle<> h) {
		handle_ = h;
		sched_ = current();
		if (!sched_) { throw std::logic_error("task::sleep_until off a scheduler"); }
		assert((at_ != never || !token_empty(token_)) && "a sleep with no time and no token never ends");
		if (at_ != never && !sched_.arm(*this, at_)) { throw std::logic_error("task::sleep_until on a scheduler without timers"); }
		callback_.emplace(token_, on_cancel{this});  // a request now only queues the hop
	}
	// True at the time; false once the token was requested first.
	bool await_resume() const noexcept { return state_.load(std::memory_order_acquire) == fired; }

private:
	enum : unsigned char { waiting, fired, cancelled, dead };
	struct on_cancel {
		sleep_awaiter* self;
		void operator()() noexcept {
			unsigned char w = waiting;
			if (self->state_.compare_exchange_strong(w, cancelled, std::memory_order_acq_rel)) {
				TASK_TEST_HOOK(sleep_claimed);
				[[maybe_unused]] const bool posted = self->sched_.post_call(&hop, self);
				assert(posted && "a cancellable sleep needs a scheduler with post_call");
			}
		}
	};
	// On the scheduler: the entry is unfiled on its own thread, then the frame resumes.
	static void hop(void* p) {
		auto* self = static_cast<sleep_awaiter*>(p);
		if (self->armed()) { self->sched_.cancel(*self); }
		self->sched_.post(self->handle_);
	}
	static void on_fire(wheel::timer_entry& e, bool expired) noexcept {
		auto* self = static_cast<sleep_awaiter*>(&e);
		unsigned char w = waiting;
		// A dying wheel's fire leaves the frame suspended for teardown to destroy.
		if (self->state_.compare_exchange_strong(w, expired ? fired : dead, std::memory_order_acq_rel)) {
			if (expired) { self->sched_.post(self->handle_); }
		} else if (w == cancelled) {
			token_settle(self->token_);  // the cancel's hop is queued before expire() returns
		}
	}

	std::uint64_t at_;
	cancel_token token_;
	std::coroutine_handle<> handle_;
	scheduler_ref sched_;
	std::atomic<unsigned char> state_{waiting};
	std::optional<cancel_callback<on_cancel>> callback_;
};

}  // namespace detail

// On a scheduler with timers: true at `at` (nanoseconds, as now()), false once `token` is
// requested first. at = never waits for the token alone. Each sleep is a wheel entry in the
// waiting frame, so it allocates nothing.
[[nodiscard]] inline detail::sleep_awaiter sleep_until(std::uint64_t at, cancel_token token = {}) noexcept {
	return detail::sleep_awaiter(at, std::move(token));
}
// As sleep_until(now() + d), on the current scheduler's clock.
[[nodiscard]] inline detail::sleep_awaiter sleep_for(std::chrono::nanoseconds d, cancel_token token = {}) noexcept {
	const std::uint64_t now = current() ? current().now() : 0;
	return detail::sleep_awaiter(now + std::uint64_t(std::max<std::int64_t>(0, d.count())), std::move(token));
}
inline constexpr std::uint64_t never = wheel::timer_wheel::never;

// A time slice. checkpoint() is free while the slice lasts; once it is spent, it
// requeues the coroutine as demoted. Both throw cancelled when the token is set.
// On a scheduler the slice is the current resume: time spent suspended does not count,
// and a task resumed after an event, a lock or a completion starts a fresh slice. Off a
// scheduler it runs from construction, or from the last demotion.
//
// With light_slices N > 0 the budget classes its scope by the slices it spent (docs/lanes.md):
// it runs N slices light, each spent one requeued at the tail of the ordinary posts; the N-th
// spent slice, and any spent in a heavy scope, enter the heavy lane, where every resumption of
// what the scope captures then goes. light_slices 0 demotes every spent slice, as without lanes,
// and never makes a scope heavy.
class budget {
public:
	explicit budget(std::chrono::nanoseconds slice, cancel_token token = {}, unsigned light_slices = 0)
		: slice_(std::uint64_t(double(slice.count()) * cycles::per_ns())), deadline_(cycles::read() + slice_), token_(std::move(token)),
		  light_(light_slices) {}

	bool exhausted() const noexcept {
		return cycles::read() >= (current() ? detail::slice_start + slice_ : deadline_);
	}
	const cancel_token& token() const noexcept { return token_; }
	// Slices spent: the checkpoints that found the slice exhausted on a scheduler.
	unsigned spent() const noexcept { return spent_; }
	// Marks the scope light and lets it run `allowance` more slices light (light_slices by
	// default): the allowance-th spent slice enters the heavy lane. Only what the scope starts or
	// captures after the call is light again.
	void rearm_light(unsigned allowance = ~0u) noexcept {
		spent_ = allowance < light_ ? light_ - allowance : 0;
		enter_light();
	}

	auto checkpoint() noexcept {
		struct awaiter {
			budget& b;
			bool await_ready() const noexcept { return b.token_.requested() || !b.exhausted() || !current(); }
			void await_suspend(std::coroutine_handle<> h) const {
				const scheduler_ref s = current();
				++b.spent_;
				if (b.light_ == 0) {
					s.post_demoted(h);
				} else if (b.spent_ >= b.light_ || s.heavy()) {
					s.with_heavy(true).post(h);
				} else {
					s.post(h);
				}
			}
			void await_resume() const {
				if (b.token_.requested()) { throw cancelled{}; }
				if (b.exhausted()) { b.deadline_ = cycles::read() + b.slice_; }
			}
		};
		return awaiter{*this};
	}

private:
	std::uint64_t slice_;
	std::uint64_t deadline_;
	cancel_token token_;
	unsigned light_;
	unsigned spent_ = 0;
};

}  // namespace task
