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

// Yielding and cancellation: yield, schedule_on, cancel_state, cancel_token, cancel_source
// and cancel_callback.

#pragma once

#include "coroutine.hh"

namespace task {

// ---------------------------------------------------------------------------------------------
// Yielding, budgets, and cancellation

// Suspends and requeues the current coroutine on the current scheduler.
struct yield_awaiter {
	bool demote = false;
	bool await_ready() const noexcept { return !current(); }
	void await_suspend(std::coroutine_handle<> h) const {
		demote ? current().post_demoted(h) : current().post(h);
	}
	void await_resume() const noexcept {}
};
inline yield_awaiter yield() noexcept { return {false}; }

// Moves the current coroutine to scheduler s (it resumes from s's queue).
struct schedule_on {
	scheduler_ref target;
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) const { target.post(h); }
	void await_resume() const noexcept {}
};

struct cancelled : std::exception {
	const char* what() const noexcept override { return "task cancelled"; }
};

// Why a token was requested. Libraries define their own reasons from `user` up.
enum class cancel_reason : std::uint32_t { none = 0, requested = 1, deadline = 2, shutdown = 3, user = 64 };

namespace detail {

struct linked_cancel_state;

// A registered callback; request() unlinks one at a time and runs it with no lock held.
struct cancel_node {
	cancel_node* prev = nullptr;
	cancel_node* next = nullptr;
	void (*run)(cancel_node*, cancel_reason) noexcept = nullptr;
};

// The state a source and its tokens share: a pooled, counted block. One word holds the
// requested bit, a spin-lock bit, a deadline's disarmed bit and the reason, so polling is one
// load and requested() never disagrees with reason(). Callbacks follow std::stop_callback.
class cancel_state : public pooled_frame {
public:
	static constexpr std::uint64_t requested_bit = 1, locked_bit = 2, disarmed_bit = 4, linked_bit = 8, timed_bit = 16;
	static constexpr std::uint64_t delivering_bit = 32;  // a request is running its callbacks

	static cancel_state* make(cancel_state* parent);
	static timed_cancel_state* make_timed(cancel_state* parent);

	bool requested() const noexcept { return word_.load(std::memory_order_acquire) & requested_bit; }
	cancel_reason reason() const noexcept { return cancel_reason(word_.load(std::memory_order_acquire) >> 32); }
	void disarm() noexcept { word_.fetch_or(disarmed_bit, std::memory_order_release); }
	bool disarmed() const noexcept { return word_.load(std::memory_order_acquire) & disarmed_bit; }

	void retain() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
	void release() noexcept {
		if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) { destroy(); }
	}

	// Sets the reason and runs every callback, unless already requested or a `forbid` bit is
	// set; true if this call did.
	bool request(cancel_reason r, std::uint64_t forbid = 0) noexcept {
		std::uint64_t w = word_.load(std::memory_order_relaxed);
		for (;;) {
			if (w & (requested_bit | forbid)) { return false; }
			if (w & locked_bit) {
				std::this_thread::yield();
				w = word_.load(std::memory_order_relaxed);
				continue;
			}
			const std::uint64_t next = (w & 0xffffffffu) | requested_bit | locked_bit | (std::uint64_t(r) << 32);
			if (word_.compare_exchange_weak(w, next, std::memory_order_acq_rel, std::memory_order_relaxed)) { break; }
		}
		if (!head_) {
			unlock();
			return true;
		}
		retain();  // a callback may drop the last outside reference
		requester_ = std::this_thread::get_id();
		word_.fetch_or(delivering_bit, std::memory_order_relaxed);  // under the lock taken with requested_bit
		while (cancel_node* n = head_) {
			head_ = n->next;
			if (head_) { head_->prev = nullptr; }
			n->next = nullptr;
			running_ = n;
			unlock();
			n->run(n, r);  // n may be gone when it returns
			lock();
			running_ = nullptr;
			epoch_.fetch_add(1, std::memory_order_relaxed);
			unlock();
			epoch_.notify_all();
			lock();
		}
		word_.fetch_and(~(locked_bit | delivering_bit), std::memory_order_release);
		epoch_.fetch_add(1, std::memory_order_release);
		epoch_.notify_all();
		release();
		return true;
	}

	// Waits until a request running its callbacks on another thread has run them all, so what
	// they post is queued when this returns. Callbacks never block, so the wait is short.
	void settle() noexcept {
		for (;;) {
			const std::uint32_t e = epoch_.load(std::memory_order_acquire);  // before the check
			lock();
			const bool busy = (word_.load(std::memory_order_relaxed) & delivering_bit) && requester_ != std::this_thread::get_id();
			unlock();
			if (!busy) { return; }
			epoch_.wait(e, std::memory_order_relaxed);
		}
	}

	// Links n, or returns false when already requested (the caller then runs it).
	bool add(cancel_node* n) noexcept {
		std::uint64_t w = word_.load(std::memory_order_acquire);
		for (;;) {
			if (w & requested_bit) { return false; }
			if (w & locked_bit) {
				std::this_thread::yield();
				w = word_.load(std::memory_order_acquire);
				continue;
			}
			if (word_.compare_exchange_weak(w, w | locked_bit, std::memory_order_acquire, std::memory_order_acquire)) { break; }
		}
		n->prev = nullptr;
		n->next = head_;
		if (head_) { head_->prev = n; }
		head_ = n;
		unlock();
		return true;
	}

	// Unlinks n. If n is running on another thread, waits until it returns; if it is running
	// on this one (it is removing itself), returns at once and request() never touches it again.
	void remove(cancel_node* n) noexcept {
		lock();
		if (n->prev || head_ == n) {
			(n->prev ? n->prev->next : head_) = n->next;
			if (n->next) { n->next->prev = n->prev; }
			unlock();
			return;
		}
		const bool wait = running_ == n && requester_ != std::this_thread::get_id();
		unlock();
		while (wait) {
			const std::uint32_t e = epoch_.load(std::memory_order_acquire);  // before the check
			lock();
			const bool busy = running_ == n;
			unlock();
			if (!busy) { return; }
			epoch_.wait(e, std::memory_order_relaxed);
		}
	}

private:
	void lock() noexcept {
		std::uint64_t w = word_.load(std::memory_order_relaxed);
		for (;;) {
			if (w & locked_bit) {
				std::this_thread::yield();
				w = word_.load(std::memory_order_relaxed);
			} else if (word_.compare_exchange_weak(w, w | locked_bit, std::memory_order_acquire, std::memory_order_relaxed)) {
				return;
			}
		}
	}
	void unlock() noexcept { word_.fetch_and(~locked_bit, std::memory_order_release); }
	void destroy() noexcept;
	static void link(linked_cancel_state* s, cancel_state* parent) noexcept;

	std::atomic<std::uint64_t> word_{0};
	std::atomic<std::uint32_t> refs_{1};
	std::atomic<std::uint32_t> epoch_{0};  // bumped after each callback returns; remove() waits on it
	cancel_node* head_ = nullptr;
	cancel_node* running_ = nullptr;
	std::thread::id requester_;
	cancel_state* parent_ = nullptr;  // a linked state's, while registered on it
};

// A linked source's state: its node on the parent requests it with the parent's reason.
struct linked_cancel_state : cancel_state, cancel_node {};

// A deadline's state: a linked state (with or without a parent) and the entry a scheduler's
// wheel files. The wheel holds a reference while the entry is armed; the fire requests the
// deadline unless it is disarmed, and releases that reference. A fire that loses to a request
// still running callbacks on another thread waits for them, so a driver that finds its queue
// empty after expiring sees the hop a cross-thread cancel posts.
struct timed_cancel_state final : wheel::timer_entry, linked_cancel_state {
	timed_cancel_state() noexcept { fire = &on_wheel; }
	static void on_wheel(wheel::timer_entry& e, bool expired) noexcept {
		auto& s = static_cast<timed_cancel_state&>(e);
		if (expired && !s.request(cancel_reason::deadline, disarmed_bit)) { s.settle(); }
		s.release();
	}
};

inline void cancel_state::link(linked_cancel_state* s, cancel_state* parent) noexcept {
	TASK_TEST_HOOK(before_link);
	s->run = [](cancel_node* n, cancel_reason r) noexcept { static_cast<linked_cancel_state*>(n)->request(r); };
	if (parent->add(s)) {
		parent->retain();
		s->parent_ = parent;
	} else {
		s->request(parent->reason());  // requested meanwhile
	}
}

inline cancel_state* cancel_state::make(cancel_state* parent) {
	if (!parent) { return new cancel_state; }
	if (parent->requested()) {  // born requested, nothing to link
		auto* s = new cancel_state;
		s->word_.store(requested_bit | (std::uint64_t(parent->reason()) << 32), std::memory_order_relaxed);
		return s;
	}
	auto* s = new linked_cancel_state;
	s->word_.store(linked_bit, std::memory_order_relaxed);
	link(s, parent);
	return s;
}

inline timed_cancel_state* cancel_state::make_timed(cancel_state* parent) {
	auto* s = new timed_cancel_state;
	s->word_.store(linked_bit | timed_bit, std::memory_order_relaxed);
	if (parent) { link(s, parent); }
	return s;
}

inline void cancel_state::destroy() noexcept {
	const std::uint64_t w = word_.load(std::memory_order_relaxed);
	if (w & linked_bit) {
		auto* self = static_cast<linked_cancel_state*>(this);
		if (cancel_state* p = parent_) {
			p->remove(self);
			p->release();
		}
		if (w & timed_bit) {
			delete static_cast<timed_cancel_state*>(self);
		} else {
			delete self;
		}
	} else {
		delete this;
	}
}

}  // namespace detail

template <typename F>
class cancel_callback;
class cancel_token;

namespace detail {
bool token_empty(const cancel_token& t) noexcept;
void token_settle(const cancel_token& t) noexcept;
}

// A view of a source's state: polling is one acquire load, and an empty token (the default)
// is never requested and allocates nothing.
class cancel_token {
public:
	cancel_token() = default;
	cancel_token(const cancel_token& o) noexcept : s_(o.s_) {
		if (s_) { s_->retain(); }
	}
	cancel_token(cancel_token&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}
	cancel_token& operator=(cancel_token o) noexcept {
		std::swap(s_, o.s_);
		return *this;
	}
	~cancel_token() {
		if (s_) { s_->release(); }
	}

	bool requested() const noexcept { return s_ && s_->requested(); }
	cancel_reason reason() const noexcept { return s_ ? s_->reason() : cancel_reason::none; }

private:
	friend class cancel_source;
	template <typename F>
	friend class cancel_callback;
	friend bool detail::token_empty(const cancel_token& t) noexcept;
	friend void detail::token_settle(const cancel_token& t) noexcept;
	explicit cancel_token(detail::cancel_state* s) noexcept : s_(s) {
		if (s_) { s_->retain(); }
	}
	detail::cancel_state* s_ = nullptr;
};

// True for a token no source can request (the default one).
inline bool detail::token_empty(const cancel_token& t) noexcept { return t.s_ == nullptr; }
// Waits until a request delivering the token's callbacks on another thread has run them all.
inline void detail::token_settle(const cancel_token& t) noexcept {
	if (t.s_) { t.s_->settle(); }
}

// Requests cancellation; copies share the state. A source built from a parent token is
// requested, with the parent's reason, when the parent is.
class cancel_source {
public:
	cancel_source() : s_(detail::cancel_state::make(nullptr)) {}
	explicit cancel_source(const cancel_token& parent) : s_(detail::cancel_state::make(parent.s_)) {}
	cancel_source(const cancel_source& o) noexcept : s_(o.s_) {
		if (s_) { s_->retain(); }
	}
	cancel_source(cancel_source&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}
	cancel_source& operator=(cancel_source o) noexcept {
		std::swap(s_, o.s_);
		return *this;
	}
	~cancel_source() {
		if (s_) { s_->release(); }
	}

	cancel_token token() const noexcept { return cancel_token(s_); }
	// The first request wins and sets the reason (none counts as requested); true if this call did.
	bool request(cancel_reason r = cancel_reason::requested) noexcept {
		return s_ && s_->request(r == cancel_reason::none ? cancel_reason::requested : r);
	}
	bool requested() const noexcept { return s_ && s_->requested(); }
	cancel_reason reason() const noexcept { return s_ ? s_->reason() : cancel_reason::none; }

private:
	template <typename S>
	friend class deadline;
	struct timed_tag {
		bool entry;  // a wheel files the state's entry
	};
	cancel_source(const cancel_token& parent, timed_tag t)
		: s_(t.entry ? detail::cancel_state::make_timed(parent.s_) : detail::cancel_state::make(parent.s_)) {}
	detail::cancel_state* s_;
};

// Runs f(cancel_reason) (or f()) once when the token is requested, on the requesting thread,
// or now if it already is; an empty token never runs it. With std::stop_callback's lifetime:
// the destructor deregisters and waits for f running on another thread, and f may destroy its
// own registration. An exception from f is dropped. Registering allocates nothing.
template <typename F>
class cancel_callback : detail::cancel_node {
public:
	template <typename G>
	cancel_callback(const cancel_token& t, G&& f) noexcept(std::is_nothrow_constructible_v<F, G>) : f_(std::forward<G>(f)) {
		run = &invoke;
		if (!t.s_) { return; }
		if (t.s_->add(this)) {
			t.s_->retain();
			s_ = t.s_;
		} else {
			invoke(this, t.s_->reason());
		}
	}
	cancel_callback(const cancel_callback&) = delete;
	cancel_callback& operator=(const cancel_callback&) = delete;
	~cancel_callback() {
		if (s_) {
			s_->remove(this);
			s_->release();
		}
	}

private:
	static void invoke(detail::cancel_node* n, cancel_reason r) noexcept {
		F& f = static_cast<cancel_callback*>(n)->f_;
		try {
			if constexpr (std::is_invocable_v<F&, cancel_reason>) {
				f(r);
			} else {
				f();
			}
		} catch (...) {}  // dropped; the remaining callbacks still run
	}

	detail::cancel_state* s_ = nullptr;
	F f_;
};

template <typename F>
cancel_callback(const cancel_token&, F) -> cancel_callback<F>;

}  // namespace task
