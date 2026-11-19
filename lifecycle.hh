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

// A process's lifecycle (docs/design/service-shape.md): components start in order, and at the
// stop source's stop every stop() runs in reverse, then each drain in reverse inside one bound,
// then every flush() in reverse. A set of components is a component, so lifecycles nest. Stop
// sources: stop_source (tests, admin commands, a parent component, a tool's end) and
// stop_signals (SIGINT and SIGTERM: stop, hurry, flush then die). POSIX.

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <initializer_list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "task.hh"

namespace task {

// What a drain gets. Requests in flight wait on `cancel`; cancelled work on `abandon`; pacing
// sleeps and durable work on neither (service-shape D13).
struct stop_bound {
	cancel_token cancel;   // bound - grace after the stop, or at the hurry
	cancel_token abandon;  // grace after `cancel`: stop waiting and report what is left
};

struct stop_timing {
	std::chrono::nanoseconds bound = std::chrono::seconds(35);
	std::chrono::nanoseconds grace = std::chrono::seconds(5);
};

// start(stop) runs on the loop and returns once serving, or at stop; stop() runs on any thread,
// at once, idempotent, and refuses new outside work; drain(b) runs on the loop and returns what
// it left undone; flush() runs on any thread, blocks, is idempotent, and returns once
// acknowledged writes are durable. An optional name() labels a failing start.
template <typename C>
concept component = requires(C& c, cancel_token stop, const stop_bound& b) {
	{ c.start(stop) } -> std::same_as<task<void>>;
	c.stop();
	{ c.drain(b) } -> std::same_as<task<std::vector<std::string>>>;
	c.flush();
};

// A drain's lines and its failure, both kept: a set reads a nested set's through drain_all.
struct drained {
	std::vector<std::string> unfinished;
	std::exception_ptr failure;
};

// Components in start order; itself a component. Only the started ones are stopped, drained and
// flushed. A start that throws leaves the earlier ones started (a nested set its own started
// children too) and names the failing one in failed().
class components {
public:
	template <component... C>
	explicit components(C&... cs) : items_{entry::of(cs)...} {}
	components(const components&) = delete;
	components& operator=(const components&) = delete;

	task<void> start(cancel_token stop) {
		started_.store(0, std::memory_order_release);
		failed_.clear();
		for (std::size_t i = 0; i < items_.size() && !stop.requested(); ++i) {
			const entry& c = items_[i];
			std::exception_ptr e;
			try {
				co_await c.start(c.self, stop);
			} catch (...) {
				e = std::current_exception();
			}
			if (e) {
				failed_ = c.label(c.self, i);
				if (c.partial && c.partial(c.self)) { started_.store(i + 1, std::memory_order_release); }
				std::rethrow_exception(e);
			}
			started_.store(i + 1, std::memory_order_release);
		}
	}

	void stop() {
		for (std::size_t i = started(); i-- > 0;) { items_[i].stop(items_[i].self); }
	}

	// Each started drain in reverse. One that leaves lines ends the drains: what it left may
	// still use the earlier components, so those only flush.
	task<drained> drain_all(const stop_bound& b) {
		drained d;
		for (std::size_t i = started(); i-- > 0 && d.unfinished.empty();) {
			drained one = co_await items_[i].drain(items_[i].self, b);
			for (auto& line : one.unfinished) { d.unfinished.push_back(std::move(line)); }
			if (!d.failure) { d.failure = one.failure; }
		}
		co_return d;
	}
	// drain_all's lines; its failure rethrown instead, once every drain ran.
	task<std::vector<std::string>> drain(const stop_bound& b) {
		drained d = co_await drain_all(b);
		if (d.failure) { std::rethrow_exception(d.failure); }
		co_return std::move(d.unfinished);
	}

	// Any thread: every started flush in reverse; the first throw is rethrown after all ran.
	void flush() {
		std::exception_ptr first;
		for (std::size_t i = started(); i-- > 0;) {
			try {
				items_[i].flush(items_[i].self);
			} catch (...) {
				if (!first) { first = std::current_exception(); }
			}
		}
		if (first) { std::rethrow_exception(first); }
	}

	std::size_t started() const noexcept { return started_.load(std::memory_order_acquire); }
	// The component whose start threw: its name() or position, a nested set's as "1/0".
	const std::string& failed() const noexcept { return failed_; }

private:
	struct entry {
		void* self;
		task<void> (*start)(void*, cancel_token);
		void (*stop)(void*);
		task<drained> (*drain)(void*, const stop_bound&);
		void (*flush)(void*);
		std::string (*label)(void*, std::size_t);
		bool (*partial)(void*);  // a set whose start threw with children started

		template <typename C>
		static entry of(C& c) {
			entry e{&c, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};
			e.start = [](void* p, cancel_token stop) { return static_cast<C*>(p)->start(std::move(stop)); };
			e.stop = [](void* p) { static_cast<C*>(p)->stop(); };
			e.flush = [](void* p) { static_cast<C*>(p)->flush(); };
			e.label = [](void* p, std::size_t i) -> std::string {
				std::string s;
				if constexpr (requires(C& x) { std::string(x.name()); }) {
					s = std::string(static_cast<C*>(p)->name());
				} else {
					s = std::to_string(i);
				}
				if constexpr (std::is_same_v<C, components>) {
					if (!static_cast<C*>(p)->failed().empty()) { s += "/" + static_cast<C*>(p)->failed(); }
				}
				return s;
			};
			if constexpr (std::is_same_v<C, components>) {
				e.drain = [](void* p, const stop_bound& b) { return static_cast<components*>(p)->drain_all(b); };
				e.partial = [](void* p) { return static_cast<components*>(p)->started() > 0; };
			} else {
				e.drain = &drain_one<C>;
			}
			return e;
		}
		template <typename C>
		static task<drained> drain_one(void* p, const stop_bound& b) {
			drained d;
			try {
				d.unfinished = co_await static_cast<C*>(p)->drain(b);
			} catch (...) {
				d.failure = std::current_exception();
			}
			co_return d;
		}
	};

	std::vector<entry> items_;
	std::atomic<std::size_t> started_{0};
	std::string failed_;
};
static_assert(component<components>);

// Where a stop comes from: request_stop() (a test, an admin QUIT, a tool's end), a parent's
// tokens (a parent component's stop), fail(), or stop_signals. Hurry implies stop.
class stop_source {
public:
	stop_source() = default;
	// Stopped with `parent` and hurried with `parent_hurry`: a nested lifecycle's source.
	explicit stop_source(const cancel_token& parent, const cancel_token& parent_hurry = {}) : stop_(parent), hurry_(parent_hurry) {}
	stop_source(const stop_source&) = delete;
	stop_source& operator=(const stop_source&) = delete;

	cancel_token stop() const noexcept { return stop_.token(); }
	cancel_token hurry() const noexcept { return hurry_.token(); }
	// Any thread.
	void request_stop() noexcept { stop_.request(cancel_reason::shutdown); }
	void request_hurry() noexcept {
		request_stop();
		hurry_.request(cancel_reason::shutdown);
	}
	// Any thread: keeps the first failure, then stops.
	void fail(std::exception_ptr e) noexcept {
		{
			std::lock_guard lock(m_);
			if (!failure_) { failure_ = std::move(e); }
		}
		request_stop();
	}
	std::exception_ptr failure() const {
		std::lock_guard lock(m_);
		return failure_;
	}

	// Removes its flush when destroyed.
	class [[nodiscard]] flush_registration {
	public:
		flush_registration(stop_source* s, std::uint64_t id) noexcept : s_(s), id_(id) {}
		flush_registration(flush_registration&& o) noexcept : s_(std::exchange(o.s_, nullptr)), id_(o.id_) {}
		flush_registration(const flush_registration&) = delete;
		flush_registration& operator=(const flush_registration&) = delete;
		~flush_registration() {
			if (s_) { s_->remove_flush(id_); }
		}

	private:
		stop_source* s_;
		std::uint64_t id_;
	};
	// What run_flushes() runs, last added first: the third signal's flushes.
	flush_registration add_flush(std::function<void()> f) {
		std::lock_guard lock(m_);
		flushes_.push_back({++last_id_, std::move(f)});
		return {this, last_id_};
	}
	// Any thread: every added flush, last first; a throw is dropped and the rest still run.
	void run_flushes() noexcept {
		std::lock_guard lock(m_);  // a lifecycle removing its flush waits for it to return
		for (std::size_t i = flushes_.size(); i-- > 0;) {
			try {
				flushes_[i].second();
			} catch (...) {}
		}
	}

private:
	void remove_flush(std::uint64_t id) noexcept {
		std::lock_guard lock(m_);
		std::erase_if(flushes_, [id](const auto& f) { return f.first == id; });
	}

	cancel_source stop_, hurry_;
	mutable std::mutex m_;
	std::exception_ptr failure_;
	std::vector<std::pair<std::uint64_t, std::function<void()>>> flushes_;
	std::uint64_t last_id_ = 0;
};

// The process's signals as a stop source, one per process. A sigaction handler (SA_RESTART)
// writes the signal to a pipe; a thread reads it: the first stop signal stops, the second
// hurries, the third runs every flush, restores SIG_DFL and raises it (service-shape D6). A dump
// signal calls `dump` on that thread, so a wedged loop still dumps.
class stop_signals : public stop_source {
public:
	explicit stop_signals(std::initializer_list<int> stops = {SIGINT, SIGTERM}, std::initializer_list<int> dumps = {},
	                      std::function<void(int)> dump = {})
		: stops_(stops), dumps_(dumps), dump_(std::move(dump)) {
		int fds[2];
		if (::pipe(fds) != 0) { throw std::runtime_error("task::stop_signals: pipe"); }
		read_fd_ = fds[0];
		write_fd_ = fds[1];
		::fcntl(read_fd_, F_SETFD, FD_CLOEXEC);
		::fcntl(write_fd_, F_SETFD, FD_CLOEXEC);
		::fcntl(write_fd_, F_SETFL, ::fcntl(write_fd_, F_GETFL) | O_NONBLOCK);  // the handler never blocks
		[[maybe_unused]] const int before = handler_fd().exchange(write_fd_);
		assert(before < 0 && "one stop_signals per process");
		for (int sig : stops_) { install(sig); }  // a signal before the thread waits in the pipe
		for (int sig : dumps_) { install(sig); }
		thread_ = std::thread([this] { loop(); });
	}
	stop_signals(const stop_signals&) = delete;
	stop_signals& operator=(const stop_signals&) = delete;
	// Restores the previous handlers, then joins the signal thread.
	~stop_signals() {
		for (auto& [sig, old] : previous_) { ::sigaction(sig, &old, nullptr); }
		handler_fd().store(-1);
		const unsigned char quit = 0;
		while (::write(write_fd_, &quit, 1) < 0 && errno == EINTR) {}
		thread_.join();
		::close(read_fd_);
		::close(write_fd_);
	}

	// The first stop signal, 0 while none came (a stop asked for, or none yet).
	int first_signal() const noexcept { return first_.load(std::memory_order_acquire); }

private:
	static std::atomic<int>& handler_fd() noexcept {
		static std::atomic<int> fd{-1};
		return fd;
	}
	static void handler(int sig) noexcept {
		const int saved = errno;
		if (const int fd = handler_fd().load(std::memory_order_relaxed); fd >= 0) {
			const unsigned char b = static_cast<unsigned char>(sig);
			[[maybe_unused]] const auto n = ::write(fd, &b, 1);
		}
		errno = saved;
	}
	void install(int sig) {
		struct sigaction sa {};
		sa.sa_handler = &handler;
		sigemptyset(&sa.sa_mask);
		sa.sa_flags = SA_RESTART;
		struct sigaction old {};
		if (::sigaction(sig, &sa, &old) != 0) { throw std::runtime_error("task::stop_signals: sigaction"); }
		previous_.emplace_back(sig, old);
	}
	void loop() {
		int seen = 0;
		for (;;) {
			unsigned char b = 0;
			const auto n = ::read(read_fd_, &b, 1);
			if (n < 0 && errno == EINTR) { continue; }
			if (n <= 0 || b == 0) { return; }
			const int sig = b;
			if (std::find(dumps_.begin(), dumps_.end(), sig) != dumps_.end()) {
				if (dump_) { dump_(sig); }
				continue;
			}
			switch (++seen) {
				case 1:
					first_.store(sig, std::memory_order_release);
					request_stop();
					break;
				case 2:
					request_hurry();
					break;
				default:
					run_flushes();
					::signal(sig, SIG_DFL);
					::raise(sig);
					std::_Exit(128 + sig);  // a signal whose default does not kill
			}
		}
	}

	std::vector<int> stops_, dumps_;
	std::function<void(int)> dump_;
	std::vector<std::pair<int, struct sigaction>> previous_;
	std::atomic<int> first_{0};
	int read_fd_ = -1, write_fd_ = -1;
	std::thread thread_;
};

// What a lifecycle reports once every flush returned.
struct stopped {
	std::exception_ptr failure;           // the first start, ready, drain or flush error, else the source's fail()
	std::string failed;                   // the component whose start threw (components::failed())
	std::vector<std::string> unfinished;  // what drains left at `abandon`, in drain order
	bool ready = false;                   // every start returned and ready() ran
};

namespace detail {

// b.cancel at `cancel_at` or the hurry, b.abandon `grace` after it; `finish` ends both waits.
inline task<void> stop_bound_timer(std::uint64_t cancel_at, std::chrono::nanoseconds grace, cancel_token hurry, cancel_token finish,
                                   cancel_source cancel, cancel_source abandon) {
	cancel_source wake(hurry);
	cancel_callback on_finish(finish, [&wake] { wake.request(); });
	co_await sleep_until(cancel_at, wake.token());
	if (finish.requested()) { co_return; }
	cancel.request(hurry.requested() ? cancel_reason::shutdown : cancel_reason::deadline);
	if (co_await sleep_for(grace, finish)) { abandon.request(cancel_reason::deadline); }
}

inline task<stopped> run_lifecycle(stop_source& source, stop_timing timing, std::function<void()> ready, components& set) {
	const scheduler_ref where = current();
	stopped r;
	const auto registration = source.add_flush([&set] { set.flush(); });
	try {
		co_await set.start(source.stop());
	} catch (...) {
		r.failure = std::current_exception();
		r.failed = set.failed();
	}
	if (!r.failure && !source.stop().requested()) {
		try {
			if (ready) { ready(); }
			r.ready = true;
		} catch (...) {
			r.failure = std::current_exception();
		}
		if (r.ready) { co_await sleep_until(never, source.stop()); }
	}

	const auto lead = timing.bound > timing.grace ? timing.bound - timing.grace : std::chrono::nanoseconds(0);
	cancel_source cancel, abandon, finish;
	const stop_bound b{cancel.token(), abandon.token()};
	event timer_ended;
	spawn(where, stop_bound_timer(where.now() + std::uint64_t(lead.count()), timing.grace, source.hurry(), finish.token(), cancel, abandon),
	      [&timer_ended](std::exception_ptr) { timer_ended.set(); });
	set.stop();
	drained d = co_await set.drain_all(b);
	finish.request();
	co_await timer_ended;
	try {
		set.flush();
	} catch (...) {
		if (!r.failure) { r.failure = std::current_exception(); }
	}
	r.unfinished = std::move(d.unfinished);
	if (!r.failure) { r.failure = d.failure; }
	if (!r.failure) { r.failure = source.failure(); }
	co_return r;
}

}  // namespace detail

// Runs on `loop`: starts the components in order, calls ready, and at source.stop() stops every
// one in reverse, then drains each in reverse inside timing's bound, then flushes every started
// one in reverse. A start failure skips to the stop of those already started. Every path ends
// with the flushes; each is also registered with the source for the third signal.
template <scheduler L, component... C>
task<stopped> lifecycle(L& loop, stop_source& source, stop_timing timing, std::function<void()> ready, C&... cs) {
	assert(current() == scheduler_ref::of(loop) && "a lifecycle runs on its loop");
	(void)loop;
	components set(cs...);
	co_return co_await detail::run_lifecycle(source, timing, std::move(ready), set);
}

}  // namespace task
