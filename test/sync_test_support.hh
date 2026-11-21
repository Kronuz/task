// Shared by the sync_test pieces: the includes, CHECK, and the schedulers the tests drive.

#pragma once

#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "task.hh"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <sys/wait.h>
#include <unistd.h>
#define HAVE_FORK 1
#endif

extern int failures;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); if (++failures > 20) std::exit(1); } \
	} while (0)

using namespace std::chrono_literals;

// ---------------------------------------------------------------------------------------------
// Schedulers

// A scheduler on its own thread, for cross-thread tests under TSan.
class thread_scheduler {
public:
	thread_scheduler() : thread_([this] { loop(); }) {}
	~thread_scheduler() {
		{
			std::lock_guard lock(mutex_);
			stop_ = true;
		}
		cv_.notify_one();
		thread_.join();
	}
	void post(std::coroutine_handle<> h) {
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(h);
		}
		cv_.notify_one();
	}
	std::uint64_t now() const noexcept { return 0; }

private:
	void loop() {
		for (;;) {
			std::coroutine_handle<> h;
			{
				std::unique_lock lock(mutex_);
				cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
				if (queue_.empty()) { return; }
				h = queue_.front();
				queue_.pop_front();
			}
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		}
	}
	std::mutex mutex_;
	std::condition_variable cv_;
	std::deque<std::coroutine_handle<>> queue_;
	bool stop_ = false;
	std::thread thread_;
};

struct callback_base {
	virtual ~callback_base() = default;
	virtual void call() = 0;
};
template <typename F>
struct callback final : callback_base {
	explicit callback(F f) : f_(std::move(f)) {}
	void call() override { f_(); }
	F f_;
};

// A single-threaded scheduler whose timers fire only when the test says so. A timer's
// callback can be fired at once, or expired into a queue that runs later, like a completion
// that Asio has queued but not yet run. `immediate` makes post() resume in place, and `on_start`
// runs inside start_timer. `posts` counts post() calls.
class manual_scheduler {
public:
	class timer {
	public:
		timer(manual_scheduler& s, std::size_t id) : s_(&s), id_(id) {}
		timer(timer&& o) noexcept : s_(std::exchange(o.s_, nullptr)), id_(o.id_) {}
		~timer() {
			if (s_) { s_->pending_.erase(id_); }  // destroys the callback unrun, as cancel does
		}

	private:
		manual_scheduler* s_;
		std::size_t id_;
	};

	void post(std::coroutine_handle<> h) {
		++posts;
		if (immediate) {
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		} else {
			ready_.push_back(h);
		}
	}
	std::uint64_t now() const noexcept { return 0; }

	template <typename F>
	timer start_timer(std::chrono::steady_clock::time_point deadline, F&& f) {
		if (fail_start) { throw std::runtime_error("no timer"); }
		last_deadline = deadline;
		std::size_t id = next_id_++;
		pending_[id] = std::make_unique<callback<std::decay_t<F>>>(std::forward<F>(f));
		timer t(*this, id);
		if (on_start) { on_start(id); }
		return t;
	}

	void fire(std::size_t id) {  // the deadline passes and the callback runs now
		auto it = pending_.find(id);
		if (it == pending_.end()) { return; }
		auto cb = std::move(it->second);
		pending_.erase(it);
		cb->call();
	}
	void expire(std::size_t id) {  // the deadline passes; the callback is queued, not run
		auto it = pending_.find(id);
		if (it == pending_.end()) { return; }
		queued_.push_back(std::move(it->second));
		pending_.erase(it);
	}
	void run_queued() {
		auto q = std::move(queued_);
		queued_.clear();
		for (auto& cb : q) { cb->call(); }
	}
	void run() {
		while (!ready_.empty()) {
			auto h = ready_.front();
			ready_.pop_front();
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		}
	}
	task::scheduler_ref ref() { return task::scheduler_ref::of(*this); }
	std::size_t pending() const { return pending_.size(); }
	std::size_t ready() const { return ready_.size(); }

	bool immediate = false, fail_start = false;
	int posts = 0;
	std::function<void(std::size_t)> on_start;
	std::chrono::steady_clock::time_point last_deadline;

private:
	std::deque<std::coroutine_handle<>> ready_;
	std::map<std::size_t, std::unique_ptr<callback_base>> pending_;
	std::vector<std::unique_ptr<callback_base>> queued_;
	std::size_t next_id_ = 0;
};

struct probe_result {
	int got = 0;      // 0 pending, 1 holds the lock, 2 cancelled
	int resumed = 0;  // times the coroutine ran past its lock
};

// Defined in the piece that owns them.
task::task<void> plain_waiter(task::async_mutex& m, probe_result* out);
void test_mutex();
void test_deadline();
void test_settle_inline();
void test_lock_token();
void test_lock_destroyed();
