// task: values and exceptions, symmetric transfer in constant stack, executor affinity
// (single- and multi-threaded), budgets and demotion, cancellation, when_all, and a
// sync_wait, and a randomized cancellation stress run (argv[1] tasks, default 1,000,000).
#include "task.hh"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

static int failures = 0;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); if (++failures > 20) std::exit(1); } \
	} while (0)

// A scheduler on its own thread, for cross-thread affinity under TSan.
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
	std::thread::id id() const { return thread_.get_id(); }

private:
	void loop() {
		task::running_on scope(task::scheduler_ref::of(*this));
		for (;;) {
			std::coroutine_handle<> h;
			{
				std::unique_lock lock(mutex_);
				cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
				if (queue_.empty()) { return; }
				h = queue_.front();
				queue_.pop_front();
			}
			h.resume();
		}
	}
	std::mutex mutex_;
	std::condition_variable cv_;
	std::deque<std::coroutine_handle<>> queue_;
	bool stop_ = false;
	std::thread thread_;
};

task::task<int> answer() { co_return 42; }
task::task<void> nothing() { co_return; }
task::task<int> fails() {
	throw std::runtime_error("boom");
	co_return 0;
}

task::task<int> add(int a, int b) { co_return a + co_await answer() - 42 + b; }

task::task<long> many_awaits(int n) {
	long sum = 0;
	for (int i = 0; i < n; ++i) { sum += co_await answer(); }  // synchronous completions
	co_return sum;
}

task::task<int> chain(int depth) {
	if (depth == 0) { co_return 0; }
	co_return 1 + co_await chain(depth - 1);
}

task::task<int> hop(task::scheduler_ref other, std::thread::id* ran_on) {
	co_await task::schedule_on{other};
	*ran_on = std::this_thread::get_id();
	co_return 7;
}

task::task<int> stays_home(task::scheduler_ref home, task::scheduler_ref other, bool* home_ok, std::thread::id* hop_thread, std::thread::id* after) {
	int v = co_await hop(other, hop_thread);
	*home_ok = task::current() == home;
	*after = std::this_thread::get_id();
	co_return v;
}

task::task<int> busy(std::chrono::nanoseconds slice, std::vector<int>& trace, int id, int rounds) {
	task::budget b(slice);
	for (int r = 0; r < rounds; ++r) {
		trace.push_back(id);
		auto until = std::chrono::steady_clock::now() + slice * 2;  // always outlive the slice
		while (std::chrono::steady_clock::now() < until) {}
		co_await b.checkpoint();
	}
	co_return id;
}

std::coroutine_handle<> parked;
struct park {
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) const noexcept { parked = h; }
	void await_resume() const noexcept {}
};

task::task<int> cancellable(task::cancel_token token, int* progress) {
	task::budget b(std::chrono::seconds(10), token);
	for (;;) {
		++*progress;
		co_await b.checkpoint();
		if (*progress == 1000) { co_await park{}; }
	}
}

// A child that yields is later resumed by the scheduler, not from inside a transfer; its
// completion then starts the transfer loop, and the awaiter destroys the child's frame
// (the temporary task) from inside that loop. ASan checks nothing touches the frame after.
task::task<int> yields_then_returns(int v) {
	co_await task::yield();
	co_return v;
}

task::task<int> awaits_temporaries(int n) {
	int sum = 0;
	for (int i = 0; i < n; ++i) { sum += co_await yields_then_returns(i); }
	co_return sum;
}

// Helpers for sync_wait: hop to a scheduler, then work there.
task::task<std::thread::id> hop_id(task::scheduler_ref where) {
	co_await task::schedule_on{where};
	co_await task::yield();
	co_return std::this_thread::get_id();
}
task::task<int> hop_value(task::scheduler_ref where, int n) {
	co_await task::schedule_on{where};
	int sum = 0;
	for (int i = 0; i < n; ++i) {
		co_await task::yield();
		sum += i;
	}
	co_return sum;
}
task::task<void> hop_void(task::scheduler_ref where, std::atomic<int>* ran) {
	co_await task::schedule_on{where};
	++*ran;
}
task::task<int> hop_fail(task::scheduler_ref where) {
	co_await task::schedule_on{where};
	throw std::runtime_error("worker boom");
	co_return 0;
}
task::task<int> hop_until_cancelled(task::scheduler_ref where, task::cancel_token token) {
	co_await task::schedule_on{where};
	task::budget b(std::chrono::seconds(10), token);
	int n = 0;
	for (;;) {
		co_await task::yield();
		co_await b.checkpoint();
		++n;
	}
	co_return n;
}
struct sentinel {
	std::atomic<bool>* destroyed;
	sentinel(std::atomic<bool>* d) : destroyed(d) {}
	sentinel(sentinel&& o) noexcept : destroyed(std::exchange(o.destroyed, nullptr)) {}
	~sentinel() {
		if (destroyed) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));  // widens the window a late destroy would lose
			destroyed->store(true);
		}
	}
};
task::task<int> hop_holding(task::scheduler_ref where, sentinel) {
	co_await task::schedule_on{where};
	co_return 9;
}
task::task<int> sync_inside(task::scheduler_ref inner_home, task::scheduler_ref where) {
	co_return task::sync_wait(hop_value(where, 10)) + (inner_home ? 0 : 1);
}

int main(int argc, char** argv) {
	task::cycles::calibrate();
	task::inline_scheduler s;
	CHECK(task::run(s, awaits_temporaries(1000)) == 499500);

	// Values, void, exceptions, nesting.
	CHECK(task::run(s, answer()) == 42);
	task::run(s, nothing());
	CHECK(task::run(s, add(1, 2)) == 3);
	bool threw = false;
	try { task::run(s, fails()); } catch (const std::runtime_error& e) { threw = std::string(e.what()) == "boom"; }
	CHECK(threw);

	// Symmetric transfer: a million synchronous awaits and a deep chain in constant stack.
	CHECK(task::run(s, many_awaits(1000000)) == 42000000L);
	CHECK(task::run(s, chain(100000)) == 100000);

	// Affinity on one thread: a task that moves to another scheduler returns home.
	{
		task::inline_scheduler other;
		bool home_ok = false;
		std::thread::id a, b;
		std::optional<int> got;
		task::spawn(task::scheduler_ref::of(s), stays_home(task::scheduler_ref::of(s), task::scheduler_ref::of(other), &home_ok, &a, &b),
			[&](std::exception_ptr, std::optional<int> v) { got = v; });
		while (s.queued() || other.queued()) { s.run(); other.run(); }
		CHECK(got == 7);
		CHECK(home_ok);
	}

	// Affinity across threads (TSan): the hop runs on the worker, the rest on the home thread.
	{
		thread_scheduler home, worker;
		bool home_ok = false;
		std::thread::id hop_thread, after;
		std::promise<int> done;
		auto f = done.get_future();
		task::spawn(task::scheduler_ref::of(home),
			stays_home(task::scheduler_ref::of(home), task::scheduler_ref::of(worker), &home_ok, &hop_thread, &after),
			[&](std::exception_ptr, std::optional<int> v) { done.set_value(v.value_or(-1)); });
		CHECK(f.get() == 7);
		CHECK(home_ok);
		CHECK(hop_thread == worker.id());
		CHECK(after == home.id());
	}

	// Budgets: spent slices demote, so two busy tasks interleave instead of running back to back.
	{
		std::vector<int> trace;
		auto t1 = busy(std::chrono::microseconds(200), trace, 1, 3);
		auto t2 = busy(std::chrono::microseconds(200), trace, 2, 3);
		std::vector<task::task<int>> both;
		both.push_back(std::move(t1));
		both.push_back(std::move(t2));
		auto r = task::run(s, task::when_all(std::move(both)));
		CHECK((r == std::vector<int>{1, 2}));
		CHECK((trace == std::vector<int>{1, 2, 1, 2, 1, 2}));
	}

	// Cancellation is observed at the next checkpoint.
	{
		task::cancel_source source;
		int progress = 0;
		std::exception_ptr error;
		task::spawn(task::scheduler_ref::of(s), cancellable(source.token(), &progress), [&](std::exception_ptr e, std::optional<int>) { error = e; });
		s.run();  // runs until the task parks at 1000
		CHECK(progress == 1000);
		source.request();
		s.post(parked);
		s.run();
		bool was_cancelled = false;
		try { std::rethrow_exception(error); } catch (const task::cancelled&) { was_cancelled = true; } catch (...) {}
		CHECK(was_cancelled);
		CHECK(progress == 1001);  // one more step, then the checkpoint throws
	}

	// when_all: tuples with void, vectors, and the first exception after all finish.
	{
		auto [x, y, z] = task::run(s, task::when_all(answer(), nothing(), add(2, 3)));
		CHECK(x == 42);
		CHECK(z == 5);
		(void)y;
		std::vector<task::task<int>> v;
		for (int i = 0; i < 100; ++i) { v.push_back(chain(i)); }
		auto r = task::run(s, task::when_all(std::move(v)));
		for (int i = 0; i < 100; ++i) { CHECK(r[std::size_t(i)] == i); }
		bool caught = false;
		try { task::run(s, task::when_all(answer(), fails())); } catch (const std::runtime_error&) { caught = true; }
		CHECK(caught);
		CHECK(task::run(s, task::when_all(std::vector<task::task<int>>{})).empty());
	}

	// sync_wait: blocks the caller until a task finishes on another thread.
	{
		thread_scheduler worker;
		auto w = task::scheduler_ref::of(worker);
		CHECK(task::sync_wait(hop_id(w)) == worker.id());
		CHECK(task::sync_wait(hop_value(w, 100)) == 4950);
		CHECK(task::sync_wait(answer()) == 42);  // completes synchronously
		task::sync_wait(nothing());
		std::atomic<int> ran{0};
		task::sync_wait(hop_void(w, &ran));
		CHECK(ran == 1);

		bool caught = false;
		try { task::sync_wait(hop_fail(w)); } catch (const std::runtime_error& e) { caught = std::string(e.what()) == "worker boom"; }
		CHECK(caught);
		caught = false;
		try { task::sync_wait(fails()); } catch (const std::runtime_error&) { caught = true; }
		CHECK(caught);

		// Cancellation requested from another thread is observed on the worker and rethrown.
		task::cancel_source source;
		std::thread canceller([&] {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			source.request();
		});
		bool was_cancelled = false;
		try { task::sync_wait(hop_until_cancelled(w, source.token())); } catch (const task::cancelled&) { was_cancelled = true; }
		canceller.join();
		CHECK(was_cancelled);

		// The task's frame is gone by the time the caller wakes.
		for (int i = 0; i < 20; ++i) {
			std::atomic<bool> destroyed{false};
			CHECK(task::sync_wait(hop_holding(w, sentinel(&destroyed))) == 9);
			CHECK(destroyed.load());
		}

		// A caller that is itself inside a scheduler still waits for another one.
		{
			thread_scheduler home;
			CHECK(task::sync_wait(sync_inside(task::scheduler_ref::of(home), w)) == 45);
			std::promise<int> done;
			task::spawn(task::scheduler_ref::of(home), sync_inside(task::scheduler_ref::of(home), w),
				[&](std::exception_ptr, std::optional<int> v) { done.set_value(v.value_or(-1)); });
			CHECK(done.get_future().get() == 45);
		}

		// Concurrent waiters (TSan).
		std::vector<std::thread> callers;
		std::atomic<int> total{0};
		for (int c = 0; c < 4; ++c) {
			callers.emplace_back([&] {
				for (int i = 0; i < 500; ++i) { total += task::sync_wait(hop_value(w, 3)); }
			});
		}
		for (auto& t : callers) { t.join(); }
		CHECK(total == 4 * 500 * 3);
	}

	// sync_wait on a thread that is draining an inline_scheduler drains it instead of deadlocking.
	{
		task::inline_scheduler inl;
		auto r = task::scheduler_ref::of(inl);
		CHECK(task::run(inl, sync_inside(r, r)) == 45);
	}

	// Stress: many tasks with random yields and random cancellation, all accounted for.
	{
		long n = argc > 1 ? std::atol(argv[1]) : 1000000;
		std::mt19937_64 rng(5);
		std::vector<task::cancel_source> sources(1024);
		long finished = 0, cancelled = 0;
		auto worker = [](task::cancel_token token, int steps) -> task::task<int> {
			task::budget b(std::chrono::seconds(10), token);
			for (int i = 0; i < steps; ++i) {
				co_await task::yield();
				co_await b.checkpoint();
			}
			co_return steps;
		};
		for (long i = 0; i < n; ++i) {
			auto& src = sources[std::size_t(i) % sources.size()];
			task::spawn(task::scheduler_ref::of(s), worker(src.token(), int(rng() % 4)), [&](std::exception_ptr e, std::optional<int>) {
				if (e) { ++cancelled; } else { ++finished; }
			});
			if (rng() % 64 == 0) { sources[rng() % sources.size()].request(); }
			if (i % 4096 == 0) { s.run(); }
		}
		s.run();
		CHECK(finished + cancelled == n);
		CHECK(cancelled > 0);
	}

	std::printf("task: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
