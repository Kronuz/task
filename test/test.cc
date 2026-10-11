// task: values and exceptions, symmetric transfer in constant stack, executor affinity
// (single- and multi-threaded), budgets and demotion, cancellation, when_all, and a
// randomized cancellation stress run (argv[1] tasks, default 1,000,000).
#include "task.hh"

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

int main(int argc, char** argv) {
	task::inline_scheduler s;

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
