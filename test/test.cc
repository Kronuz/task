// task: values and exceptions, symmetric transfer in constant stack, executor affinity
// (single- and multi-threaded), budgets and demotion, cancellation, when_all, and a
// sync_wait (threads, ordering, restore, pool bound), a randomized cancellation stress run (argv[1] tasks, default 1,000,000),
// and cancel tokens: reasons, callbacks and linked sources.
#define TASK_COUNT_FRAMES 1
#define TASK_TEST_HOOKS 1
#include "task.hh"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
task::task<int> sync_inside(task::scheduler_ref where) { co_return task::sync_wait(hop_value(where, 10)); }

// Calls sync_wait and records whether the caller's current scheduler survived it.
task::task<void> restores_current(task::scheduler_ref where, bool* restored) {
	auto before = task::current();
	task::sync_wait(hop_value(where, 3));
	*restored = bool(before) && task::current() == before;
	co_return;
}

// Appends to a trace around a sync_wait of work that is already done or hops to `where`.
task::task<void> traced_sync(std::vector<std::string>* trace, task::scheduler_ref where, bool hop) {
	trace->push_back("A:before");
	if (hop) { task::sync_wait(hop_value(where, 1)); } else { task::sync_wait(answer()); }
	trace->push_back("A:after");
	co_return;
}
task::task<void> traced(std::vector<std::string>* trace, const char* name) {
	trace->push_back(name);
	co_return;
}

// Results that are move-constructible only, and moves that throw.
struct pinned {
	int value;
	int& ref;
	pinned(int v, int& r) : value(v), ref(r) {}
	pinned(pinned&&) = default;  // not move-assignable: reference member
};
task::task<pinned> make_pinned(int& r) { co_return pinned(5, r); }

inline int move_budget = -1;  // the move constructor that makes this reach zero throws
struct shaky {
	int value = 0;
	explicit shaky(int v) : value(v) {}
	shaky(shaky&& o) : value(o.value) {
		if (move_budget >= 0 && move_budget-- == 0) { throw std::runtime_error("move"); }
	}
};
task::task<shaky> make_shaky(task::scheduler_ref where) {
	co_await task::schedule_on{where};
	co_return shaky(3);
}

// Frames allocated here and freed on `where`'s thread; reports that thread's cached frames.
task::task<std::size_t> worker_cached(task::scheduler_ref where) {
	co_await task::schedule_on{where};
	co_return task::detail::pool().cached();
}

// ---------------------------------------------------------------------------------------------
// Cancel tokens: reasons (D1), callbacks (D2), linked sources (D3)

// Sanitizer runtimes replace operator new themselves, so only plain builds count allocations.
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define TEST_COUNTS_NEW 0
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define TEST_COUNTS_NEW 0
#endif
#endif
#ifndef TEST_COUNTS_NEW
#define TEST_COUNTS_NEW 1
#endif

#if TEST_COUNTS_NEW
static std::atomic<std::size_t> allocations{0};
void* operator new(std::size_t n) {
	allocations.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(n ? n : 1)) { return p; }
	throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
#endif

// Sleeps inside the callback, so a deregistration from another thread overlaps it.
struct slow_callback {
	std::atomic<bool>* entered;
	int* value;
	void operator()() const {
		entered->store(true);
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
		*value = 42;  // plain write: TSan checks the deregistration waited for it
	}
};

// Destroys its own registration and the source being requested, from inside the callback.
struct self_destroying {
	std::optional<task::cancel_callback<self_destroying>>* self;
	std::optional<task::cancel_source>* source;
	int* ran;
	void operator()() const {
		++*ran;
		source->reset();
		self->reset();
	}
};

task::cancel_source* hook_parent = nullptr;
void hook_request_parent() { hook_parent->request(task::cancel_reason::shutdown); }

void test_cancel_tokens() {
	using task::cancel_reason;
	const auto base = task::detail::live_frames();

	// D1: an empty token is never requested and has no reason.
	{
		task::cancel_token t;
		CHECK(!t.requested() && t.reason() == cancel_reason::none);
	}
	// D1: the first request wins and sets the reason; tokens see it.
	{
		task::cancel_source s;
		auto t = s.token();
		CHECK(!t.requested() && t.reason() == cancel_reason::none);
		CHECK(s.request(cancel_reason::deadline));
		CHECK(!s.request(cancel_reason::shutdown));
		CHECK(t.requested() && t.reason() == cancel_reason::deadline && s.reason() == cancel_reason::deadline);
		task::cancel_source d;
		CHECK(d.request() && d.reason() == cancel_reason::requested);
		task::cancel_source n;
		CHECK(n.request(cancel_reason::none) && n.reason() == cancel_reason::requested);  // never requested without a reason
		task::cancel_source u;
		CHECK(u.request(cancel_reason(int(cancel_reason::user) + 1)) && u.token().reason() == cancel_reason(65));
	}
	// D1: eight racing requests: exactly one wins and its reason stays; a reader never sees a
	// requested token without a reason.
	for (int round = 0; round < 200; ++round) {
		task::cancel_source s;
		auto t = s.token();
		std::atomic<int> ready{0}, winner{-1}, wins{0};
		std::vector<std::thread> threads;
		for (int i = 0; i < 8; ++i) {
			threads.emplace_back([&, i] {
				ready.fetch_add(1);
				while (ready.load() < 8) { std::this_thread::yield(); }
				if (s.request(cancel_reason(64 + i))) {
					winner.store(i);
					wins.fetch_add(1);
				}
			});
		}
		while (!t.requested()) {
			if (t.reason() != cancel_reason::none) { CHECK(t.requested()); }
		}
		CHECK(t.reason() != cancel_reason::none);
		for (auto& th : threads) { th.join(); }
		CHECK(wins.load() == 1);
		CHECK(t.reason() == cancel_reason(64 + winner.load()));
	}
#if TEST_COUNTS_NEW
	// D1: steady-state sources, linked sources, registrations and requests call no operator new;
	// an empty token allocates nothing at all.
	{
		{
			task::cancel_source warm;
			task::cancel_source warm_child(warm.token());
		}
		const auto before = allocations.load();
		for (int i = 0; i < 10000; ++i) {
			task::cancel_source s;
			task::cancel_source child(s.token());
			auto t = child.token();
			int ran = 0;
			task::cancel_callback cb(t, [&] { ++ran; });
			if (i % 2) { s.request(cancel_reason::shutdown); }
			CHECK(ran == i % 2);
		}
		task::cancel_token empty;
		task::budget b(std::chrono::seconds(1), empty);
		for (int i = 0; i < 10000; ++i) {
			auto copy = empty;
			CHECK(!copy.requested() && !b.token().requested());
		}
		CHECK(allocations.load() == before);
	}
#endif

	// D2: a callback registered before the request runs on it with the reason; one registered
	// after runs at once; a destroyed one, or one on an empty token, never runs.
	{
		task::cancel_source s;
		int before = 0, after = 0, gone = 0, inert = 0;
		cancel_reason seen = cancel_reason::none;
		task::cancel_callback a(s.token(), [&](cancel_reason r) {
			++before;
			seen = r;
		});
		{
			task::cancel_callback g(s.token(), [&] { ++gone; });
		}
		task::cancel_callback e(task::cancel_token{}, [&] { ++inert; });
		CHECK(before == 0);
		CHECK(s.request(cancel_reason::shutdown));
		CHECK(before == 1 && seen == cancel_reason::shutdown && gone == 0);
		task::cancel_callback b(s.token(), [&](cancel_reason r) {
			++after;
			seen = r;
		});
		CHECK(after == 1 && seen == cancel_reason::shutdown);
		CHECK(!s.request());
		CHECK(before == 1 && after == 1 && inert == 0);
	}
	// D2: deregistering while the callback runs on another thread waits for it to return.
	for (int round = 0; round < 20; ++round) {
		task::cancel_source s;
		std::atomic<bool> entered{false};
		int value = 0;
		std::optional<task::cancel_callback<slow_callback>> cb;
		cb.emplace(s.token(), slow_callback{&entered, &value});
		std::thread requester([&] { s.request(); });
		while (!entered.load()) { std::this_thread::yield(); }
		cb.reset();
		CHECK(value == 42);
		requester.join();
	}
	// D2: a callback destroys its own registration and the source being requested (the last
	// outside reference to the state) from inside request().
	{
		std::optional<task::cancel_callback<self_destroying>> self;
		std::optional<task::cancel_source> source;
		source.emplace();
		int ran = 0;
		self.emplace(source->token(), self_destroying{&self, &source, &ran});
		CHECK(task::detail::live_frames() == base + 1);
		CHECK(source->request());
		CHECK(ran == 1 && !self && !source);
		CHECK(task::detail::live_frames() == base);
	}
	// D2: a throwing callback is dropped and the next still runs, now and on request.
	{
		task::cancel_source s;
		int ran = 0;
		task::cancel_callback a(s.token(), [&] { ++ran; });
		task::cancel_callback b(s.token(), [] { throw std::runtime_error("callback"); });  // runs first
		CHECK(s.request());
		CHECK(ran == 1);
		task::cancel_callback c(s.token(), [](cancel_reason) { throw 1; });
		task::cancel_callback d(s.token(), [&] { ++ran; });
		CHECK(ran == 2);
	}

	// D3: a parent's request reaches its children and grandchildren with its reason; a child's
	// request stays below it.
	{
		task::cancel_source parent;
		task::cancel_source child(parent.token());
		task::cancel_source grandchild(child.token());
		task::cancel_source sibling(parent.token());
		int ran = 0;
		cancel_reason seen = cancel_reason::none;
		task::cancel_callback cb(grandchild.token(), [&](cancel_reason r) {
			++ran;
			seen = r;
		});
		CHECK(sibling.request(cancel_reason::user));
		CHECK(!parent.requested() && !child.requested());
		CHECK(parent.request(cancel_reason::shutdown));
		CHECK(child.reason() == cancel_reason::shutdown && grandchild.reason() == cancel_reason::shutdown);
		CHECK(ran == 1 && seen == cancel_reason::shutdown);
		CHECK(sibling.reason() == cancel_reason::user);
	}
	// D3: a child of a requested parent is born requested; an empty parent links nothing.
	{
		task::cancel_source parent;
		parent.request(cancel_reason::deadline);
		task::cancel_source child(parent.token());
		CHECK(child.requested() && child.reason() == cancel_reason::deadline);
		// Requested after the check and before the child registers: still born requested.
		task::cancel_source late;
		hook_parent = &late;
		task::detail::test_hooks::before_link = hook_request_parent;
		task::cancel_source late_child(late.token());
		task::detail::test_hooks::before_link = nullptr;
		CHECK(late_child.requested() && late_child.reason() == cancel_reason::shutdown);
		task::cancel_source alone{task::cancel_token{}};
		CHECK(!alone.requested() && alone.request() && alone.reason() == cancel_reason::requested);
	}
	// D3: a child keeps its parent's state until it dies; both are freed after; a child that
	// died first is no longer on its parent's list (ASan).
	CHECK(task::detail::live_frames() == base);
	{
		{
			std::optional<task::cancel_source> parent;
			parent.emplace();
			task::cancel_source child(parent->token());
			CHECK(task::detail::live_frames() == base + 2);
			parent.reset();
			CHECK(task::detail::live_frames() == base + 2);
			CHECK(!child.requested());
		}
		CHECK(task::detail::live_frames() == base);
		{
			task::cancel_source parent;
			{
				task::cancel_source child(parent.token());
			}
			CHECK(task::detail::live_frames() == base + 1);
			CHECK(parent.request());
		}
		CHECK(task::detail::live_frames() == base);
	}
	// D3: children come and go on one thread while the parent is requested on another (TSan).
	for (int round = 0; round < 100; ++round) {
		task::cancel_source parent;
		std::atomic<bool> go{false};
		std::thread requester([&] {
			while (!go.load()) { std::this_thread::yield(); }
			parent.request(cancel_reason::shutdown);
		});
		for (int i = 0; i < 100; ++i) {
			if (i == 50) { go.store(true); }
			task::cancel_source child(parent.token());
			int ran = 0;
			task::cancel_callback cb(child.token(), [&] { ++ran; });
			if (child.requested()) { CHECK(child.reason() == cancel_reason::shutdown); }
		}
		requester.join();
	}
	// D1: a poller that sees requested() also sees the requester's earlier writes (TSan reports
	// a race on `data` if the load is relaxed).
	for (int round = 0; round < 200; ++round) {
		task::cancel_source s;
		auto t = s.token();
		int data = 0;
		int seen = -1;
		std::thread poller([&] {
			while (!t.requested()) {}
			seen = data;
		});
		data = 1;
		s.request(cancel_reason::user);
		poller.join();
		CHECK(seen == 1);
	}
	CHECK(task::detail::live_frames() == base);
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
			CHECK(task::sync_wait(sync_inside(w)) == 45);
			std::promise<int> done;
			task::spawn(task::scheduler_ref::of(home), sync_inside(w),
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
		CHECK(task::run(inl, sync_inside(r)) == 45);

		// The caller's current scheduler is restored, on a worker and under an inline run().
		bool restored = false;
		task::run(inl, restores_current(r, &restored));
		CHECK(restored);
		CHECK(!task::current());
		thread_scheduler worker, home;
		restored = false;
		std::promise<void> done;
		task::spawn(task::scheduler_ref::of(home), restores_current(task::scheduler_ref::of(worker), &restored),
			[&](std::exception_ptr) { done.set_value(); });
		done.get_future().get();
		CHECK(restored);

		// A task that is already done leaves queued work alone: FIFO order holds.
		std::vector<std::string> trace;
		task::spawn(r, traced_sync(&trace, r, false));
		task::spawn(r, traced(&trace, "B"));
		task::spawn(r, traced(&trace, "C"));
		inl.run();
		CHECK((trace == std::vector<std::string>{"A:before", "A:after", "B", "C"}));

		// One that hops to the same scheduler waits behind B and C only, not work queued after it.
		trace.clear();
		task::spawn(r, traced_sync(&trace, r, true));
		task::spawn(r, traced(&trace, "B"));
		task::spawn(r, traced(&trace, "C"));
		inl.run();
		CHECK((trace == std::vector<std::string>{"A:before", "B", "C", "A:after"}));
	}

	// sync_wait needs only move-constructible results, and a throwing move reaches the caller.
	{
		int x = 1;
		CHECK(task::sync_wait(make_pinned(x)).value == 5);
		thread_scheduler worker;
		int thrown = 0;
		for (int n = 0; n < 8; ++n) {
			move_budget = n;
			try { CHECK(task::sync_wait(make_shaky(task::scheduler_ref::of(worker))).value == 3); } catch (const std::runtime_error&) { ++thrown; }
		}
		move_budget = -1;
		CHECK(thrown >= 3);
	}

	// Frames freed on another thread cannot pile up without bound in its pool.
	{
		thread_scheduler worker;
		auto w = task::scheduler_ref::of(worker);
		for (int i = 0; i < 20000; ++i) { task::sync_wait(hop_value(w, 1)); }
		CHECK(task::sync_wait(worker_cached(w)) <= 4 * task::detail::frame_pool::cap);
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

	test_cancel_tokens();

	std::printf("task: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
