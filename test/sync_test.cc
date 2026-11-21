// task: the per-resume slice, event, and async_mutex with lock(token) and deadline,
// including both orders of the grant/cancel race, the await_suspend handshake, threads (TSan),
// frames destroyed while waiting, and the destructor asserts.
#include "sync_test_support.hh"

int failures = 0;

// ---------------------------------------------------------------------------------------------
// Per-resume slice

// Runs each handle in its own running_on scope, like asio_scheduler, and counts demotions.
struct counting {
	std::deque<std::coroutine_handle<>> ready, demoted_q;
	int demoted = 0;
	void post(std::coroutine_handle<> h) { ready.push_back(h); }
	void post_demoted(std::coroutine_handle<> h) {
		++demoted;
		demoted_q.push_back(h);
	}
	std::uint64_t now() const noexcept { return 0; }
	void run() {
		while (!ready.empty() || !demoted_q.empty()) {
			auto& q = ready.empty() ? demoted_q : ready;
			auto h = q.front();
			q.pop_front();
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		}
	}
};

std::coroutine_handle<> parked_handle;
struct park_here {
	bool await_ready() const noexcept { return false; }
	void await_suspend(std::coroutine_handle<> h) const noexcept { parked_handle = h; }
	void await_resume() const noexcept {}
};

int suspended_demotions = -1, busy_demotions = -1;

// 200 us slice, a 2 ms suspension, almost no work.
task::task<void> suspended_task(counting& c) {
	task::budget b(200us);
	int before = c.demoted;
	co_await park_here{};
	co_await b.checkpoint();
	suspended_demotions = c.demoted - before;
}

// The same slice, 1 ms of continuous work.
task::task<void> busy_task(counting& c) {
	task::budget b(200us);
	int before = c.demoted;
	auto end = std::chrono::steady_clock::now() + 1ms;
	while (std::chrono::steady_clock::now() < end) {}
	co_await b.checkpoint();
	busy_demotions = c.demoted - before;
}

void spin(std::chrono::nanoseconds d) {
	auto end = std::chrono::steady_clock::now() + d;
	while (std::chrono::steady_clock::now() < end) {}
}

task::task<void> trace_x(std::vector<std::string>* t) {
	task::budget b(200us);
	co_await task::yield();
	co_await b.checkpoint();
	t->push_back("X");
}
task::task<void> trace_busy(std::vector<std::string>* t) {
	spin(1ms);
	t->push_back("Y");
	co_return;
}
task::task<void> trace_z(std::vector<std::string>* t) {
	t->push_back("Z1");
	co_await task::yield();
	t->push_back("Z2");
}

void test_slice() {
	counting c;
	task::spawn(task::scheduler_ref::of(c), suspended_task(c), [](std::exception_ptr) {});
	c.run();
	std::this_thread::sleep_for(2ms);
	c.post(parked_handle);
	c.run();
	CHECK(suspended_demotions == 0);  // suspended time is not run time
	task::spawn(task::scheduler_ref::of(c), busy_task(c), [](std::exception_ptr) {});
	c.run();
	CHECK(busy_demotions == 1);  // a long continuous run is still demoted

	// inline_scheduler starts a slice per handle: X's slice does not include Y's run.
	task::inline_scheduler s;
	std::vector<std::string> trace;
	auto r = task::scheduler_ref::of(s);
	task::spawn(r, trace_x(&trace));
	task::spawn(r, trace_busy(&trace));
	task::spawn(r, trace_z(&trace));
	s.run();
	CHECK((trace == std::vector<std::string>{"Y", "Z1", "X", "Z2"}));  // X demoted would come last

	// Scopes nest: leaving one restores the enclosing slice.
	{
		task::running_on outer(r);
		auto start = task::detail::slice_start;
		spin(100us);
		{
			task::running_on inner(r);
			CHECK(task::detail::slice_start > start);
		}
		CHECK(task::detail::slice_start == start);
	}
}

// ---------------------------------------------------------------------------------------------
// event

task::task<void> wait_then_record(task::event& e, std::vector<int>* order, int id) {
	co_await e;
	order->push_back(id);
}
task::task<void> set_it(task::event& e) {
	e.set();
	co_return;
}

void test_event() {
	{
		task::inline_scheduler s;
		auto r = task::scheduler_ref::of(s);
		task::event e;
		std::vector<int> order;
		for (int i = 0; i < 3; ++i) { task::spawn(r, wait_then_record(e, &order, i)); }
		s.run();
		CHECK(order.empty() && !e.is_set());
		e.set();  // from outside any scheduler: waiters are posted home, not run here
		CHECK(order.empty() && e.is_set());
		CHECK(s.queued() == 3);
		s.run();
		CHECK((order == std::vector<int>{0, 1, 2}));  // in the order they began waiting
		e.set();                                       // idempotent
		task::spawn(r, wait_then_record(e, &order, 3));
		s.run();
		CHECK((order == std::vector<int>{0, 1, 2, 3}));  // a set event does not suspend
	}
	{  // A waiter resumes on its own scheduler, whichever scheduler calls set().
		task::inline_scheduler a, b;
		task::event e;
		std::vector<int> order;
		task::spawn(task::scheduler_ref::of(a), wait_then_record(e, &order, 1));
		a.run();
		task::spawn(task::scheduler_ref::of(b), set_it(e));
		b.run();
		CHECK(order.empty() && a.queued() == 1);
		a.run();
		CHECK((order == std::vector<int>{1}));
	}
	{  // Waiters with no scheduler resume where set() runs.
		task::event e;
		int ran = 0;
		auto waiter = [](task::event& ev, int* n) -> task::task<void> {
			co_await ev;
			++*n;
		};
		std::thread t([&] { task::sync_wait(waiter(e, &ran)); });
		std::this_thread::sleep_for(5ms);
		e.set();
		t.join();
		CHECK(ran == 1);
	}
	{  // Waiters arriving while another thread sets are all resumed exactly once.
		thread_scheduler w1, w2;
		for (int round = 0; round < 50; ++round) {
			task::event e;
			std::atomic<int> done{0};
			auto waiter = [](task::event& ev, std::atomic<int>* n) -> task::task<void> {
				co_await ev;
				++*n;
			};
			std::vector<std::thread> adders;
			for (int t = 0; t < 4; ++t) {
				adders.emplace_back([&, t] {
					for (int i = 0; i < 50; ++i) {
						task::spawn(task::scheduler_ref::of(t % 2 ? w1 : w2), waiter(e, &done));
					}
				});
			}
			std::this_thread::sleep_for(std::chrono::microseconds(100 * (round % 5)));
			std::thread setter([&] { e.set(); });
			for (auto& t : adders) { t.join(); }
			setter.join();
			for (int i = 0; i < 4000 && done.load() < 200; ++i) { std::this_thread::sleep_for(1ms); }
			CHECK(done.load() == 200);
		}
	}
}

// An event's waiters at teardown: a frame destroyed while linked unlinks itself, one whose
// wake was dropped leaves the event alone, and an event destroyed first orphans its waiters.
void test_event_teardown() {
	const auto base = task::detail::live_frames();
	// Destroyed while linked (the middle of three): set() resumes only the other two.
	{
		task::inline_scheduler s;
		task::event e;
		std::vector<int> order;
		auto a = wait_then_record(e, &order, 0), b = wait_then_record(e, &order, 1), c = wait_then_record(e, &order, 2);
		for (auto* t : {&a, &b, &c}) { s.post(t->handle()); }
		s.run();
		b = {};
		e.set();
		CHECK(s.queued() == 2);
		s.run();
		CHECK((order == std::vector<int>{0, 2}));
	}
	// Taken by set() (its wake dropped, as by a closed scheduler), then the event and the frame
	// die: the frame does not touch the event.
	{
		task::inline_scheduler s;
		auto* e = new task::event;
		std::vector<int> order;
		auto a = wait_then_record(*e, &order, 0);
		s.post(a.handle());
		s.run();
		e->set();
		delete e;
		a = {};  // the queued wake is never run
		CHECK(order.empty());
	}
	// The event dies first: its waiter is orphaned (counted in Debug) and later destroyed.
	{
		task::inline_scheduler s;
		auto* e = new task::event;
		std::vector<int> order;
		auto a = wait_then_record(*e, &order, 0);
		s.post(a.handle());
		s.run();
		const auto orphans = task::detail::orphaned_waiters();
		delete e;
#ifndef NDEBUG
		CHECK(task::detail::orphaned_waiters() == orphans + 1);
#else
		(void)orphans;
#endif
		a = {};
		CHECK(order.empty());
	}
	CHECK(task::detail::live_frames() == base);
}

// ---------------------------------------------------------------------------------------------
// Destructor asserts

#if defined(HAVE_FORK) && !defined(NDEBUG)
bool aborts(const std::function<void()>& f) {
	pid_t pid = fork();
	if (pid == 0) {
		std::freopen("/dev/null", "w", stderr);
		f();
		_exit(0);
	}
	int status = 0;
	waitpid(pid, &status, 0);
	return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}

void test_destructor_asserts() {
	CHECK(!aborts([] {  // control: a clean teardown does not abort
		task::event e;
		task::async_mutex m;
		e.set();
		CHECK(m.try_lock());
		m.unlock();
	}));
	CHECK(!aborts([] {  // an event with a waiter orphans it instead of asserting
		task::inline_scheduler s;
		auto* e = new task::event;
		std::vector<int> order;
		task::spawn(task::scheduler_ref::of(s), wait_then_record(*e, &order, 1));
		s.run();
		delete e;
	}));
	// The ownership rule: a mutex local to root A, awaited by root B. Destroying A first
	// grants B and then destroys the mutex with the lock held: the assertion catches it.
	auto own = [](bool a_first) {
		task::inline_scheduler s;
		probe_result w;
		task::task<void> b;
		auto a = [](task::inline_scheduler& sch, task::task<void>* waiter, probe_result* out) -> task::task<void> {
			task::async_mutex m;
			auto g = co_await m.lock();
			*waiter = plain_waiter(m, out);
			sch.post(waiter->handle());
			co_await park_here{};
		}(s, &b, &w);
		s.post(a.handle());
		s.run();  // A holds the lock and parks; B queues on it
		if (!a_first) { b = {}; }
		a = {};
	};
	CHECK(aborts([&] { own(true); }));    // own_a_first
	CHECK(!aborts([&] { own(false); }));  // own_b_first: B's waiter leaves the queue first
	CHECK(aborts([] {  // a mutex still locked
		task::async_mutex m;
		(void)m.try_lock();
	}));
	CHECK(aborts([] {  // a mutex with a waiter
		task::inline_scheduler s;
		auto* m = new task::async_mutex;
		(void)m->try_lock();
		probe_result w;
		task::spawn(task::scheduler_ref::of(s), plain_waiter(*m, &w));
		s.run();
		m->unlock();  // hands the lock to the waiter, which has not run
		delete m;
	}));
}
#endif

int main() {
	task::cycles::calibrate();
#if defined(HAVE_FORK) && !defined(NDEBUG)
	test_destructor_asserts();  // before any thread exists
#endif
	test_slice();
	test_event();
	test_mutex();
	test_deadline();
	test_settle_inline();
	test_lock_token();
	test_lock_destroyed();
	test_event_teardown();
	std::printf("task_sync: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
