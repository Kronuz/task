// task: the per-resume slice, event, and async_mutex with lock_until, including both
// orders of the lock_until race (timeout first, grant first), the await_suspend handshake,
// threads (TSan), and the destructor asserts.
#include "task.hh"

#include <atomic>
#include <chrono>
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

static int failures = 0;
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
// runs inside start_timer, to interleave another party with a waiter's await_suspend.
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
	std::function<void(std::size_t)> on_start;
	std::chrono::steady_clock::time_point last_deadline;

private:
	std::deque<std::coroutine_handle<>> ready_;
	std::map<std::size_t, std::unique_ptr<callback_base>> pending_;
	std::vector<std::unique_ptr<callback_base>> queued_;
	std::size_t next_id_ = 0;
};

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

// ---------------------------------------------------------------------------------------------
// async_mutex

task::task<void> critical(task::async_mutex& m, std::vector<std::string>* t, std::string name, int yields) {
	auto g = co_await m.lock();
	t->push_back(name + ":in");
	for (int i = 0; i < yields; ++i) { co_await task::yield(); }
	t->push_back(name + ":out");
}
task::task<void> twice(task::async_mutex& m, std::vector<std::string>* t) {
	{
		auto g = co_await m.lock();
		t->push_back("A1");
		co_await task::yield();  // B runs and queues
	}
	auto g = co_await m.lock();  // the lock went to B: this queues behind it
	t->push_back("A2");
}
task::task<void> once(task::async_mutex& m, std::vector<std::string>* t, const char* name) {
	auto g = co_await m.lock();
	t->push_back(name);
}
task::task<void> unlock_and_note(task::async_mutex::guard g, std::vector<std::string>* t) {
	g.unlock();
	t->push_back("unlocked");
	co_return;
}

void test_mutex() {
	{
		task::async_mutex m;
		CHECK(m.try_lock());
		CHECK(!m.try_lock());
		m.unlock();
		CHECK(m.try_lock());
		m.unlock();
	}
	{  // FIFO, mutual exclusion across yields.
		task::inline_scheduler s;
		auto r = task::scheduler_ref::of(s);
		task::async_mutex m;
		std::vector<std::string> t;
		task::spawn(r, critical(m, &t, "A", 2));
		task::spawn(r, critical(m, &t, "B", 2));
		task::spawn(r, critical(m, &t, "C", 0));
		s.run();
		CHECK((t == std::vector<std::string>{"A:in", "A:out", "B:in", "B:out", "C:in", "C:out"}));
	}
	{  // Handoff keeps the lock: the unlocker cannot barge back in.
		task::inline_scheduler s;
		auto r = task::scheduler_ref::of(s);
		task::async_mutex m;
		std::vector<std::string> t;
		task::spawn(r, twice(m, &t));
		task::spawn(r, once(m, &t, "B"));
		s.run();
		CHECK((t == std::vector<std::string>{"A1", "B", "A2"}));
	}
	{  // A waiter resumes through its own scheduler, never inside unlock().
		task::inline_scheduler a, b;
		task::async_mutex m;
		CHECK(m.try_lock());
		std::vector<std::string> t;
		task::spawn(task::scheduler_ref::of(a), once(m, &t, "waiter"));
		a.run();
		CHECK(t.empty());
		task::spawn(task::scheduler_ref::of(b), unlock_and_note(task::async_mutex::guard{}, &t));  // empty guard: no-op
		b.run();
		m.unlock();  // called from outside any scheduler
		CHECK((t == std::vector<std::string>{"unlocked"}));
		CHECK(a.queued() == 1);
		a.run();
		CHECK((t == std::vector<std::string>{"unlocked", "waiter"}));
		CHECK(m.try_lock());  // the waiter's guard released it
		m.unlock();
	}
	{  // Many threads, several schedulers: mutual exclusion and no lost wakeup.
		thread_scheduler w[4];
		task::async_mutex m;
		long counter = 0;
		std::atomic<int> inside{0}, done{0};
		std::atomic<bool> overlapped{false};
		auto worker = [](task::async_mutex& mu, long* c, std::atomic<int>* in, std::atomic<bool>* bad, std::atomic<int>* d) -> task::task<void> {
			for (int i = 0; i < 200; ++i) {
				auto g = co_await mu.lock();
				if (in->fetch_add(1) != 0) { *bad = true; }
				++*c;
				if (i % 3 == 0) { co_await task::yield(); }
				in->fetch_sub(1);
			}
			++*d;
		};
		std::vector<std::thread> spawners;
		for (int t = 0; t < 4; ++t) {
			spawners.emplace_back([&, t] {
				for (int i = 0; i < 8; ++i) { task::spawn(task::scheduler_ref::of(w[(t + i) % 4]), worker(m, &counter, &inside, &overlapped, &done)); }
			});
		}
		for (auto& t : spawners) { t.join(); }
		for (int i = 0; i < 20000 && done.load() < 32; ++i) { std::this_thread::sleep_for(1ms); }
		CHECK(done.load() == 32);
		CHECK(!overlapped.load());
		CHECK(counter == 32 * 200);
	}
}

// ---------------------------------------------------------------------------------------------
// lock_until: both orders of the grant/timeout race, and the await_suspend handshake

struct probe_result {
	int got = 0;      // 0 pending, 1 holds the lock, 2 timed out
	int resumed = 0;  // times the coroutine ran past its lock_for
};
task::task<void> timed_waiter(task::async_mutex& m, manual_scheduler& s, probe_result* out) {
	auto g = co_await m.lock_for(s, 1000ns);
	++out->resumed;
	out->got = g ? 1 : 2;
}
task::task<void> plain_waiter(task::async_mutex& m, probe_result* out) {
	auto g = co_await m.lock();
	++out->resumed;
	out->got = 1;
}

void test_lock_until() {
	// A: the timeout wins, the waiter's frame ends, then the holder unlocks and walks the queue.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w));
		ms.run();
		CHECK(w.resumed == 0 && ms.pending() == 1);
		ms.fire(0);
		CHECK(w.resumed == 0);  // the waiter is posted home, not run inside the timer
		ms.run();
		CHECK(w.got == 2 && w.resumed == 1);
		m.unlock();  // reaches the dead entry
		CHECK(m.try_lock());
		m.unlock();
	}
	// B: the grant wins while the timer's completion is already queued; the frame ends first.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w));
		ms.run();
		ms.expire(0);
		m.unlock();
		ms.run();
		CHECK(w.got == 1 && w.resumed == 1);
		ms.run_queued();  // the late timer meets a settled waiter
		CHECK(w.got == 1 && w.resumed == 1);
		CHECK(m.try_lock());
		m.unlock();
	}
	// C: a lock is never granted to a waiter that timed out; the next waiter gets it.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w1, w2;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w1));
		task::spawn(ms.ref(), plain_waiter(m, &w2));
		ms.run();
		ms.fire(0);
		ms.run();
		CHECK(w1.got == 2 && w2.got == 0);
		m.unlock();
		ms.run();
		CHECK(w1.got == 2 && w1.resumed == 1);
		CHECK(w2.got == 1 && w2.resumed == 1);
		CHECK(m.try_lock());
		m.unlock();
	}
	// D: the timer is cancelled by a grant; its callback is destroyed unrun and nothing leaks.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w));
		ms.run();
		m.unlock();
		ms.run();
		CHECK(w.got == 1 && ms.pending() == 0);
	}
	// E, F: the other party settles the waiter inside start_timer, before await_suspend
	// returns. The coroutine continues in place, once, whether post() queues or resumes.
	for (bool immediate : {false, true}) {
		{  // E: the deadline passes first
			manual_scheduler ms;
			ms.immediate = immediate;
			task::async_mutex m;
			CHECK(m.try_lock());
			probe_result w;
			ms.on_start = [&](std::size_t id) { ms.fire(id); };
			task::spawn(ms.ref(), timed_waiter(m, ms, &w));
			ms.run();
			CHECK(w.got == 2 && w.resumed == 1);
			m.unlock();
			CHECK(m.try_lock());
			m.unlock();
		}
		{  // F: the holder unlocks first
			manual_scheduler ms;
			ms.immediate = immediate;
			task::async_mutex m;
			CHECK(m.try_lock());
			probe_result w;
			ms.on_start = [&](std::size_t) { m.unlock(); };
			task::spawn(ms.ref(), timed_waiter(m, ms, &w));
			ms.run();
			CHECK(w.got == 1 && w.resumed == 1);
			CHECK(m.try_lock());
			m.unlock();
		}
	}
	// G: a free lock is taken without a timer, whatever the deadline.
	{
		manual_scheduler ms;
		task::async_mutex m;
		probe_result w;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w));
		ms.run();
		CHECK(w.got == 1 && ms.pending() == 0);
	}
	// H: a timer that cannot start leaves the mutex usable and the waiter dead.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		bool threw = false;
		auto t = [](task::async_mutex& mu, manual_scheduler& s, bool* threw) -> task::task<void> {
			try {
				auto g = co_await mu.lock_for(s, 1000ns);
			} catch (const std::runtime_error&) {
				*threw = true;
			}
		};
		ms.fail_start = true;
		task::spawn(ms.ref(), t(m, ms, &threw));
		ms.run();
		CHECK(threw);
		m.unlock();
		CHECK(m.try_lock());
		m.unlock();
	}
	// I: lock_for adds the timeout to the scheduler's clock.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), timed_waiter(m, ms, &w));
		ms.run();
		CHECK(ms.last_deadline.time_since_epoch() == 1000ns);  // manual_scheduler's now() is 0
		ms.fire(0);
		ms.run();
		m.unlock();
	}
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
	CHECK(aborts([] {  // an event with a waiter
		task::inline_scheduler s;
		auto* e = new task::event;
		std::vector<int> order;
		task::spawn(task::scheduler_ref::of(s), wait_then_record(*e, &order, 1));
		s.run();
		delete e;
	}));
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
	test_lock_until();
	std::printf("task_sync: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
