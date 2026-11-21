// sync_test: async_mutex, deadline, lock(token) and a frame destroyed while it waits for the lock.
#include "sync_test_support.hh"

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
	{  // A waiter with no scheduler (under sync_wait) resumes on the unlocking thread.
		task::async_mutex m;
		CHECK(m.try_lock());
		std::thread unlocker([&] {
			std::this_thread::sleep_for(5ms);
			m.unlock();
		});
		auto locked = [](task::async_mutex& mu) -> task::task<int> {
			auto g = co_await mu.lock();
			co_return 1;
		};
		CHECK(task::sync_wait(locked(m)) == 1);
		unlocker.join();
		CHECK(m.try_lock());
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
// deadline (D4): a timer that requests a token, armed until destroyed

using time_point = std::chrono::steady_clock::time_point;

void test_deadline() {
	using task::cancel_reason;
	const auto base = task::detail::live_frames();
	// It requests cancel_reason::deadline when its timer fires.
	{
		manual_scheduler ms;
		task::deadline dl(ms, time_point{1000ns});
		CHECK(ms.pending() == 1 && ms.last_deadline == time_point{1000ns});
		CHECK(!dl.token().requested() && !dl.expired());
		ms.fire(0);
		CHECK(dl.expired() && dl.token().reason() == cancel_reason::deadline);
	}
	// Destroyed first, it never requests: its callback is destroyed unrun.
	{
		manual_scheduler ms;
		task::cancel_token t;
		{
			task::deadline dl(ms, time_point{1000ns});
			t = dl.token();
		}
		CHECK(ms.pending() == 0 && !t.requested());
	}
	// Destroyed after its expiry was queued (the expiry still runs, as Asio's does), it never
	// requests: the disarm bit stops the late callback.
	{
		manual_scheduler ms;
		task::cancel_token t;
		{
			task::deadline dl(ms, time_point{1000ns});
			t = dl.token();
			ms.expire(0);
		}
		ms.run_queued();
		CHECK(!t.requested() && t.reason() == cancel_reason::none);
	}
	// Linked to a parent: the parent's request reaches it with the parent's reason, and a
	// later expiry does not change it.
	{
		manual_scheduler ms;
		task::cancel_source parent;
		task::deadline dl(ms, time_point{1000ns}, parent.token());
		CHECK(parent.request(cancel_reason::shutdown));
		CHECK(dl.token().reason() == cancel_reason::shutdown && !dl.expired());
		ms.fire(0);
		CHECK(dl.token().reason() == cancel_reason::shutdown);
	}
	// Its expiry does not reach the parent.
	{
		manual_scheduler ms;
		task::cancel_source parent;
		task::deadline dl(ms, time_point{1000ns}, parent.token());
		ms.fire(0);
		CHECK(dl.expired() && !parent.requested());
	}
	// A timer that cannot start: the constructor throws and frees the state.
	{
		manual_scheduler ms;
		ms.fail_start = true;
		bool threw = false;
		try {
			task::deadline dl(ms, time_point{1000ns});
		} catch (const std::runtime_error&) {
			threw = true;
		}
		CHECK(threw);
	}
	CHECK(task::detail::live_frames() == base);
}

// A deadline's callback that advances its own inline_scheduler past the deadline: the wheel's fire
// loses to the request this thread is delivering, and settle() must not wait for that thread.
// A watchdog turns the hang a self-wait would be into a failure.
void test_settle_inline() {
	using task::cancel_reason;
	std::atomic<bool> done{false};
	std::thread watchdog([&] {
		for (int i = 0; i < 500 && !done.load(); ++i) { std::this_thread::sleep_for(10ms); }
		if (!done.load()) {
			std::fprintf(stderr, "%s:%d: settle waits on its own thread\n", __FILE__, __LINE__);
			std::_Exit(EXIT_FAILURE);
		}
	});
	{
		task::inline_scheduler s;
		task::cancel_source parent;
		task::deadline dl(s, task::inline_scheduler::clock::time_point(5ms), parent.token());
		int calls = 0;
		task::cancel_callback on_cancel(dl.token(), [&] {
			++calls;
			s.advance(10'000'000);  // the deadline's entry is due: it fires inside this delivery
			CHECK(s.armed_timers() == 0);
		});
		CHECK(s.armed_timers() == 1);
		CHECK(parent.request(cancel_reason::requested));
		CHECK(calls == 1 && s.armed_timers() == 0);
		CHECK(dl.token().reason() == cancel_reason::requested && !dl.expired());
	}
	done = true;
	watchdog.join();
}

// ---------------------------------------------------------------------------------------------
// lock(token): both orders of the grant/cancel race, and the await_suspend handshake

task::task<void> deadline_waiter(task::async_mutex& m, manual_scheduler& s, probe_result* out, task::cancel_token* token = nullptr) {
	task::deadline dl(s, time_point{1000ns});
	if (token) { *token = dl.token(); }
	auto g = co_await m.lock(dl.token());
	++out->resumed;
	out->got = g ? 1 : 2;
}
task::task<void> token_waiter(task::async_mutex& m, task::cancel_token t, probe_result* out) {
	auto g = co_await m.lock(std::move(t));
	++out->resumed;
	out->got = g ? 1 : 2;
}
task::task<void> plain_waiter(task::async_mutex& m, probe_result* out) {
	auto g = co_await m.lock();
	++out->resumed;
	out->got = 1;
}

// What the after_enqueue hook does: request a source, or unlock a mutex.
task::cancel_source* hook_source = nullptr;
task::async_mutex* hook_mutex = nullptr;
void hook_request() { hook_source->request(); }
void hook_unlock() { hook_mutex->unlock(); }

void test_lock_token() {
	const auto base = task::detail::live_frames();
	// A: the deadline wins, the waiter's frame ends, then the holder unlocks and walks the queue.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), deadline_waiter(m, ms, &w));
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
	// B: the grant wins while the timer's expiry is already queued; the frame ends first, and
	// the late expiry neither requests the token nor wakes anything.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::cancel_token t;
		task::spawn(ms.ref(), deadline_waiter(m, ms, &w, &t));
		ms.run();
		ms.expire(0);
		m.unlock();
		ms.run();
		CHECK(w.got == 1 && w.resumed == 1);
		ms.run_queued();
		CHECK(ms.ready() == 0 && !t.requested());
		ms.run();
		CHECK(w.got == 1 && w.resumed == 1);
		CHECK(m.try_lock());
		m.unlock();
	}
	// C: a lock is never granted to a waiter that was cancelled; the next waiter gets it.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w1, w2;
		task::spawn(ms.ref(), deadline_waiter(m, ms, &w1));
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
	// D: a grant ends the frame, which destroys the deadline and its timer unrun.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), deadline_waiter(m, ms, &w));
		ms.run();
		m.unlock();
		ms.run();
		CHECK(w.got == 1 && ms.pending() == 0);
	}
	// E, F: the other party acts after the waiter is queued and before its callback is
	// registered. The coroutine continues in place, once, whether post() queues or resumes:
	// spawn's is the only post.
	for (bool immediate : {false, true}) {
		{  // E: the token is requested first
			manual_scheduler ms;
			ms.immediate = immediate;
			task::async_mutex m;
			CHECK(m.try_lock());
			task::cancel_source source;
			probe_result w;
			hook_source = &source;
			task::detail::test_hooks::after_enqueue = hook_request;
			task::spawn(ms.ref(), token_waiter(m, source.token(), &w));
			ms.run();
			task::detail::test_hooks::after_enqueue = nullptr;
			CHECK(w.got == 2 && w.resumed == 1 && ms.posts == 1);
			m.unlock();
			CHECK(m.try_lock());
			m.unlock();
		}
		{  // F: the holder unlocks first
			manual_scheduler ms;
			ms.immediate = immediate;
			task::async_mutex m;
			CHECK(m.try_lock());
			task::cancel_source source;
			probe_result w;
			hook_mutex = &m;
			task::detail::test_hooks::after_enqueue = hook_unlock;
			task::spawn(ms.ref(), token_waiter(m, source.token(), &w));
			ms.run();
			task::detail::test_hooks::after_enqueue = nullptr;
			CHECK(w.got == 1 && w.resumed == 1 && ms.posts == 1);
			CHECK(m.try_lock());
			m.unlock();
		}
	}
	// G: a free lock is taken whatever the token, requested or not.
	{
		manual_scheduler ms;
		task::async_mutex m;
		probe_result w1, w2;
		task::spawn(ms.ref(), deadline_waiter(m, ms, &w1));
		ms.run();
		CHECK(w1.got == 1 && ms.pending() == 0);
		task::cancel_source source;
		source.request();
		task::spawn(ms.ref(), token_waiter(m, source.token(), &w2));
		ms.run();
		CHECK(w2.got == 1);
		CHECK(m.try_lock());  // released when the waiter ended
		m.unlock();
	}
	// H: a held lock with a requested token returns an empty guard without queueing.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		source.request();
		probe_result w;
		const auto before = task::detail::live_frames();
		task::spawn(ms.ref(), token_waiter(m, source.token(), &w));
		ms.run();
		CHECK(w.got == 2 && w.resumed == 1 && ms.posts == 1);
		CHECK(task::detail::live_frames() == before);  // no lock_waiter left in the queue
		m.unlock();
	}
	// I: a request while queued, from this thread outside the scheduler: the waiter is posted
	// home, not resumed inside request().
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		task::spawn(ms.ref(), token_waiter(m, source.token(), &w));
		ms.run();
		CHECK(source.request());
		CHECK(w.resumed == 0 && ms.ready() == 1);
		ms.run();
		CHECK(w.got == 2 && w.resumed == 1);
		m.unlock();
		CHECK(m.try_lock());
		m.unlock();
	}
	// J: an empty token is lock(): it waits for the grant.
	{
		manual_scheduler ms;
		task::async_mutex m;
		CHECK(m.try_lock());
		probe_result w;
		task::spawn(ms.ref(), token_waiter(m, task::cancel_token{}, &w));
		ms.run();
		CHECK(w.resumed == 0);
		m.unlock();
		ms.run();
		CHECK(w.got == 1);
	}
	// K: requests from another thread race the waiter on its own thread at every point of the
	// await (before, between enqueue and registration, while queued) (TSan).
	{
		thread_scheduler worker;
		task::async_mutex m;
		CHECK(m.try_lock());
		constexpr int rounds = 2000;
		std::atomic<int> done{0}, cancelled{0};
		for (int i = 0; i < rounds; ++i) {
			task::cancel_source source;
			auto waiter = [](task::async_mutex& mu, task::cancel_token t, std::atomic<int>* d, std::atomic<int>* c) -> task::task<void> {
				auto g = co_await mu.lock(std::move(t));
				if (!g) { c->fetch_add(1); }
				d->fetch_add(1);
			};
			task::spawn(task::scheduler_ref::of(worker), waiter(m, source.token(), &done, &cancelled));
			if (i % 3) { std::this_thread::yield(); }
			source.request();
		}
		for (int i = 0; i < 20000 && done.load() < rounds; ++i) { std::this_thread::sleep_for(1ms); }
		CHECK(done.load() == rounds && cancelled.load() == rounds);
		m.unlock();
		CHECK(m.try_lock());
		m.unlock();
	}
	CHECK(task::detail::live_frames() == base);
}

// ---------------------------------------------------------------------------------------------
// A frame destroyed while it waits for the lock (D8.4)

void test_lock_destroyed() {
	const auto base = task::detail::live_frames();
	// grant_granted: granted and its resume posted, then destroyed unrun: the lock passes on.
	for (bool with_token : {false, true}) {
		task::inline_scheduler s;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		auto t = with_token ? token_waiter(m, source.token(), &w) : plain_waiter(m, &w);
		s.post(t.handle());
		s.run();  // queued
		m.unlock();  // granted; the resume is posted and never runs
		CHECK(s.queued() == 1);
		t = {};
		CHECK(w.resumed == 0);
		CHECK(m.try_lock());
		m.unlock();
	}
	// grant_timedout: a token-bound waiter cancelled, then destroyed unrun: the holder keeps the lock.
	{
		task::inline_scheduler s;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		auto t = token_waiter(m, source.token(), &w);
		s.post(t.handle());
		s.run();
		CHECK(source.request());  // timed_out; the resume is posted and never runs
		t = {};
		CHECK(!m.try_lock());
		m.unlock();
		CHECK(m.try_lock());
		m.unlock();
	}
	// grant_then_request: granted (resume posted), then the token is requested before the resume
	// runs: the callback finds the waiter settled, posts nothing, and the waiter resumes once.
	{
		task::inline_scheduler s;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		auto t = token_waiter(m, source.token(), &w);
		s.post(t.handle());
		s.run();
		m.unlock();
		CHECK(s.queued() == 1);
		CHECK(source.request());
		CHECK(s.queued() == 1);
		s.run();
		CHECK(w.resumed == 1 && w.got == 1);
		CHECK(m.try_lock());  // the guard died with the frame
		m.unlock();
	}
	// grant_waiting: destroyed while still queued: the holder's unlock skips it and frees the lock.
	for (bool with_token : {false, true}) {
		task::inline_scheduler s;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		auto t = with_token ? token_waiter(m, source.token(), &w) : plain_waiter(m, &w);
		s.post(t.handle());
		s.run();
		t = {};
		CHECK(source.request() && s.queued() == 0);  // a destroyed waiter's callback is gone
		m.unlock();
		CHECK(s.queued() == 0);
		CHECK(m.try_lock());
		m.unlock();
	}
	// X3 (C1 review): the frame is destroyed while its cancel callback runs on another thread,
	// between settle and wake. The destructor deregisters (waiting for the callback) before it
	// gives up its reference, so the callback's wake never meets a freed waiter even when the
	// holder's unlock sheds the entry meanwhile.
	{
		task::inline_scheduler s;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source source;
		probe_result w;
		auto t = token_waiter(m, source.token(), &w);
		s.post(t.handle());
		s.run();  // queued
		static std::atomic<bool> in_callback;
		static task::async_mutex* holder;
		in_callback = false;
		holder = &m;
		std::thread requester([&] {
			task::detail::test_hooks::cancel_settled = [] {
				in_callback = true;
				std::this_thread::sleep_for(std::chrono::milliseconds(50));  // the frame dies meanwhile
				holder->unlock();  // sheds the timed-out entry
			};
			source.request();
			task::detail::test_hooks::cancel_settled = nullptr;
		});
		while (!in_callback) { std::this_thread::yield(); }
		t = {};  // waits for the callback to return
		requester.join();
		CHECK(w.resumed == 0);
		CHECK(s.queued() == 1);  // the wake posted the dead handle; a closed scheduler drops it
		CHECK(m.try_lock());
		m.unlock();
	}
	CHECK(task::detail::live_frames() == base);
}

