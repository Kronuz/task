// asio_test: scheduler_ref wrappers (the Host shape) and the teardown of loops, roots and waiters.
#include "asio_test_support.hh"

// Phases 1-3 across every loop.
void teardown(std::initializer_list<loop*> loops) {
	for (auto* l : loops) { l->stop(); }
	for (auto* l : loops) { l->sched->close(); }
	for (auto* l : loops) { l->sched->destroy_roots(); }
}

task::task<void> sleep_on(asio::io_context& io, std::atomic<int>* destroyed, std::atomic<bool>* parked, task::cancel_token t = {}) {
	destroy_mark m{destroyed};
	asio::steady_timer timer(io, 1h);
	*parked = true;
	co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, std::move(t));
}

// Xapiand's Host shape: a virtual base that spawns through scheduler_ref::of(*this) and
// forwards the optional members to the reactor's scheduler (D7).
struct host {
	virtual ~host() = default;
	virtual void post(std::coroutine_handle<> h) = 0;
	virtual std::uint64_t now() const noexcept = 0;
	virtual bool runs_here() const noexcept = 0;
	virtual void post_call(void (*fn)(void*), void* arg) = 0;
	virtual task::root_list* roots() noexcept = 0;
};
struct asio_host final : host {
	explicit asio_host(task::asio_scheduler& s) : s_(s) {}
	void post(std::coroutine_handle<> h) override {
		if (s_.closed()) { return; }
		asio::post(s_.executor(), [this, h] {
			task::running_on scope(task::scheduler_ref::of(static_cast<host&>(*this)));
			h.resume();
		});
	}
	std::uint64_t now() const noexcept override { return s_.now(); }
	bool runs_here() const noexcept override { return s_.runs_here(); }
	void post_call(void (*fn)(void*), void* arg) override { s_.post_call(fn, arg); }
	task::root_list* roots() noexcept override { return s_.roots(); }
	task::asio_scheduler& s_;
};

void test_wrapper() {
	const auto base = task::detail::live_frames();
	loop l;
	asio_host h(*l.sched);
	auto hr = task::scheduler_ref::of(static_cast<host&>(h));
	l.start();
	std::atomic<int> destroyed{0}, done{0};
	std::atomic<bool> parked{false}, parked2{false};
	// A cancel from another thread reaches the operation through the wrapper's post_call.
	task::cancel_source src;
	auto cancelled = [&]() -> task::task<void> {
		co_await sleep_on(l.io, &destroyed, &parked, src.token());
		CHECK(task::current() == hr);
		++done;
	};
	task::spawn(hr, cancelled());
	wait_for(parked);
	std::this_thread::sleep_for(5ms);
	src.request();
	for (int i = 0; i < 2000 && done == 0; ++i) { std::this_thread::sleep_for(1ms); }
	CHECK(done == 1 && destroyed == 1);
	// A completion on the loop resumes a task spawned through the wrapper inline, in its scope.
	{
		std::atomic<bool> finished{false};
		int depth = -1;
		bool scoped = false;
		auto inline_wait = [&]() -> task::task<void> {
			asio::steady_timer t(l.io, 1ms);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			depth = task::detail::inline_resume_depth;
			scoped = task::current() == hr;
			finished = true;
		};
		task::spawn(hr, inline_wait());
		wait_for(finished);
		CHECK(finished && depth == 1 && scoped);
	}
	// A task spawned through the wrapper is a root of the reactor's scheduler.
	task::spawn(hr, sleep_on(l.io, &destroyed, &parked2));
	wait_for(parked2);
	teardown({&l});
	CHECK(destroyed == 2);
	CHECK(task::detail::live_frames() == base + 1);  // src's state
}

struct sets_on_death {
	task::event* e;
	~sets_on_death() { e->set(); }
};
struct spawns_on_death {
	task::scheduler_ref s;
	std::atomic<int>* destroyed;
	std::atomic<int>* done_calls;
	~spawns_on_death() {
		auto late = [](std::unique_ptr<destroy_mark>) -> task::task<void> { co_return; };  // the mark dies with the frame
		std::atomic<int>* calls = done_calls;
		task::spawn(s, late(std::unique_ptr<destroy_mark>(new destroy_mark{destroyed})), [calls](std::exception_ptr) { ++*calls; });
	}
};

void test_teardown() {
	const auto base = task::detail::live_frames();
	// One loop with roots suspended in await_asio (with and without a token), an event, a lock
	// wait, a demoted checkpoint and a when_all; a connection-sized frame (over 2 KiB) counted.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		task::event e;
		task::async_mutex m;
		CHECK(m.try_lock());
		task::cancel_source src;
		std::atomic<int> destroyed{0};
		std::atomic<bool> p1{false}, p2{false};
		task::spawn(r, sleep_on(io, &destroyed, &p1));
		task::spawn(r, sleep_on(io, &destroyed, &p2, src.token()));
		auto on_event = [](task::event& ev, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			co_await ev;
		};
		auto on_lock = [](task::async_mutex& mu, task::cancel_token t, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			auto g = co_await mu.lock(std::move(t));
		};
		auto demoter = [](std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			task::budget b(1ns);
			for (;;) { co_await b.checkpoint(); }
		};
		auto joiner = [](task::event& ev, std::atomic<int>* d) -> task::task<void> {
			auto child = [](task::event& x, std::atomic<int>* dd) -> task::task<int> {
				destroy_mark mk{dd};
				co_await x;
				co_return 1;
			};
			co_await task::when_all(child(ev, d), child(ev, d));
		};
		auto big = [](asio::io_context& c, std::atomic<int>* d) -> task::task<void> {
			destroy_mark mk{d};
			volatile char tmp[64 * 1024];
			tmp[0] = 1;
			asio::steady_timer t(c, 1h);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			tmp[1] = tmp[0];
		};
		task::spawn(r, on_event(e, &destroyed));
		task::spawn(r, on_lock(m, {}, &destroyed));
		task::spawn(r, on_lock(m, src.token(), &destroyed));
		task::spawn(r, demoter(&destroyed));
		task::spawn(r, joiner(e, &destroyed));
		const auto before_big = task::detail::live_frames();
		task::spawn(r, big(io, &destroyed));
		CHECK(task::detail::live_frames() > before_big);
		for (int i = 0; i < 50; ++i) { io.poll_one(); }
		CHECK(s.demoted() == 1);
		io.stop();
		s.close();
		s.destroy_roots();
		CHECK(destroyed == 9);
		CHECK(s.demoted() == 0);
		m.unlock();  // sheds the two waiters the destroyed frames left timed out
		CHECK(m.try_lock());
		m.unlock();
		CHECK(task::detail::live_frames() == base + 1);  // src's state
		e.set();
	}
	// crossroot: a root on A suspended on B's timer.
	{
		loop a, b;
		a.start();
		b.start();
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false};
		auto cross = [](loop& to, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
			co_await task::schedule_on{to.ref()};
			co_await sleep_on(to.io, d, p);
		};
		task::spawn(a.ref(), cross(b, &destroyed, &parked));
		wait_for(parked);
		teardown({&a, &b});
		CHECK(destroyed == 1);
	}
	// holder: a guard held by a root on A, a waiter rooted on B; destroyed in both orders. The
	// holder's unlock grants the waiter, whose wake B drops; the waiter's destructor unlocks.
	for (bool a_first : {true, false}) {
		task::async_mutex m;  // owned outside every root
		{
			loop a, b;
			a.start();
			b.start();
			std::atomic<int> destroyed{0};
			std::atomic<bool> held{false}, queued{false};
			auto holder = [](task::async_mutex& mu, loop& l, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
				auto g = co_await mu.lock();
				co_await sleep_on(l.io, d, p);
			};
			auto waiter = [](task::async_mutex& mu, std::atomic<int>* d, std::atomic<bool>* q) -> task::task<void> {
				destroy_mark mk{d};
				*q = true;
				auto g = co_await mu.lock();
			};
			task::spawn(a.ref(), holder(m, a, &destroyed, &held));
			wait_for(held);
			task::spawn(b.ref(), waiter(m, &destroyed, &queued));
			wait_for(queued);
			std::this_thread::sleep_for(5ms);
			a.stop();
			b.stop();
			a.sched->close();
			b.sched->close();
			if (a_first) {
				a.sched->destroy_roots();
				b.sched->destroy_roots();
			} else {
				b.sched->destroy_roots();
				a.sched->destroy_roots();
			}
			CHECK(destroyed == 2);
		}
		CHECK(m.try_lock());
		m.unlock();
	}
	// An event set by a dying frame on A, its waiter rooted on B; both orders.
	for (bool a_first : {true, false}) {
		task::event e;
		loop a, b;
		a.start();
		b.start();
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false}, waiting{false};
		auto setter = [](task::event& ev, loop& l, std::atomic<int>* d, std::atomic<bool>* p) -> task::task<void> {
			sets_on_death s{&ev};
			co_await sleep_on(l.io, d, p);
		};
		auto waiter = [](task::event& ev, std::atomic<int>* d, std::atomic<bool>* w) -> task::task<void> {
			destroy_mark mk{d};
			*w = true;
			co_await ev;
		};
		task::spawn(a.ref(), setter(e, a, &destroyed, &parked));
		task::spawn(b.ref(), waiter(e, &destroyed, &waiting));
		wait_for(parked);
		wait_for(waiting);
		std::this_thread::sleep_for(5ms);
		a.stop();
		b.stop();
		a.sched->close();
		b.sched->close();
		if (a_first) {
			a.sched->destroy_roots();
			b.sched->destroy_roots();
		} else {
			b.sched->destroy_roots();
			a.sched->destroy_roots();
		}
		CHECK(destroyed == 2 && e.is_set());
	}
	// A destructor that spawns during phase 3: the task is destroyed at once, unrun, and its
	// done is never called; a frame destroyed with a cross-thread emit pending.
	{
		loop l;
		l.start();
		std::atomic<int> destroyed{0}, late{0}, done_calls{0};
		std::atomic<bool> parked{false}, parked2{false};
		auto spawner = [](loop& lp, std::atomic<int>* d, std::atomic<int>* lt, std::atomic<int>* calls, std::atomic<bool>* p) -> task::task<void> {
			spawns_on_death sp{lp.ref(), lt, calls};
			co_await sleep_on(lp.io, d, p);
		};
		task::cancel_source src;
		task::spawn(l.ref(), spawner(l, &destroyed, &late, &done_calls, &parked));
		task::spawn(l.ref(), sleep_on(l.io, &destroyed, &parked2, src.token()));
		wait_for(parked);
		wait_for(parked2);
		l.stop();
		src.request();  // off the loop: the emit is posted to the stopped loop
		l.sched->close();
		l.sched->destroy_roots();
		CHECK(destroyed == 2 && late == 1 && done_calls == 0);
		CHECK(l.sched->roots()->empty());
	}
	// spawn onto a closed scheduler destroys the task before returning; a blocking caller whose
	// done owns a promise returns (broken_promise) instead of hanging. post, post_demoted and
	// post_call drop.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		s.close();
		std::atomic<int> destroyed{0};
		auto quick = [](std::unique_ptr<destroy_mark>) -> task::task<void> { co_return; };
		std::promise<void> p;
		auto f = p.get_future();
		task::spawn(task::scheduler_ref::of(s), quick(std::unique_ptr<destroy_mark>(new destroy_mark{&destroyed})), [p = std::move(p)](std::exception_ptr) mutable { p.set_value(); });
		CHECK(destroyed == 1);
		bool broken = false;
		try {
			f.get();
		} catch (const std::future_error& err) {
			broken = err.code() == std::future_errc::broken_promise;
		}
		CHECK(broken);
		s.post(std::noop_coroutine());
		s.post_demoted(std::noop_coroutine());
		s.post_call([](void*) {}, nullptr);
		CHECK(io.poll() == 0);
		s.close();  // idempotent
		s.destroy_roots();
		s.destroy_roots();
	}
	// The io_context run again after phase 3 (io.restart(); io.poll()): a resume or a cancel emit
	// queued before close() is dropped, not run against a destroyed frame.
	for (bool emit : {false, true}) {
		std::atomic<int> destroyed{0};
		asio::io_context io;
		asio::steady_timer timer(io, 1h);  // outside the frame: its pending operation outlives it
		{
			task::asio_scheduler s(io.get_executor());
			task::event ev;
			task::cancel_source src;
			auto frame = [&]() -> task::task<void> {
				destroy_mark mark{&destroyed};
				if (emit) {
					co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, src.token());
				} else {
					co_await ev;
				}
			};
			task::spawn(task::scheduler_ref::of(s), frame());
			io.poll();
			if (emit) {
				std::thread([&] { src.request(); }).join();  // queues the emit, which has not run
			} else {
				ev.set();  // queues the resume
			}
			io.stop();
			s.close();
			s.destroy_roots();
			CHECK(destroyed == 1);
			io.restart();
			CHECK(io.poll() >= 1);  // the queued handler runs, and does nothing
		}
		CHECK(task::detail::live_frames() == base);
	}
	// The destructor runs the phases itself when the owner did not (a stopped loop).
	{
		std::atomic<int> destroyed{0};
		std::atomic<bool> parked{false};
		asio::io_context io;
		{
			task::asio_scheduler s(io.get_executor());
			task::spawn(task::scheduler_ref::of(s), sleep_on(io, &destroyed, &parked));
			io.poll();
			io.stop();
		}
		CHECK(destroyed == 1);
	}
	CHECK(task::detail::live_frames() == base);
}
