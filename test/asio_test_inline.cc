// asio_test: the inline resume of an await_asio completion, and where it must stay top-level.
#include "asio_test_support.hh"

void wait_for(const std::atomic<bool>& flag) {
	for (int i = 0; i < 20000 && !flag; ++i) { std::this_thread::sleep_for(100us); }
}

// Inline resume (D6)

using kick = std::function<void()>;

// An init that leaves the handler in `slot` until someone calls it.
auto kick_init(kick* slot) {
	return [slot](auto h) { *slot = [h = std::move(h)]() mutable { std::move(h)(asio::error_code{}); }; };
}

// A chain of links: each resumed link dispatches the next one's handler on the loop with no
// scheduler current (as inside sync_wait's driver), so the next resume is a top-level one.
struct chain {
	asio::io_context& io;
	std::vector<kick> next;
	int stop_at = -1;
	int nest = 0, max_nest = 0, max_depth = 0, done = 0, posted = 0;
};
task::task<void> chain_link(chain& c, int i, std::atomic<int>* destroyed) {
	destroy_mark m{destroyed};
	task::cancel_source src;
	if (i % 2) {
		co_await task::await_asio<void(asio::error_code)>(kick_init(&c.next[std::size_t(i)]), src.token());
	} else {
		co_await task::await_asio<void(asio::error_code)>(kick_init(&c.next[std::size_t(i)]));
	}
	++c.nest;
	c.max_nest = std::max(c.max_nest, c.nest);
	c.max_depth = std::max(c.max_depth, task::detail::inline_resume_depth);
	c.posted += task::detail::inline_resume_depth == 0;
	if (i == c.stop_at) { c.io.stop(); }
	if (std::size_t(i + 1) < c.next.size()) {
		task::running_on none{task::scheduler_ref{}};
		asio::dispatch(c.io, std::move(c.next[std::size_t(i + 1)]));
	}
	--c.nest;
	++c.done;
}

// A budget loop over `slice`: runs until `cap` checkpoints let other loop work run, or `limit`.
task::task<void> budget_loop(std::chrono::nanoseconds slice, std::chrono::nanoseconds limit, int cap, const int* others, int* yields) {
	task::budget b(slice);
	auto until = std::chrono::steady_clock::now() + limit;
	do {
		const bool spent = b.exhausted();
		const int before = *others;
		co_await b.checkpoint();
		if (spent && *others != before) { ++*yields; }
	} while (*yields < cap && std::chrono::steady_clock::now() < until);
}

void test_inline_resume() {
	const auto base = task::detail::live_frames();
	// On the loop's thread with no scheduler current: inline, in the scheduler's scope, with
	// no queue trip (the spawn's post and the timer's handler: 2 io handlers, not 3).
	for (bool token : {false, true}) {
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		int depth = -1;
		bool scoped = false;
		auto wait_timer = [&]() -> task::task<void> {
			asio::steady_timer t(io, 1ms);
			task::cancel_source src;
			auto init = [&](auto h) { t.async_wait(std::move(h)); };
			asio::error_code ec = token ? co_await task::await_asio<void(asio::error_code)>(init, src.token())
			                            : co_await task::await_asio<void(asio::error_code)>(init);
			depth = ec ? -2 : task::detail::inline_resume_depth;
			scoped = task::current() == r;
		};
		task::spawn(r, wait_timer());
		CHECK(io.run() == 2);
		CHECK(depth == 1 && scoped);
	}
	// A loop on its own thread: the continuation runs on that thread, inline (TSan).
	{
		loop l;
		l.start();
		std::atomic<bool> finished{false};
		std::thread::id ran_on;
		int depth = -1;
		auto on_loop = [&]() -> task::task<void> {
			asio::steady_timer t(l.io, 1ms);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			ran_on = std::this_thread::get_id();
			depth = task::detail::inline_resume_depth;
			finished = true;
		};
		task::spawn(l.ref(), on_loop());
		wait_for(finished);
		const auto loop_id = l.thread.get_id();
		l.stop();
		CHECK(finished && ran_on == loop_id && depth == 1);
	}
	// A completion on another loop's thread is posted to the waiting scheduler's loop.
	{
		loop a, b;
		a.start();
		b.start();
		std::atomic<bool> finished{false};
		std::thread::id ran_on;
		int depth = -1;
		auto cross_loop = [&]() -> task::task<void> {
			asio::steady_timer t(b.io, 1ms);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			ran_on = std::this_thread::get_id();
			depth = task::detail::inline_resume_depth;
			finished = true;
		};
		task::spawn(a.ref(), cross_loop());
		wait_for(finished);
		const auto a_id = a.thread.get_id();
		a.stop();
		b.stop();
		CHECK(finished && ran_on == a_id && depth == 0);
	}
	// A handler dispatched by a coroutine, of the same scheduler or another on the same thread,
	// posts its waiter's resume: inline resume is top-level only (D6.1).
	for (bool same : {true, false}) {
		asio::io_context io;
		task::asio_scheduler s1(io.get_executor()), s2(io.get_executor());
		kick k;
		bool waiter_done = false, dispatcher_after = false, saw_dispatcher_after = true;
		int depth = -1;
		auto waiter = [&]() -> task::task<void> {
			co_await task::await_asio<void(asio::error_code)>(kick_init(&k));
			saw_dispatcher_after = dispatcher_after;
			depth = task::detail::inline_resume_depth;
			CHECK(task::current() == task::scheduler_ref::of(s1));
			waiter_done = true;
		};
		task::spawn(task::scheduler_ref::of(s1), waiter());
		io.poll();
		auto dispatcher = [&]() -> task::task<void> {
			asio::dispatch(io, std::move(k));
			dispatcher_after = true;
			co_return;
		};
		task::spawn(task::scheduler_ref::of(same ? s1 : s2), dispatcher());
		io.restart();
		io.run();
		CHECK(waiter_done);
		CHECK(saw_dispatcher_after && depth == 0);
	}
	// A chain of 100 completions, each dispatched by the link the previous one resumed: inline
	// resumes nest at most four deep, then one is posted and the chain goes on from it.
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		chain c{io, std::vector<kick>(100)};
		std::atomic<int> destroyed{0};
		for (int i = 0; i < 100; ++i) { task::spawn(task::scheduler_ref::of(s), chain_link(c, i, &destroyed)); }
		io.poll();
		asio::post(io, [&] { c.next[0](); });
		io.restart();
		io.run();
		CHECK(c.done == 100 && destroyed == 100);
		// Links 0-3 resume inline at depths 1-4; link 4 is posted and starts the next run of
		// five (a posted link, then four inline): links 4, 9, ..., 99 are posted.
		CHECK(c.max_depth == task::detail::inline_resume_bound && c.max_nest == task::detail::inline_resume_bound + 1);
		CHECK(c.posted == 20);
	}
	// An inline resume is a resume: its slice starts fresh, and a budget that spends it yields
	// (probe nocurrent's raw resume never does).
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		int others = 0, yields = 0, fresh_yields = -1;
		bool done = false;
		std::function<void()> tick = [&] {
			if (!done) {
				++others;
				asio::post(io, tick);
			}
		};
		auto spender = [&]() -> task::task<void> {
			asio::steady_timer t(io, 1ms);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			CHECK(task::detail::inline_resume_depth == 1);
			fresh_yields = 0;
			co_await budget_loop(1s, 0ns, 1, &others, &fresh_yields);  // one checkpoint early in a 1 s slice
			co_await budget_loop(200us, 2s, 10, &others, &yields);
			done = true;
		};
		task::spawn(task::scheduler_ref::of(s), spender());
		asio::post(io, tick);
		io.run();
		CHECK(fresh_yields == 0);
		CHECK(yields == 10);
	}
	// Teardown with a chain in flight: the loop stops inside an inline resume, leaving one
	// posted resume queued and later links suspended; the phases destroy them (ASan).
	{
		std::atomic<int> destroyed{0};
		{
			asio::io_context io;
			auto s = std::make_unique<task::asio_scheduler>(io.get_executor());
			chain c{io, std::vector<kick>(12), 6};
			for (int i = 0; i < 12; ++i) { task::spawn(task::scheduler_ref::of(*s), chain_link(c, i, &destroyed)); }
			io.poll();
			asio::post(io, [&] { c.next[0](); });
			io.restart();
			io.run();
			CHECK(c.done == 9 && destroyed == 9);  // links 0-8 ran; link 9's resume was posted
			s->close();
			s->destroy_roots();
			CHECK(destroyed == 12);
		}
		CHECK(destroyed == 12);
	}
	CHECK(task::detail::live_frames() == base);
}

// An operation whose cancellation handler completes it inside the emit (a hand-written
// operation; asio's own post their cancelled completions).
struct sync_op {
	std::function<void(asio::error_code)> h;
	auto init() {
		return [this](auto handler) {
			auto slot = asio::get_associated_cancellation_slot(handler);
			h = [hh = std::move(handler)](asio::error_code ec) mutable { std::move(hh)(ec); };
			if (slot.is_connected()) {
				slot.assign([this](asio::cancellation_type) { complete(asio::error::operation_aborted); });
			}
		};
	}
	void complete(asio::error_code ec) {
		if (auto f = std::exchange(h, nullptr)) { f(ec); }
	}
};

struct sync_seen {
	std::atomic<int> done{0}, inlined{0};
	asio::error_code ec;
	task::scheduler_ref scope;
};
task::task<void> await_sync_op(sync_op& op, task::cancel_token t, sync_seen* seen) {
	seen->ec = co_await task::await_asio<void(asio::error_code)>(op.init(), std::move(t));
	seen->inlined += task::detail::inline_resume_depth > 0;
	seen->scope = task::current();
	++seen->done;
}

// A scheduler whose post_call runs its function as a top-level handler, with no scheduler
// current (D7 does not require a scope there), so a posted emit may resume its frame inline.
struct bare_scheduler {
	asio::io_context& io;
	void post(std::coroutine_handle<> h) {
		asio::post(io, [this, h] {
			task::running_on scope(task::scheduler_ref::of(*this));
			h.resume();
		});
	}
	std::uint64_t now() const noexcept { return 0; }
	bool runs_here() const noexcept { return io.get_executor().running_in_this_thread(); }
	void post_call(void (*fn)(void*), void* arg) {
		asio::post(io, [fn, arg] { fn(arg); });
	}
};

// Two tasks whose operations complete through each other's initiation.
struct pingpong {
	asio::io_context& io;
	std::function<void(asio::error_code)> a, b;
	int rounds = 0, limit = 0, ticks = 0;
};
task::task<void> pingpong_side(pingpong& p, bool is_a) {
	auto& mine = is_a ? p.a : p.b;
	auto& peer = is_a ? p.b : p.a;
	while (p.rounds < p.limit) {
		co_await task::await_asio<void(asio::error_code)>([&](auto h) {
			mine = [h = std::move(h)](asio::error_code ec) mutable { std::move(h)(ec); };
			if (auto f = std::exchange(peer, nullptr)) {
				asio::dispatch(p.io, [f = std::move(f)]() mutable { f(asio::error_code{}); });
			}
		});
		++p.rounds;
	}
	if (auto f = std::exchange(peer, nullptr)) { asio::post(p.io, [f = std::move(f)]() mutable { f(asio::error_code{}); }); }
}

// A coroutine type that is not a task: it starts at once and records no scheduler.
struct eager_co {
	struct promise_type {
		eager_co get_return_object() noexcept { return {}; }
		std::suspend_never initial_suspend() noexcept { return {}; }
		std::suspend_never final_suspend() noexcept { return {}; }
		void return_void() noexcept {}
		void unhandled_exception() noexcept { std::terminate(); }
	};
};
task::task<void> kicked_child(kick* k) { co_await task::await_asio<void(asio::error_code)>(kick_init(k)); }
task::task<void> kick_from_task(asio::io_context& io, kick* k) {
	asio::dispatch(io, std::move(*k));
	co_return;
}
eager_co kick_unscoped(asio::io_context& io, kick* k) { co_await kick_from_task(io, k); }

// Inline resume is top-level only (D6.1): a completion inside a task, a cancel request or a
// scheduler's posted call queues its resume; the inline path keeps the scope on both compilers.
void test_inline_top_level() {
	const auto base = task::detail::live_frames();
	// A cancel requested inside a task on the loop, of an operation that completes inside the
	// emit: the cancelled frame resumes through the queue, never inside request() (CR2).
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		sync_op op;
		task::cancel_source src;
		bool in_request = false;
		int done = 0, inside = -1, depth = -1;
		auto cancelled = [&]() -> task::task<void> {
			auto ec = co_await task::await_asio<void(asio::error_code)>(op.init(), src.token());
			CHECK(ec == asio::error::operation_aborted);
			inside = in_request;
			depth = task::detail::inline_resume_depth;
			++done;
		};
		auto requester = [&]() -> task::task<void> {
			in_request = true;
			src.request();
			in_request = false;
			co_return;
		};
		task::spawn(r, cancelled());
		io.poll();
		task::spawn(r, requester());
		io.restart();
		io.run();
		CHECK(done == 1 && inside == 0 && depth == 0);
	}
	// A cross-thread cancel of that operation: the posted emit completes it and drops the count
	// last, inside asio_scheduler's post_call scope, so the resume is posted (CR2, CR4).
	{
		loop l;
		l.start();
		constexpr int rounds = 2000;
		sync_seen seen;
		for (int i = 0; i < rounds; ++i) {
			sync_op op;
			task::cancel_source src;
			std::atomic<bool> parked{false};
			task::spawn(l.ref(), await_sync_op(op, src.token(), &seen));
			asio::post(l.io, [&] { parked = true; });  // after the frame's first run: it has parked
			wait_for(parked);
			src.request();
			while (seen.done.load() != i + 1) { std::this_thread::yield(); }
			CHECK(seen.ec == asio::error::operation_aborted);
		}
		l.stop();
		CHECK(seen.done == rounds && seen.inlined == 0);
	}
	// The emit drops the count last on a scheduler whose post_call runs it at the top level: it
	// resumes the frame inline, in the scheduler's scope, and touches nothing after (CR4). The
	// operation completes inside the emit, or before the emit runs (the real result wins).
	for (bool completed_first : {false, true}) {
		asio::io_context io;
		bare_scheduler b{io};
		auto br = task::scheduler_ref::of(b);
		for (int i = 0; i < 100; ++i) {
			sync_op op;
			task::cancel_source src;
			sync_seen seen;
			task::spawn(br, await_sync_op(op, src.token(), &seen));
			io.restart();
			io.poll();
			if (completed_first) { asio::post(io, [&] { op.complete(asio::error_code{}); }); }
			src.request();  // off the loop: the emit is posted behind the completion
			io.restart();
			io.run();
			CHECK(seen.done == 1 && seen.inlined == 1 && seen.scope == br);
			if (completed_first) {
				CHECK(!seen.ec);
			} else {
				CHECK(seen.ec == asio::error::operation_aborted);
			}
		}
	}
	// Two tasks in lockstep, each completing the other's operation from its initiation: every
	// round goes through the loop's queue, so other work runs between rounds (CR3).
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		pingpong p{io};
		p.limit = 2000;
		std::function<void()> tick = [&] {
			if (p.rounds < p.limit) {
				++p.ticks;
				asio::post(io, tick);
			}
		};
		task::spawn(task::scheduler_ref::of(s), pingpong_side(p, true));
		task::spawn(task::scheduler_ref::of(s), pingpong_side(p, false));
		asio::post(io, tick);
		io.run();
		std::printf("pingpong: rounds %d, other loop work %d\n", p.rounds, p.ticks);
		CHECK(p.rounds == p.limit + 1 && p.ticks == p.limit);  // the side released last adds a round
	}
	// A task resumed inline inside a coroutine that runs with no scheduler (here a non-task
	// coroutine awaiting a task): its parent continues in the scheduler's scope and keeps it
	// across its next await, under symmetric transfer and under g++'s trampoline (CR1).
	{
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		auto r = task::scheduler_ref::of(s);
		kick k;
		int scoped_after_child = -1, depth_after_child = -1, scoped_after_next = -1;
		auto parent = [&]() -> task::task<void> {
			co_await kicked_child(&k);
			scoped_after_child = task::current() == r;
			depth_after_child = task::detail::inline_resume_depth;
			asio::steady_timer t(io, 1ms);
			co_await task::await_asio<void(asio::error_code)>([&](auto h) { t.async_wait(std::move(h)); });
			scoped_after_next = task::current() == r;
		};
		task::spawn(r, parent());
		io.poll();
		asio::post(io, [&] { kick_unscoped(io, &k); });
		io.restart();
		io.run();
		CHECK(scoped_after_child == 1 && depth_after_child == 1 && scoped_after_next == 1);
	}
	CHECK(task::detail::live_frames() == base);
}
