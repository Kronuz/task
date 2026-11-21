// asio_test: await_asio with a token, and with a completion that cannot be default-constructed.
#include "asio_test_support.hh"

task::task<void> read_with(sock& s, task::cancel_token t, asio::error_code* ec, std::size_t* n, std::atomic<int>* done) {
	char b[8];
	std::tie(*ec, *n) = co_await task::await_asio<read_sig>([&](auto h) { s.async_read_some(asio::buffer(b), std::move(h)); }, std::move(t));
	++*done;
}

// A request made on the loop's thread inside init: before the operation starts or after. The
// emit has to wait until the slot holds the operation (D5.3), so the await ends aborted.
task::task<void> request_in_init(asio::steady_timer& timer, task::cancel_source& src, bool before, bool posted, asio::error_code* ec,
	std::atomic<int>* done) {
	auto request = [&] {
		if (posted) {
			std::thread([&] { src.request(); }).join();  // from another thread: a posted emit
		} else {
			src.request();  // on the loop: pending until the operation is in the slot
		}
	};
	*ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) {
		if (before) { request(); }
		timer.async_wait(std::move(h));
		if (!before) { request(); }
	}, src.token());
	++*done;
}

// A cross-thread request that races the operation's own completion. kind 0: a timer; 1: the
// handler runs inside init through a dispatch; 2: init throws.
struct race_stats {
	std::atomic<int> aborted{0}, ok{0}, thrown{0}, other{0}, finished{0}, inlined{0};
};
task::task<void> race_one(asio::io_context& io, task::cancel_token t, int kind, race_stats* st) {
	try {
		asio::steady_timer timer(io, std::chrono::microseconds(100 + (kind % 8) * 50));
		asio::error_code ec;
		if (kind % 3 == 1) {
			ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) {
				asio::dispatch(io, [h = std::move(h)]() mutable { std::move(h)(asio::error_code{}); });
			}, t);
		} else if (kind % 3 == 2) {
			co_await task::await_asio<void(asio::error_code)>([&](auto) { throw std::runtime_error("init"); }, t);
		} else {
			ec = co_await task::await_asio<void(asio::error_code)>([&](auto h) { timer.async_wait(std::move(h)); }, t);
		}
		st->inlined += task::detail::inline_resume_depth > 0;  // the handler or the emit resumed it inline
		if (!ec) {
			++st->ok;
		} else if (ec == asio::error::operation_aborted) {
			++st->aborted;
		} else {
			++st->other;
		}
	} catch (const std::runtime_error&) {
		++st->thrown;
	}
	++st->finished;
}

void test_await_token() {
	const auto base = task::detail::live_frames();
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	auto r = task::scheduler_ref::of(s);
	// A requested token never calls init: before the await, and between registration and the
	// re-check, on the loop or from another thread (NF1: the posted emit outlives nothing).
	for (int mode = 0; mode < 3; ++mode) {
		task::cancel_source src;
		if (mode == 0) { src.request(); }
		bool called = false;
		asio::error_code ec;
		std::size_t n = 7;
		std::atomic<int> done{0};
		auto body = [&]() -> task::task<void> {
			std::tie(ec, n) = co_await task::await_asio<read_sig>([&](auto) { called = true; }, src.token());
			++done;
		};
		static task::cancel_source* hooked;
		hooked = &src;
		if (mode == 1) { task::detail::test_hooks::asio_registered = [] { hooked->request(); }; }
		if (mode == 2) { task::detail::test_hooks::asio_registered = [] { std::thread([] { hooked->request(); }).join(); }; }
		task::spawn(r, body());
		io.restart();
		io.run();
		task::detail::test_hooks::asio_registered = nullptr;
		CHECK(!called && done == 1 && ec == asio::error::operation_aborted && n == 0);
	}
	// init throws after a request from another thread posted its emit: the exception is
	// rethrown once the emit has run, and without a request at once.
	for (bool request : {false, true}) {
		task::cancel_source src;
		bool caught = false;
		auto body = [&]() -> task::task<void> {
			try {
				co_await task::await_asio<void(asio::error_code)>([&](auto) {
					if (request) { std::thread([&] { src.request(); }).join(); }
					throw std::runtime_error("init");
				}, src.token());
			} catch (const std::runtime_error&) {
				caught = true;
			}
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(caught);
	}
	// A request inside init, before and after the operation starts, on the loop's thread (emitted
	// once init returns) and from another thread (a posted emit): the operation ends aborted.
	for (int mode = 0; mode < 4; ++mode) {
		task::cancel_source src;
		asio::steady_timer timer(io, 1h);
		asio::error_code ec;
		std::atomic<int> done{0};
		task::spawn(r, request_in_init(timer, src, mode & 1, mode & 2, &ec, &done));
		io.restart();
		io.run_for(10s);
		CHECK(done == 1 && ec == asio::error::operation_aborted);
	}
	// A handler run inside init (a dispatch on the loop, a direct call), with and without a
	// token: the continuation runs after init returned, and the root ends there.
	for (int mode = 0; mode < 4; ++mode) {
		const bool token = mode & 1, dispatch = mode & 2;
		task::cancel_source src;
		bool returned = false, ok = false;
		auto init = [&](auto h) {
			if (dispatch) {
				asio::dispatch(io.get_executor(), [h = std::move(h)]() mutable { h(asio::error_code{}); });
			} else {
				h(asio::error_code{});
			}
			returned = true;
		};
		auto body = [&]() -> task::task<void> {
			if (token) {
				co_await task::await_asio<void(asio::error_code)>(init, src.token());
			} else {
				co_await task::await_asio<void(asio::error_code)>(init);
			}
			ok = returned;
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(ok);
		if (!dispatch) {  // no scheduler at all: the handler cannot post, it only drops its count
			ok = returned = false;
			task::sync_wait(body());
			CHECK(ok);
		}
	}
	sock a(io), b(io);
	asio::local::connect_pair(a, b);
	// Requested on the loop while the read is pending: operation_aborted.
	{
		task::cancel_source src;
		asio::error_code ec;
		std::size_t n = 7;
		std::atomic<int> done{0};
		task::spawn(r, read_with(a, src.token(), &ec, &n, &done));
		io.restart();
		io.poll();
		CHECK(done == 0);
		asio::post(io, [&] { src.request(); });
		int ran = 0;
		while (done == 0 && ran < 10) { ran += int(io.run_one()); }
		CHECK(done == 1 && ec == asio::error::operation_aborted && n == 0);
		CHECK(ran == 2);  // the request, then the handler, which resumes the read inline (D6)
	}
	// A cancel that loses the race: the data was there, the read completed during initiation,
	// and the request inside init is emitted after: the real result comes back.
	{
		asio::write(b, asio::buffer("abcd", 4));
		task::cancel_source src;
		asio::error_code ec = asio::error::would_block;
		std::size_t n = 0;
		auto body = [&]() -> task::task<void> {
			char buf[8];
			std::tie(ec, n) = co_await task::await_asio<read_sig>([&](auto h) {
				a.async_read_some(asio::buffer(buf), std::move(h));
				src.request();
			}, src.token());
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(!ec && n == 4 && src.requested());
	}
	// An empty token binds no slot; a live one does.
	for (bool live : {false, true}) {
		task::cancel_source src;
		int connected = -1;
		auto body = [&]() -> task::task<void> {
			co_await task::await_asio<void(asio::error_code)>([&](auto h) {
				connected = asio::get_associated_cancellation_slot(h).is_connected();
				h(asio::error_code{});
			}, live ? src.token() : task::cancel_token{});
		};
		task::spawn(r, body());
		io.restart();
		io.run();
		CHECK(connected == int(live));
	}
	// Requested from another thread while the read is pending on a running loop: the frame
	// ends once, after the posted emit (TSan, ASan).
	{
		loop l;
		sock c(l.io), d(l.io);
		asio::local::connect_pair(c, d);
		l.start();
		constexpr int n = 10000;
		std::atomic<int> done{0};
		int aborted = 0;
		for (int i = 0; i < n; ++i) {
			task::cancel_source src;
			asio::error_code ec;
			std::size_t got = 0;
			task::spawn(l.ref(), read_with(c, src.token(), &ec, &got, &done));
			for (int k = 0; k < i % 3; ++k) { std::this_thread::yield(); }
			src.request();
			while (done.load() != i + 1) { std::this_thread::yield(); }
			aborted += ec == asio::error::operation_aborted;
		}
		CHECK(aborted == n);
		l.stop();
	}
	// Cross-thread requests race the operation's completion (random spins), with a timer, a
	// handler inside init and a throwing init: every task ends once, only as ok, aborted or
	// thrown, and no frame outlives its await (the handler's cb_.reset(), ASan).
	{
		loop l;
		l.start();
		constexpr int rounds = 20000;
		race_stats st;
		std::mt19937 rng(42);
		for (int i = 0; i < rounds; ++i) {
			task::cancel_source src;
			task::spawn(l.ref(), race_one(l.io, src.token(), i, &st));
			spin(std::chrono::nanoseconds(rng() % 400 * 100));  // 0-40 us
			src.request(task::cancel_reason::shutdown);
			if ((i & 63) == 0) { std::this_thread::yield(); }
		}
		while (st.finished.load() < rounds) { std::this_thread::sleep_for(1ms); }
		l.stop();
		CHECK(st.other == 0 && st.finished == rounds && st.ok + st.aborted + st.thrown == rounds);
		CHECK(st.inlined > 0);
		std::printf("race: ok %d aborted %d thrown %d, resumed inline %d\n", st.ok.load(), st.aborted.load(), st.thrown.load(), st.inlined.load());
	}
	CHECK(task::detail::live_frames() == base);
}

using tcp = asio::ip::tcp;
using accept_sig = void(asio::error_code, tcp::socket);

struct accepted {
	asio::error_code ec = asio::error::would_block;
	bool engaged = false, open = false, called = false;
	std::uint16_t peer_port = 0;
};
task::task<void> accept_with(tcp::acceptor& a, task::cancel_token t, accepted* out, std::atomic<int>* done) {
	auto [ec, peer] = co_await task::await_asio<accept_sig>([&](auto h) {
		out->called = true;
		a.async_accept(std::move(h));
	}, std::move(t));
	out->ec = ec;
	out->engaged = peer.has_value();
	out->open = peer && peer->is_open();
	if (out->open) { out->peer_port = peer->remote_endpoint().port(); }
	++*done;
}

// A completion argument without a default constructor (a socket): await_asio returns it as a
// std::optional with or without a token, empty only when a request came before the start.
void test_await_token_move_only() {
	const auto base = task::detail::live_frames();
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<accept_sig>(0, task::cancel_token{}))&>().await_resume()),
		std::tuple<asio::error_code, std::optional<tcp::socket>>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<accept_sig>(0))&>().await_resume()),
		std::tuple<asio::error_code, std::optional<tcp::socket>>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<read_sig>(0, task::cancel_token{}))&>().await_resume()),
		std::tuple<asio::error_code, std::size_t>>);
	static_assert(std::is_same_v<decltype(std::declval<decltype(task::await_asio<read_sig>(0))&>().await_resume()),
		std::tuple<asio::error_code, std::size_t>>);
	std::printf("sizes: token awaiter, socket argument %zu\n", sizeof(decltype(task::await_asio<accept_sig>(0, task::cancel_token{}))));
	asio::io_context io;
	task::asio_scheduler s(io.get_executor());
	auto r = task::scheduler_ref::of(s);
	tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
	// Requested before the await: init is not called, there is no socket.
	{
		task::cancel_source src;
		src.request();
		accepted out;
		std::atomic<int> done{0};
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.run();
		CHECK(done == 1 && !out.called && out.ec == asio::error::operation_aborted && !out.engaged);
	}
	// Requested while the accept is pending, on the loop and from another thread: asio completes
	// it with operation_aborted and a socket that is not open.
	for (bool other_thread : {false, true}) {
		task::cancel_source src;
		accepted out;
		std::atomic<int> done{0};
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.poll();
		CHECK(done == 0 && out.called);
		if (other_thread) {
			std::thread([&] { src.request(); }).join();
		} else {
			asio::post(io, [&] { src.request(); });
		}
		io.run_for(10s);
		CHECK(done == 1 && out.ec == asio::error::operation_aborted && out.engaged && !out.open);
	}
	// A connection arrives: the socket is handed over, open, connected to the client.
	{
		task::cancel_source src;
		accepted out;
		std::atomic<int> done{0};
		tcp::socket client(io);
		client.connect(acceptor.local_endpoint());
		task::spawn(r, accept_with(acceptor, src.token(), &out, &done));
		io.restart();
		io.run_for(10s);
		CHECK(done == 1 && !out.ec && out.engaged && out.open && out.peer_port == client.local_endpoint().port());
	}
	CHECK(task::detail::live_frames() == base);
}
