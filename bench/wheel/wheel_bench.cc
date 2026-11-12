// Detent's wheel-evidence harness (docs/design/wheel-evidence.md, async-drivers-bench/
// wheel_vs_stash.cc at Detent:5ecbeb4c), ported with its workload unchanged and stash's arms
// dropped:
//   (a)  the prototype wheel (proto.hh), owner thread only: the single-loop reference
//   (f)  (a) with a 64-stripe inbox through the entry's own `next`, as wheel-evidence measured it:
//        the cross-thread reference
//   (t)  task::timer_wheel
// Modes, one line of results each:
//   single ARM N        arm, re-arm, cancel and expire on one thread, on a manual clock
//   cross ARM P TOTAL   P producer threads arm TOTAL entries (default 1,000,000), one owner
//                       fires on steady_clock
// ARM is a, f or t; a2 and f2 run a and f again (the A/A pairs). WHEEL_BENCH_PAD=BYTES allocates
// that many bytes after the wheel and before the entries (a placement probe, 0 by default).
#include "proto.hh"
#include "task.hh"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
using u64 = std::uint64_t;
constexpr u64 ms = 1'000'000;
constexpr u64 manual_base = 7'000 * ms;  // far below steady_clock: a wheel reading it would fire early

#define CHECK(c)                                                                     \
	do {                                                                             \
		if (!(c)) {                                                                  \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
			std::exit(1);                                                            \
		}                                                                            \
	} while (0)

u64 now_ns() {
	return static_cast<u64>(std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::steady_clock::now().time_since_epoch()).count());
}
std::string utc() {
	auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	char b[32];
	std::strftime(b, sizeof b, "%FT%TZ", std::gmtime(&t));
	return b;
}
unsigned mod64k(const void* p) { return static_cast<unsigned>(reinterpret_cast<std::uintptr_t>(p) & 0xffff); }

// ---- (a) ----------------------------------------------------------------------------------
struct cand_a {
	using entry = bench::wheel_entry;
	static constexpr bool cross = false;
	static constexpr const char* shape = "intrusive wheel, 4096 x 1 ms, owner thread only";
	std::unique_ptr<bench::wheel> w;
	explicit cand_a(u64 base) : w(std::make_unique<bench::wheel>(base)) {}
	const void* heads() const { return w.get(); }
	static u64 deadline(const entry& e) { return e.deadline; }
	void arm(entry& e, u64 d) { w->arm(e, d); }
	void cancel(entry& e) { w->cancel(e); }
	template <class F>
	std::size_t expire(u64 now, F&& f) { return w->expire(now, [&](bench::wheel_entry& e) { f(e); }); }
};

// ---- (f) ----------------------------------------------------------------------------------
// Each producer thread pushes on its own stripe (64, by first use); the push that finds its
// stripe empty sets the stripe's bit, and the owner drains only the stripes whose bits it took.
inline unsigned inbox_stripe() noexcept {
	static std::atomic<unsigned> seq{0};
	static thread_local unsigned stripe = seq.fetch_add(1, std::memory_order_relaxed) % 64;
	return stripe;
}
struct cand_f {
	using entry = bench::wheel_entry;
	static constexpr bool cross = true;
	static constexpr const char* shape = "intrusive wheel + 64-stripe inbox through the entry's next";
	struct alignas(64) stripe { std::atomic<entry*> head{nullptr}; };
	std::unique_ptr<bench::wheel> w;
	std::unique_ptr<stripe[]> stripes = std::make_unique<stripe[]>(64);
	alignas(64) std::atomic<u64> pending{0};
	explicit cand_f(u64 base) : w(std::make_unique<bench::wheel>(base)) {}
	const void* heads() const { return w.get(); }
	static u64 deadline(const entry& e) { return e.deadline; }
	void arm(entry& e, u64 d) { w->arm(e, d); }
	void cancel(entry& e) { w->cancel(e); }
	// Any thread, an unarmed entry: `next` links its stripe until the owner files it.
	void arm_from(entry& e, u64 d) {
		e.deadline = d;
		const unsigned s = inbox_stripe();
		auto& head = stripes[s].head;
		entry* first = head.load(std::memory_order_relaxed);
		do { e.next = first; } while (!head.compare_exchange_weak(first, &e, std::memory_order_release, std::memory_order_relaxed));
		if (!first) { pending.fetch_or(u64{1} << s, std::memory_order_release); }
	}
	template <class F>
	std::size_t expire(u64 now, F&& f) {
		if (pending.load(std::memory_order_relaxed)) {
			for (u64 bits = pending.exchange(0, std::memory_order_acquire); bits; bits &= bits - 1) {
				auto& head = stripes[static_cast<unsigned>(std::countr_zero(bits))].head;
				for (entry* e = head.exchange(nullptr, std::memory_order_acquire); e;) {
					entry* following = e->next;
					e->next = nullptr;
					w->arm(*e, e->deadline);
					e = following;
				}
			}
		}
		return w->expire(now, [&](bench::wheel_entry& e) { f(e); });
	}
};

// ---- (t) ----------------------------------------------------------------------------------
struct cand_t {
	using entry = task::timer_entry;
	static constexpr bool cross = true;
	static constexpr const char* shape = "task::timer_wheel";
	std::unique_ptr<task::timer_wheel> w;
	explicit cand_t(u64 base) : w(std::make_unique<task::timer_wheel>(base)) {}
	const void* heads() const { return w.get(); }
	static u64 deadline(const entry& e) { return e.deadline; }
	void arm(entry& e, u64 d) { w->arm(e, d); }
	void cancel(entry& e) { w->cancel(e); }
	void arm_from(entry& e, u64 d) { w->arm_remote(e, d); }
	template <class F>
	std::size_t expire(u64 now, F&& f) { return w->expire(now, [&](task::timer_entry& e) { f(e); }); }
};

template <class F>
auto dispatch(std::string_view arm, F&& f) {
	if (arm == "a" || arm == "a2") { return f(static_cast<cand_a*>(nullptr)); }
	if (arm == "f" || arm == "f2") { return f(static_cast<cand_f*>(nullptr)); }
	if (arm == "t") { return f(static_cast<cand_t*>(nullptr)); }
	std::fprintf(stderr, "unknown arm %.*s\n", int(arm.size()), arm.data());
	std::exit(2);
}

// ---- single loop --------------------------------------------------------------------------
// Each repetition: arm every entry, re-arm every entry, cancel every entry (each pass in one
// random order), arm every entry again, then expire in 1 ms steps until all fired. The manual
// clock moves 2,002 ms per repetition; at least 1,000,000 / n repetitions, and more until the
// timed passes add up to 100 ms of wall time.
template <class C>
void single(std::string_view arm, std::size_t n) {
	const std::size_t min_reps = std::max<std::size_t>(1, 1'000'000 / n);
	constexpr u64 min_timed_ns = 100 * ms;
	std::mt19937_64 random(n);
	std::vector<std::uint32_t> order(n);
	std::iota(order.begin(), order.end(), 0);
	std::shuffle(order.begin(), order.end(), random);
	auto spread = [&] { return 1 * ms + random() % (1'999 * ms); };
	std::vector<u64> d1(n), d2(n), d3(n);
	for (std::size_t i = 0; i < n; ++i) { d1[i] = spread(); d2[i] = spread(); d3[i] = spread(); }
	C c(manual_base);
	const char* pad_env = std::getenv("WHEEL_BENCH_PAD");
	std::vector<char> pad(pad_env ? std::strtoull(pad_env, nullptr, 10) : 0);
	std::vector<typename C::entry> entries(n);
	std::printf("config row=single arm=%.*s n=%zu min_reps=%zu min_timed_ms=%llu shape=\"%s\" entry_bytes=%zu clock=manual base_ns=%llu pad=%zu heads_mod64k=%u entries_mod64k=%u\n",
		int(arm.size()), arm.data(), n, min_reps, static_cast<unsigned long long>(min_timed_ns / ms), C::shape, sizeof(typename C::entry),
		static_cast<unsigned long long>(manual_base), pad.size(), mod64k(c.heads()), mod64k(entries.data()));
	std::printf("start=%s\n", utc().c_str());
	u64 t_arm = 0, t_rearm = 0, t_cancel = 0, t_expire = 0;
	std::size_t fired = 0;
	const u64 started = now_ns();
	std::size_t reps = 0;
	for (; reps < min_reps || t_arm + t_rearm + t_cancel + t_expire < min_timed_ns; ++reps) {
		const u64 base = manual_base + reps * 2'002 * ms;
		u64 t0 = now_ns();
		for (auto i : order) { c.arm(entries[i], base + d1[i]); }
		u64 t1 = now_ns();
		for (auto i : order) { c.arm(entries[i], base + d2[i]); }
		u64 t2 = now_ns();
		for (auto i : order) { c.cancel(entries[i]); }
		u64 t3 = now_ns();
		for (auto i : order) { c.arm(entries[i], base + d3[i]); }
		u64 t4 = now_ns();
		std::size_t round = 0;
		for (u64 t = base; t <= base + 2'001 * ms; t += ms) { round += c.expire(t, [](auto&) {}); }
		u64 t5 = now_ns();
		CHECK(round == n);
		fired += round;
		t_arm += t1 - t0; t_rearm += t2 - t1; t_cancel += t3 - t2; t_expire += t5 - t4;
	}
	const double ops = static_cast<double>(n * reps);
	const double arm_ns = t_arm / ops, rearm_ns = t_rearm / ops, cancel_ns = t_cancel / ops, expire_ns = t_expire / ops;
	std::printf("timed_s=%.3f\n", (now_ns() - started) / 1e9);
	std::printf("row=single arm=%.*s n=%zu pad=%zu reps=%zu arm_ns=%.2f rearm_ns=%.2f cancel_ns=%.2f expire_ns=%.2f score=%.2f fired=%zu\n",
		int(arm.size()), arm.data(), n, pad.size(), reps, arm_ns, rearm_ns, cancel_ns, expire_ns, arm_ns + rearm_ns + cancel_ns + expire_ns, fired);
	std::printf("end=%s\n", utc().c_str());
}

// ---- cross-thread -------------------------------------------------------------------------
// P producers each arm TOTAL / P distinct entries once, at steady_clock now (read every 64 arms)
// plus uniform [1, window] ms; the owner spins on steady_clock, firing until every entry fired.
struct cross_result {
	double producer_ns = 0, span_ms = 0, last_after_deadline_ms = 0, max_late_ms = 0;
	std::size_t fired = 0, early = 0, twice = 0, missing = 0;
};
template <class C>
cross_result cross_run(std::size_t producers, std::size_t total, u64 window_ms) {
	const std::size_t per = total / producers;
	total = per * producers;
	std::vector<typename C::entry> entries(total);
	std::vector<std::uint32_t> fires(total, 0);
	std::vector<u64> offsets(total);
	std::mt19937_64 random(total + producers);
	for (auto& o : offsets) { o = 1 * ms + random() % ((window_ms - 1) * ms); }
	C c(now_ns());
	std::atomic<std::size_t> ready{0};
	std::atomic<bool> go{false};
	std::vector<u64> elapsed(producers);
	std::vector<std::thread> threads;
	for (std::size_t p = 0; p < producers; ++p) {
		threads.emplace_back([&, p] {
			ready.fetch_add(1);
			while (!go.load(std::memory_order_acquire)) {}
			const u64 t0 = now_ns();
			u64 now = t0;
			for (std::size_t k = 0; k < per; ++k) {
				if ((k & 63) == 0) { now = now_ns(); }
				const std::size_t i = p * per + k;
				c.arm_from(entries[i], now + offsets[i]);
			}
			elapsed[p] = now_ns() - t0;
		});
	}
	while (ready.load() != producers) {}
	cross_result out;
	u64 last_deadline = 0, last_fire = 0, max_late = 0;
	const u64 start = now_ns();
	go.store(true, std::memory_order_release);
	while (out.fired < total) {
		const u64 now = now_ns();
		out.fired += c.expire(now, [&](typename C::entry& e) {
			const u64 d = C::deadline(e);
			if (d > now) { ++out.early; }
			max_late = std::max(max_late, now - std::min(now, d));
			last_deadline = std::max(last_deadline, d);
			++fires[static_cast<std::size_t>(&e - entries.data())];
		});
		last_fire = now;
		if (now - start > 60'000 * ms) { break; }  // a lost entry: report it rather than hang
	}
	for (auto& t : threads) { t.join(); }
	for (auto f : fires) { out.twice += f > 1; out.missing += f == 0; }
	std::sort(elapsed.begin(), elapsed.end());
	out.producer_ns = static_cast<double>(elapsed[producers / 2]) / static_cast<double>(per);
	if (producers % 2 == 0) {
		out.producer_ns = (static_cast<double>(elapsed[producers / 2 - 1]) + static_cast<double>(elapsed[producers / 2])) / 2.0 / static_cast<double>(per);
	}
	out.span_ms = (last_fire - start) / 1e6;
	out.last_after_deadline_ms = (last_fire > last_deadline ? last_fire - last_deadline : 0) / 1e6;
	out.max_late_ms = max_late / 1e6;
	return out;
}
template <class C>
void cross(std::string_view arm, std::size_t producers, std::size_t total) {
	if constexpr (!C::cross) {
		std::fprintf(stderr, "arm %.*s has no cross-thread arm\n", int(arm.size()), arm.data());
		std::exit(2);
	} else {
		std::printf("config row=cross arm=%.*s producers=%zu total=%zu window_ms=1000 shape=\"%s\" entry_bytes=%zu clock=steady hw_threads=%u\n",
			int(arm.size()), arm.data(), producers, total, C::shape, sizeof(typename C::entry), std::thread::hardware_concurrency());
		std::printf("start=%s\n", utc().c_str());
		const u64 started = now_ns();
		const auto r = cross_run<C>(producers, total, 1'000);
		std::printf("timed_s=%.3f\n", (now_ns() - started) / 1e9);
		const bool valid = r.early == 0 && r.twice == 0 && r.missing == 0 && r.last_after_deadline_ms <= 100.0;
		std::printf("row=cross arm=%.*s p=%zu producer_ns=%.2f span_ms=%.1f last_after_deadline_ms=%.2f max_late_ms=%.2f fired=%zu early=%zu twice=%zu missing=%zu valid=%d\n",
			int(arm.size()), arm.data(), producers, r.producer_ns, r.span_ms, r.last_after_deadline_ms, r.max_late_ms, r.fired, r.early, r.twice, r.missing, int(valid));
		std::printf("end=%s\n", utc().c_str());
		CHECK(r.early == 0 && r.twice == 0 && r.missing == 0);
	}
}
}  // namespace

int main(int argc, char** argv) {
	std::setvbuf(stdout, nullptr, _IOLBF, 0);
	if (argc < 4) {
		std::fprintf(stderr, "usage: %s single ARM N | cross ARM P TOTAL\n", argv[0]);
		return 2;
	}
	const std::string_view mode = argv[1], arm = argv[2];
	const std::size_t x = std::strtoull(argv[3], nullptr, 10);
	if (mode == "single") {
		dispatch(arm, [&](auto* tag) { single<std::remove_pointer_t<decltype(tag)>>(arm, x); });
	} else if (mode == "cross") {
		const std::size_t total = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 1'000'000;
		dispatch(arm, [&](auto* tag) { cross<std::remove_pointer_t<decltype(tag)>>(arm, x, total); });
	} else {
		std::fprintf(stderr, "unknown mode\n");
		return 2;
	}
	return 0;
}
