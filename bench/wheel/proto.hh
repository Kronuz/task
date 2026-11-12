// The timer wheel the design proposes (docs/design/async-drivers.md, D4), as a prototype to
// measure and test: owner-thread only while its loop runs (Debug-asserted), clock-agnostic (deadlines are
// nanoseconds on the owner's clock, manual or steady), intrusive (a 32-byte entry inside its
// owner, no allocation), O(1) arm and cancel, and next() to arm the one reactor timer.
// 4,096 slots of 1 ms; a deadline a round or more ahead waits in its slot, skipped until due.
#pragma once

#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <thread>

namespace bench {

struct wheel_entry {
	wheel_entry* next = nullptr;
	wheel_entry** pprev = nullptr;  // the pointer that points here: a slot head, the due list or a predecessor's next
	std::uint64_t deadline = 0;
	void (*fire)(wheel_entry&) noexcept = nullptr;  // what expire() runs; may arm or cancel any entry
	bool armed() const noexcept { return pprev != nullptr; }
};

class wheel {
public:
	static constexpr std::uint64_t tick = 1'000'000;  // 1 ms
	static constexpr std::size_t slots = 4096, mask = slots - 1, words = slots / 64;

	explicit wheel(std::uint64_t now = 0) noexcept : cursor_(now / tick) {}

	// The first arm, cancel or expire binds the wheel to its thread; every later one must run there.
	// release() unbinds it once the loop is stopped and closed (teardown's phase 2), so teardown's
	// one thread may unlink entries and drop what is still armed (phases 3 and 4).
	void release() noexcept {
#ifndef NDEBUG
		owner_ = std::thread::id{};
#endif
	}
	void arm(wheel_entry& e, std::uint64_t deadline) noexcept {
		owner_check();
		if (e.armed()) { unlink(e); }
		e.deadline = deadline;
		const std::uint64_t at = deadline / tick < cursor_ ? cursor_ : deadline / tick;
		const std::size_t s = at & mask;
		link(e, head_[s]);
		bits_[s / 64] |= std::uint64_t{1} << (s % 64);
	}
	void cancel(wheel_entry& e) noexcept {
		owner_check();
		if (e.armed()) { unlink(e); }
	}
	// Fires every entry due at `now`, at most one tick after its deadline and never before.
	// The due entries move to a detached list first, so a fire may cancel or re-arm any entry,
	// itself included; one armed during the fires for a time already due fires on the next call.
	template <class F>
	std::size_t expire(std::uint64_t now, F&& fire) {
		owner_check();
		assert(!expiring_ && "expire() is not re-entrant");
		const std::uint64_t last = now / tick;
		if (last < cursor_) { return 0; }
		const std::uint64_t steps = last - cursor_ + 1 > slots ? slots : last - cursor_ + 1;
		wheel_entry** tail = &due_;  // the due list keeps slot order
		for (std::uint64_t k = 0; k < steps; ++k) {
			const std::size_t s = (cursor_ + k) & mask;
			if (!(bits_[s / 64] & (std::uint64_t{1} << (s % 64)))) { continue; }
			for (wheel_entry* e = head_[s]; e;) {
				wheel_entry* following = e->next;
				if (e->deadline <= now) {
					unlink(*e);
					e->next = nullptr;
					*tail = e;
					e->pprev = tail;
					tail = &e->next;
					++count_;
				}
				e = following;
			}
		}
		cursor_ = last;
		expiring_ = true;
		std::size_t fired = 0;
		while (wheel_entry* e = due_) {
			unlink(*e);
			++fired;
			fire(*e);
		}
		expiring_ = false;
		return fired;
	}
	std::size_t expire(std::uint64_t now) {
		return expire(now, [](wheel_entry& e) { e.fire(e); });
	}
	// The end of the next occupied slot: every entry filed there this round is due by then
	// (an entry a whole round later costs one early wake per round); ~0 when empty.
	std::uint64_t next() const noexcept {
		if (!count_) { return ~std::uint64_t{0}; }
		const std::size_t start = cursor_ & mask;
		for (std::size_t k = 0; k <= words; ++k) {
			const std::size_t w = (start / 64 + k) % words;
			std::uint64_t word = bits_[w];
			if (k == 0) { word &= ~std::uint64_t{0} << (start % 64); }
			if (word) {
				const std::size_t s = w * 64 + static_cast<std::size_t>(std::countr_zero(word));
				const std::uint64_t distance = (s - start) & mask;
				return (cursor_ + distance + 1) * tick;
			}
		}
		return ~std::uint64_t{0};
	}
	std::size_t size() const noexcept { return count_; }

private:
	void owner_check() noexcept {
#ifndef NDEBUG
		if (owner_ == std::thread::id{}) { owner_ = std::this_thread::get_id(); }
		assert(owner_ == std::this_thread::get_id() && "a wheel is used only on its loop's thread");
#endif
	}
	void link(wheel_entry& e, wheel_entry*& head) noexcept {
		e.next = head;
		if (e.next) { e.next->pprev = &e.next; }
		head = &e;
		e.pprev = &head;
		++count_;
	}
	void unlink(wheel_entry& e) noexcept {
		*e.pprev = e.next;
		if (e.next) { e.next->pprev = e.pprev; }
		auto** first = head_.data();
		if (e.pprev >= first && e.pprev < first + slots && !*e.pprev) {
			const std::size_t s = static_cast<std::size_t>(e.pprev - first);
			bits_[s / 64] &= ~(std::uint64_t{1} << (s % 64));
		}
		e.next = nullptr; e.pprev = nullptr;
		--count_;
	}

	std::array<wheel_entry*, slots> head_{};
	std::array<std::uint64_t, words> bits_{};
	wheel_entry* due_ = nullptr;
	std::uint64_t cursor_;
	std::size_t count_ = 0;
	bool expiring_ = false;
#ifndef NDEBUG
	std::thread::id owner_{};
#endif
};

}  // namespace bench
