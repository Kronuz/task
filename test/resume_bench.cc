/*
 * Copyright © 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The resume path on one asio_scheduler with the default budget (light_slices 0), in ns per
// resume: a yield (post), a spent checkpoint (post_demoted and the drainer), an await_asio
// resumed inline, and a child task completing into its parent (final_awaiter's comparison).
// Uses only the API that predates the lane, so test/resume_ab.py builds it against both.
#include "task_asio.hh"

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>

namespace {

constexpr long n = 1'000'000;

task::task<int> one() { co_return 1; }

template <typename Body>
double measure(Body body) {
	double best = 1e300;
	for (int trial = 0; trial < 6; ++trial) {  // trial 0 warms the pools
		asio::io_context io;
		task::asio_scheduler s(io.get_executor());
		long sum = 0;
		auto t0 = std::chrono::steady_clock::now();
		task::spawn(task::scheduler_ref::of(s), body(io, sum));
		io.run();
		double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t0).count() / double(n);
		if (sum != n) { std::exit(1); }
		if (trial) { best = std::min(best, ns); }
	}
	return best;
}

}  // namespace

int main() {
	const double yield = measure([](asio::io_context&, long& sum) -> task::task<void> {
		for (long i = 0; i < n; ++i) {
			co_await task::yield();
			++sum;
		}
	});
	const double demote = measure([](asio::io_context&, long& sum) -> task::task<void> {
		task::budget b(std::chrono::nanoseconds(1));
		for (long i = 0; i < n; ++i) {
			co_await b.checkpoint();
			++sum;
		}
	});
	const double inline_await = measure([](asio::io_context& io, long& sum) -> task::task<void> {
		for (long i = 0; i < n; ++i) {
			co_await task::await_asio<void()>([&](auto h) { asio::post(io, std::move(h)); });
			++sum;
		}
	});
	const double child = measure([](asio::io_context&, long& sum) -> task::task<void> {
		for (long i = 0; i < n; ++i) { sum += co_await one(); }
	});
	std::printf("{\"yield\": %.3f, \"demote\": %.3f, \"inline_await\": %.3f, \"child\": %.3f}\n", yield, demote, inline_await, child);
	return 0;
}
