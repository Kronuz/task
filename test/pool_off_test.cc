// task with TASK_FRAME_POOL=0: every frame is an operator new, so a bench can count task
// frames against another library's allocations.
#define TASK_FRAME_POOL 0
#include "task.hh"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>

#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define TEST_COUNTS_NEW 0
#endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TEST_COUNTS_NEW 0
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

task::task<int> leaf(int i) { co_return i; }
task::task<int> sum(int n) {
	int s = 0;
	for (int i = 0; i < n; ++i) { s += co_await leaf(i); }
	co_return s;
}

int main() {
	task::inline_scheduler s;
	(void)task::run(s, sum(10));  // warm up the scheduler's queue
#if TEST_COUNTS_NEW
	const std::size_t before = allocations.load();
#endif
	const int got = task::run(s, sum(1000));
	bool ok = got == 499500;
#if TEST_COUNTS_NEW
	const std::size_t n = allocations.load() - before;
	ok = ok && n >= 1000;  // one per leaf frame at least; a pooled build makes 0 after warm-up
	std::printf("operator new for 1,000 frames with the pool off: %zu\n", n);
#endif
	std::printf("task_pool_off: %s\n", ok ? "ok" : "FAILED");
	return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
