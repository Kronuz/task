# task

Part of [Kronuz C++ Libraries](https://github.com/Kronuz/cpp-libraries), a collection of small, reusable C++ libraries.

[![CI](https://github.com/Kronuz/task/actions/workflows/ci.yml/badge.svg)](https://github.com/Kronuz/task/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Header-only](https://img.shields.io/badge/header--only-yes-green.svg)

Coroutine tasks for C++20 that run on someone else's event loop: executor affinity,
time-slice budgets, and cancellation. A task is lazy, resumes its awaiter in constant
stack, and returns to the awaiter's own scheduler when it finishes on another one. The
core header has no dependencies, and `task_asio.hh` binds it to standalone Asio. Reach
for it when coroutines share a loop the host owns (Asio, a reactor) and must not stall
it.

## ✨ Highlights

- **Constant stack on every compiler.** Awaiting resumes the awaiter by symmetric
  transfer on Clang and through a per-thread trampoline on GCC, so a 100,000-deep chain
  of synchronous completions runs in constant stack at every optimization level.
- **Executor affinity.** A task that finishes on another scheduler posts its awaiter
  back to the awaiter's own.
- **Bring your own loop.** A scheduler is anything with `post(handle)` and `now()`; the
  library owns no run queue. `inline_scheduler` is deterministic for tests, and
  `asio_scheduler` runs on an Asio executor.
- **Time-slice budgets.** `co_await budget.checkpoint()` costs one cycle-counter read
  while the slice lasts, and requeues the coroutine as demoted once it is spent.
- **Cancellation.** Checkpoints observe a `cancel_token` and throw `task::cancelled`.
- **Coroutine primitives.** `event`, a FIFO `async_mutex` with deadlines, `when_all`,
  `yield`, `spawn`, `sync_wait`, and `await_asio` for any Asio operation.
- **No allocation after warm-up.** Frames come from per-thread size-class pools.

## 🚀 Quick start

```cpp
#include "task.hh"

#include <chrono>
#include <cstdio>
#include <span>
#include <vector>

task::task<int> score(std::span<const int> docs, task::cancel_token cancel) {
	task::budget slice(std::chrono::microseconds(200), cancel);
	int total = 0;
	for (std::size_t i = 0; i < docs.size(); ++i) {
		total += docs[i];
		if (i % 4096 == 4095) { co_await slice.checkpoint(); }  // requeues once 200 µs are spent
	}
	co_return total;
}

int main() {
	task::cycles::calibrate();   // once, at startup
	std::vector<int> docs(100000, 1);

	task::inline_scheduler s;
	std::printf("%d\n", task::run(s, score(docs, {})));     // prints 100000
	std::printf("%d\n", task::sync_wait(score(docs, {})));  // prints 100000, from a plain thread
}
```

[docs/API.md](docs/API.md) lists every type and shows the Asio binding.

## 📦 Installation

Requires a C++20 compiler, threads, and CMake 3.14 or newer. The `task::asio` target
fetches standalone [Asio](https://github.com/chriskohlhoff/asio) 1.36.0, or uses the one
`-DASIO_INCLUDE_DIR=` points at; `-DTASK_ASIO=OFF` leaves it out. CI builds and tests it
with GCC and Clang on Linux (x86_64, arm64), macOS (arm64, x86_64), and FreeBSD, plus
ASan, UBSan, and TSan builds.

```cmake
include(FetchContent)
FetchContent_Declare(task
  GIT_REPOSITORY https://github.com/Kronuz/task.git
  GIT_TAG        9a85e89a2dc560b042a8a41efee9c09c62451db6)  # or main, which tracks the tip
FetchContent_MakeAvailable(task)

target_link_libraries(my_app PRIVATE task::task)   # or task::asio for the Asio binding
```

Then `#include "task.hh"`, or `#include "task_asio.hh"` for the Asio binding.

## 🎯 When to use it

Coroutines that run on a loop someone else owns: a reactor, an Asio `io_context`, a
simulation clock. Long computations that must yield the loop on a time budget, and
waits that must come back to the thread they started on.

Not for:

- **The fastest coroutines under GCC.** The default trampoline takes 56 ns for a no-op
  coroutine on g++ 16, `TASK_SYMMETRIC_TRANSFER=1` 34 ns, and `asio::awaitable` 24 ns on the
  same machine; `TASK_SYMMETRIC_TRANSFER=1` is safe on GCC only for optimized builds
  without sanitizers.
- **Memory that stays on its thread.** A frame freed on another thread joins that
  thread's pool, so memory migrates with the work.
- **Throwing from fire-and-forget work.** An exception that escapes a `spawn`
  terminates the program.
- **Demoting from other threads.** `asio_scheduler::post_demoted` must be called on the
  executor's thread.
- **Destroying a primitive with waiters.** The destructors of `event`, `async_mutex`,
  and `asio_scheduler` assert that nothing is left waiting.

## ⚡ Performance

`bench/bench.cc` times two things on one thread, the best of five trials of 20 million coroutines
for the first and 200,000 samples for the second, then the best of two runs. *No-op coroutine* is
the time to create, await and destroy one: a parent loops over `co_await child()`, where the child
returns 1 at once. *task* is `task::task<int>` run by `task::run` on the inline scheduler;
*`asio::awaitable`* is `asio::awaitable<int>` run by `asio::co_spawn` on an `io_context`, the
coroutine type Asio code already uses. *Yield and resume* is one coroutine on an idle `io_context`
timing, with the cycle counter, from `co_await task::yield()` through `task::asio_scheduler` (task)
or from `co_await asio::post(executor, use_awaitable)` (`asio::post`) to the instruction after it.
Both sides are built with Asio
[asio-1-36-0](https://github.com/chriskohlhoff/asio/tree/asio-1-36-0), the version CMake pins, and
return the same sums. A replaced global `operator new` counts allocations: task allocates nothing in
20 million coroutines, and `asio::awaitable` recycles its frames (6 allocations under Apple clang,
none under g++). Clang resumes the awaiter by symmetric transfer, a tail call. g++ resumes it
through a per-thread trampoline by default (see [ARCHITECTURE.md](ARCHITECTURE.md));
`TASK_SYMMETRIC_TRANSFER=1` selects the tail call there, which is safe only for optimized builds
without sanitizers. `-O3` on every row.

| Machine, compiler | No-op coroutine: task | `asio::awaitable` | Yield and resume, p50 / p99: task | `asio::post` |
| --- | ---: | ---: | ---: | ---: |
| Apple M4 Pro, Apple clang 17 | **9.2 ns** | 10.2 ns | 42 / 83 ns | 42 / 84 ns |
| Intel Core i9-9980HK, Apple clang 17 | **18.1 ns** | 30.4 ns | 115 / 133 ns | 148 / 171 ns |
| Intel Core i9-9980HK, g++ 16 (trampoline) | 56.4 ns | **23.9 ns** | 90 / 101 ns | 122 / 143 ns |
| Intel Core i9-9980HK, g++ 16, `TASK_SYMMETRIC_TRANSFER=1` | 34.3 ns | **23.7 ns** | 92 / 108 ns | 118 / 137 ns |

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target task_bench
./build/task_bench
```

## 📚 Documentation

- [docs/API.md](docs/API.md): every type and function, and how to run a task from a
  scheduler, a plain thread, or Asio.
- [ARCHITECTURE.md](ARCHITECTURE.md): tasks, transfers, schedulers, budgets, events and
  locks, frames, and `when_all`.
- [docs/mechanisms/async-mutex.md](docs/mechanisms/async-mutex.md): how `async_mutex`
  hands the lock over and gives up at a deadline.
- [AGENTS.md](AGENTS.md): repository map, invariants, and how to verify a change.
- [Catalog entry](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md#task).

## 🛠️ Building and testing

```sh
cmake -B build
cmake --build build
ctest --test-dir build
./build/task_test 100000000   # the stress test with 10^8 tasks
```

The tests cover values and exceptions, a million synchronous awaits and a 100,000-deep
chain in constant stack, affinity across schedulers and threads, budget demotion,
cancellation at a checkpoint, `when_all`, `event`, `async_mutex` and both orders of the
`lock_until` race, `await_asio` on real timers, and a stress run with random yields and
random cancellation. `-DTASK_ASIO=OFF` builds the core without fetching Asio.

## 🤝 Contributing

Issues and pull requests are welcome. Read [AGENTS.md](AGENTS.md) first, and the
[catalog's AGENTS.md](https://github.com/Kronuz/cpp-libraries/blob/main/AGENTS.md) for
the family's rules. CI must pass on every platform.

## 📄 License

MIT; see [LICENSE](LICENSE).
