# task

C++20 coroutine tasks for code that runs on someone else's event loop. Header-only;
the core has no dependencies, and `task_asio.hh` binds it to standalone Asio.

## What it is

- `task::task<T>`: a lazy coroutine. Awaiting it runs it and resumes the awaiter
  through symmetric transfer, so chains of synchronous completions run in constant
  stack. When a task finishes on a different scheduler than its awaiter's, the
  awaiter is posted back to its own scheduler (executor affinity).
- Schedulers: anything with `post(handle)` and `now()`, optionally
  `post_demoted(handle)`. The library owns no run queue; the host's loop is the
  queue. `inline_scheduler` is a deterministic one for tests and simulation, and
  `asio_scheduler` runs on an Asio executor.
- `budget`: a time slice measured with the CPU cycle counter. `co_await
  budget.checkpoint()` costs one counter read while the slice lasts; once it is
  spent, the coroutine requeues as demoted, so a long computation cannot hold a loop
  for more than one slice. Checkpoints also observe a `cancel_token` and throw
  `task::cancelled`.
- `yield()`, `schedule_on{s}`, `when_all(...)` (variadic and vectors), `spawn`, and
  `run` (drive a task to completion on an `inline_scheduler`).
- Frames come from per-thread size-class pools: after warm-up a task allocates
  nothing.

## Usage

```cpp
#include "task.hh"

task::task<int> score(std::span<const int> docs, task::cancel_token cancel) {
    task::budget slice(std::chrono::microseconds(200), cancel);
    int total = 0;
    for (std::size_t i = 0; i < docs.size(); ++i) {
        total += docs[i];
        if (i % 4096 == 4095) { co_await slice.checkpoint(); }  // bounded stall
    }
    co_return total;
}

task::inline_scheduler s;
int total = task::run(s, score(docs, {}));
```

From Asio:

```cpp
#include "task_asio.hh"

task::asio_scheduler sched(io.get_executor());
asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    int total = co_await task::as_awaitable(sched, score(docs, {}));
}, asio::detached);
```

## Performance

Intel i9-9980HK, macOS, Apple clang `-O3`:

| Measurement | Result |
|---|---:|
| Create, await, and destroy a no-op task | 17.9 ns, 0 allocations |
| Yield to resume through `asio_scheduler`, idle loop | p50 123 ns, p99 175 ns |

`bench/bench.cc` reproduces both, counting allocations with a global `operator new`.

## Build and test

```sh
cmake -S . -B build && cmake --build build && ctest --test-dir build
build/task_test 100000000   # the stress test with 10^8 tasks
```

The tests cover values and exceptions, a million synchronous awaits and a
100,000-deep chain in constant stack, affinity across inline schedulers and across
threads, budget demotion (two busy tasks interleave), cancellation at a checkpoint,
`when_all`, and a stress run of tasks with random yields and random cancellation.
`TASK_ASIO=OFF` builds the core without fetching Asio.

## Limits

- A frame freed on another thread joins that thread's pool; memory migrates with the
  work rather than returning home.
- `asio_scheduler::post_demoted` must be called on the executor's thread.
- An exception escaping a fire-and-forget `spawn` terminates the program.

## License

MIT. See [LICENSE](LICENSE).
