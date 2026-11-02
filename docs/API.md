# task API

`task.hh` holds the core and needs only the standard library and threads; `task_asio.hh`
binds it to standalone Asio (CMake target `task::asio`). [ARCHITECTURE.md](../ARCHITECTURE.md)
explains how each piece works.

## Types and functions

- `task::task<T>`: a lazy coroutine. Awaiting it runs it and resumes the awaiter
  without growing the stack, so chains of synchronous completions run in constant
  stack on every compiler and optimization level: symmetric transfer on Clang, a
  per-thread trampoline on GCC (whose tail calls disappear at `-O0`/`-O1` and under
  sanitizers). When a task finishes on a different scheduler than its awaiter's, the
  awaiter is posted back to its own scheduler (executor affinity).
- Schedulers: anything with `post(handle)` and `now()`, optionally
  `post_demoted(handle)`. The library owns no run queue; the host's loop is the
  queue. `inline_scheduler` is a deterministic one for tests and simulation, and
  `asio_scheduler` runs on an Asio executor.
- `budget`: a time slice measured with the CPU cycle counter. `co_await
  budget.checkpoint()` costs one counter read while the slice lasts; once it is
  spent, the coroutine requeues as demoted, so a long computation cannot hold a loop
  for more than one slice. The slice is one resume: time spent suspended does not count,
  and a task resumed after an event, a lock or an Asio completion starts a fresh slice.
  Checkpoints also observe a `cancel_token` and throw `task::cancelled`.
- `cancel_source` / `cancel_token`: `request(reason)` is first-wins and sets a
  `cancel_reason` (`requested`, `deadline`, `shutdown`, or a library's own from `user`);
  `requested()` is one atomic load, and an empty token allocates nothing.
  `cancel_source(parent_token)` is requested with its parent. `cancel_callback(token, f)`
  runs `f(reason)` on the requesting thread, with `std::stop_callback`'s lifetime rules.
  `deadline(sched, time_point, parent = {})` is a source its scheduler's timer requests
  with `cancel_reason::deadline`; `sched` supplies `start_timer` (`asio_scheduler` does).
- `event`: a one-shot completion. `co_await e` suspends until `e.set()` (any thread); each
  waiter resumes through the scheduler it suspended on, in arrival order.
- `async_mutex`: a FIFO mutex for coroutines, usable from every scheduler at once.
  `co_await m.lock()` returns a `guard`; `unlock` hands the lock to the next waiter through its
  scheduler. `co_await m.lock(token)` gives up when the token is requested and returns an
  empty guard; with a `deadline`'s token it is a timed lock.
- `await_asio<void(Args...)>(init)` (in `task_asio.hh`): awaits any Asio operation started by
  `init(handler)`; `asio_scheduler::demoted()` counts the handles waiting in its demoted queue.
- The destructors of `event`, `async_mutex` and `asio_scheduler` assert that nothing is left
  waiting: a waiter must run to completion before the primitive goes away.
- `cycles::calibrate()` measures the cycle counter once; call it at startup so the first
  budget does not busy-wait 2 ms on an event loop.
- `yield()`, `schedule_on{s}`, `when_all(...)` (variadic and vectors), `spawn`, and
  `run` (drive a task to completion on an `inline_scheduler`).
- Frames come from per-thread size-class pools: after warm-up a task allocates
  nothing.

## Running a task

On an `inline_scheduler`, driven to completion by `run`:

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

From a plain thread, blocking until the task finishes wherever it runs (never on a thread
the task's work needs, such as a reactor loop):

```cpp
int total = task::sync_wait(score(docs, {}));
```

From Asio:

```cpp
#include "task_asio.hh"
#include <asio.hpp>

task::asio_scheduler sched(io.get_executor());
asio::co_spawn(io, [&]() -> asio::awaitable<void> {
    int total = co_await task::as_awaitable(sched, score(docs, {}));
}, asio::detached);
```
