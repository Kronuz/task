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
  `requested()` is one acquire load, and an empty token allocates nothing: a thread that sees
  it true also sees every write the requester made before `request()`, so a payload can be
  published before requesting and read after polling.
  `cancel_source(parent_token)` is requested with its parent. `cancel_callback(token, f)`
  runs `f(reason)` on the requesting thread, with `std::stop_callback`'s lifetime rules.
  `deadline(sched, time_point, parent = {})` is a source its scheduler's timer requests
  with `cancel_reason::deadline`; `sched` supplies `start_timer` (`asio_scheduler` does). It
  keeps no reference to `sched` and may be destroyed on any thread, before or after the expiry.
- `event`: a one-shot completion. `co_await e` suspends until `e.set()` (any thread); each
  waiter resumes through the scheduler it suspended on, in arrival order.
- `async_mutex`: a FIFO mutex for coroutines, usable from every scheduler at once.
  `co_await m.lock()` returns a `guard`; `unlock` hands the lock to the next waiter through its
  scheduler. `co_await m.lock(token)` gives up when the token is requested and returns an
  empty guard; with a `deadline`'s token it is a timed lock.
- `await_asio<void(Args...)>(init[, token])` (in `task_asio.hh`): awaits any Asio operation
  started by `init(handler)`. With a token (the signature must begin with `asio::error_code`),
  a requested token never starts the operation and completes with `operation_aborted`, and a
  request while it is pending cancels it through a cancellation slot; the operation must belong
  to the awaiting scheduler's loop. A cancel that loses the race returns the real result.
- Optional scheduler members, found by detection and forwarded by wrappers: `runs_here()`,
  `post_call(fn, arg)` (a cross-thread cancel's emit) and `roots()` (where `spawn` links roots).
- Teardown (`asio_scheduler`): `close()` makes `post`, `post_demoted` and `post_call` drop and
  `spawn` destroy its task unrun; `destroy_roots()` destroys every root, with every `io_context`
  alive; both are idempotent and the owner's, after every loop has stopped and joined; the
  destructor repeats them. `asio_scheduler::demoted()` counts the handles in its demoted queue.
- At teardown a frame suspended on any primitive may be destroyed: a granted, unresumed lock
  waiter unlocks, an `event` waiter unlinks, and an `event` destroyed first orphans its waiters
  (`detail::orphaned_waiters()` counts them in Debug). `async_mutex`'s destructor asserts in
  Debug that it is unlocked; a primitive awaited by several roots must outlive them.
- `TASK_FRAME_POOL=0` sends every frame to `operator new`, for counting frames in a bench.
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

On an Asio loop, with a socket read that a token can cancel:

```cpp
#include "task_asio.hh"

task::task<void> serve(asio::ip::tcp::socket& s, task::asio_scheduler& sched, task::cancel_token stop) {
    char buf[4096];
    for (;;) {
        task::deadline idle(sched, std::chrono::steady_clock::now() + std::chrono::seconds(30), stop);
        auto [ec, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
            [&](auto h) { s.async_read_some(asio::buffer(buf), std::move(h)); }, idle.token());
        if (ec || idle.token().requested()) { co_return; }
        // ...
    }
}

task::asio_scheduler sched(io.get_executor());
task::spawn(task::scheduler_ref::of(sched), serve(socket, sched, shutdown.token()));
```
