# AGENTS.md — task

## Repo map

```
task.hh            umbrella over task/*.hh (scheduler, drivers, frame_pool, coroutine, cancel, timing,
                   sync): task<T>, schedulers, spawn/run/sync_wait, cancellation, deadlines, event,
                   async_mutex, when_all. docs/source-layout.md lists each piece and the split tests.
task_timers.hh     group, periodic, debounce: housekeeping on the loop's wheel (needs task.hh only).
process.hh         run_daemon, ProcessLock, locked_pid (POSIX; standard library only).
lifecycle.hh       component, components, stop_bound/stop_timing, stop_source, stop_signals,
                   lifecycle (POSIX; needs task.hh only).
task_asio.hh       asio_scheduler (demoted, start_timer, roots, close/destroy_roots), await_asio
                   with an optional token (target task::asio; fetches Asio 1.36.0).
test/test.cc       Core tests; argv[1] sets the stress task count.
test/sync_test*.cc  Slice, event, deadline, async_mutex, lock(token) orders, destroyed waiters,
                   event waiters at teardown, destructor asserts.
test/asio_test*.cc  asio_scheduler and await_asio: tokens, wrappers, the teardown phases, sizes.
test/pool_off_test.cc  TASK_FRAME_POOL=0: every frame is an operator new.
test/deadline_test.cc  Deadlines and timers on inline_scheduler's wheel: reentry, reference model,
                   far deadlines, post_call. test/wheel_mutant.py builds it on a mutant wheel.
test/deadline_asio_test.cc  asio_scheduler's wheel: off-loop deadlines, teardown with roots,
                   two loops. test/deadline_alloc_test.cc  no allocation per deadline.
test/blocking_test.cc  blocking_scheduler: cross-thread wakes, lost wakes, the blocking entry.
test/timers_test.cc  sleeps, group, periodic and debounce on the manual clock; a sleep on Asio;
                     a sleep's fire against a held cross-thread cancel.
test/process_test.cc  run_daemon's ready, error and timeout reports; ProcessLock and locked_pid.
test/lifecycle_test.cc  stop order, bound, hurry, start failure, nesting, stop sources, and
                   three SIGTERMs in a child process.
bench/bench.cc     No-op task cost and allocations; Asio yield latency; the same two measurements on asio::awaitable.
docs/API.md        The types and functions, and how to run a task.
```

## Invariants

- Every scheduler sets `task::running_on` around each resume; affinity and the budget slice
  depend on it (the scope stamps the slice start, so one scope per resume, not per loop).
- A lock_waiter (async_mutex) is shared by the queue and the awaiter; only the last reference
  frees it. A cancel callback borrows the awaiter's reference, so the awaiter destroys the
  registration before releasing it. Never point a queue entry at the awaiter itself.
- A cancel callback runs with no lock held; deregistration waits for it on another thread.
  A timer callback (deadline) holds the cancel state and checks the disarm bit.
- The core header never includes Asio.
- Hot paths allocate nothing after warm-up; the bench counts allocations to prove it.

## Verify a change

```sh
cmake -S . -B build && cmake --build build && ctest --test-dir build
cmake -S . -B asan -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined" && cmake --build asan && ctest --test-dir asan
cmake -S . -B tsan -DCMAKE_CXX_FLAGS="-fsanitize=thread" && cmake --build tsan && ctest --test-dir tsan
build/task_bench
```
