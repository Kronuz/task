# AGENTS.md — task

## Repo map

```
task.hh            task<T>, scheduler concept and scheduler_ref, inline_scheduler, frame pool,
                   spawn/run/sync_wait, yield/schedule_on, budget, cancellation, event,
                   cancel_token/source/callback, deadline, async_mutex (lock(token)), when_all.
task_asio.hh       asio_scheduler (demoted, start_timer, roots, close/destroy_roots), await_asio
                   with an optional token (target task::asio; fetches Asio 1.36.0).
test/test.cc       Core tests; argv[1] sets the stress task count.
test/sync_test.cc  Slice, event, deadline, async_mutex, lock(token) orders, destroyed waiters,
                   event waiters at teardown, destructor asserts.
test/asio_test.cc  asio_scheduler and await_asio: tokens, wrappers, the teardown phases, sizes.
test/pool_off_test.cc  TASK_FRAME_POOL=0: every frame is an operator new.
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
