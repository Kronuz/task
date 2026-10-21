# AGENTS.md — task

## Repo map

```
task.hh            task<T>, scheduler concept and scheduler_ref, inline_scheduler, frame pool,
                   spawn/run/sync_wait, yield/schedule_on, budget, cancellation, when_all.
task_asio.hh       asio_scheduler and as_awaitable (target task::asio; fetches Asio 1.36.0).
test/test.cc       Core tests; argv[1] sets the stress task count.
test/asio_test.cc  Asio bridge: yields, demotion, results, exceptions.
bench/bench.cc     No-op task cost and allocations; Asio yield latency.
```

## Invariants

- Every scheduler sets `task::running_on` around each resume; affinity depends on it.
- The core header never includes Asio.
- Hot paths allocate nothing after warm-up; the bench counts allocations to prove it.

## Verify a change

```sh
cmake -S . -B build && cmake --build build && ctest --test-dir build
cmake -S . -B asan -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined" && cmake --build asan && ctest --test-dir asan
cmake -S . -B tsan -DCMAKE_CXX_FLAGS="-fsanitize=thread" && cmake --build tsan && ctest --test-dir tsan
build/task_bench
```
