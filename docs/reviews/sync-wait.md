# Code review: `task::sync_wait` (`feature/sync-wait`)

Branch `feature/sync-wait` at 462c093 (efd8a0a, 462c093), base main bbb2e14, pushed (origin equals HEAD).
Reviewer: independent code reviewer, 2026-10-21. Read: the diff, `strata/docs/design/xapiand-on-strata.md` D13,
`strata/docs/reviews/calibration.md`. CI on 462c093 (run 39619486641): all eight jobs green (freebsd, macOS x86_64 and
arm64, linux gcc x86_64 and arm64, clang, tsan, asan-ubsan); the gcc jobs build the trampoline path by default.

## Verdict: APPROVE WITH CONDITIONS

The synchronization core is correct: no lost wakeup, no use-after-free of the driver frame or the task frame, and
every mutant of it is killed by the branch's own tests. Three things must be fixed before it lands as the bridge
D13 relies on (CR1 to CR3); none changes the public signature.

### Blocking (fix round, then a scoped re-check)

1. CR1: a frame allocated on the caller and freed on the worker accumulates in the worker's free list without
   bound; `sync_wait` makes that the systematic path. About 700 bytes leak per call.
2. CR2: the inline_scheduler drain runs every unrelated queued handle even when the awaited task already finished
   (and cannot stop when it has), so `sync_wait(answer())` inside `run()` reorders the simulation.
3. CR3: nothing in the test suite asserts the caller's current scheduler is restored; removing the restore passes
   every test in all six configurations.

### Non-blocking notes: CR4 to CR9 (follow-ups, no further review).

## What was checked and held

Lost wakeup, spurious wakeup, signal-before-destroy: `task.hh:466-473` publishes `error`, `value` and `done` and calls
`notify_one` under the mutex; the caller waits on the predicate (`task.hh:495`) with the mutex held, and `st`
(declared before the lock) outlives both. The driver touches `st` last in the `lock_guard` destructor; the caller can
only leave `wait` after that unlock. The task frame (`mine`) is destroyed inside the `try`, before the lock
(`task.hh:458-459`), so a task whose frame holds resources has released them before the caller wakes. Mutants:

| Mutant (copy of task.hh in `.scratch/review/mutants/`) | Repo test result |
|---|---|
| m3: destroy `mine` after signalling | killed: `CHECK(destroyed.load())` at test.cc:322 and a TSan race, sym and trampoline |
| m7: publish `done` and the result before taking the lock | killed: TSan race, sym and trampoline |
| m1: do not clear the current scheduler | killed: hang (timeout 40 s), sym and trampoline |
| m2: no drain | killed: hang, sym and trampoline |
| m4: trampoline nested swap removed | killed in the trampoline build (hang); dead code in the symmetric build, as expected |
| m5: clear the scheduler without restoring it | SURVIVES in both builds (CR3) |

Exceptions after completion: everything that can throw while the work runs is inside the `try` (`task.hh:458-466`);
the tail (`task.hh:468-473`) is lock, move, flag, notify. Its only throwing step is the move at `task.hh:471` (CR4).
Frame allocation of the driver failing leaves `t` owned by `sync_wait`'s parameter, so the task is destroyed on unwind.

Scheduler clearing: `running_on scope{scheduler_ref{}}` (`task.hh:490`) is restored by its destructor on every exit
path from the block (the only throwing call inside it is `s->run()`, whose handles cannot throw). Probes confirm
`current()` equals its prior value after `sync_wait` from a plain thread, from a `thread_scheduler` worker and
inside `inline_scheduler::run()` (probe `p_semantics.cc` steps 1 to 3; passes on the branch, fails on m5).

Nesting: `sync_wait` inside a task that itself runs under `sync_wait`, across two workers, and eight levels of
same-thread synchronous nesting (`nest_sync(50)`) return the right values in all six configurations. In trampoline
builds the nested call gets a fresh loop and the outer queue and `head` are restored by swap (`task.hh:477-487`);
the outer `transfer` loop is suspended inside `resume()` while the swap is in effect, so its `t.head`/`queue` are not
read until restored.

Concurrency: `p_asio_pool.cc` runs 8 caller threads x 5000 `sync_wait`s (values, exceptions, void) over 4 single-threaded
Asio contexts with random multi-hop tasks. Clean under native, ASan+UBSan and TSan, symmetric and trampoline.
Repo tests (`test.cc` at stress 100000, `asio_test.cc`) pass in rel, ASan+UBSan, TSan, and the same three with
`TASK_SYMMETRIC_TRANSFER=0`, built with Homebrew LLVM 23.1.2 (x86_64 macOS); `-Wall -Wextra` is silent.

Additivity: the public surface is one new function template plus new includes (`<mutex>`, `<condition_variable>`).
`inline_scheduler` layout, `scheduler_ref`, `task<T>` and every existing signature are unchanged; `run()` gains a
scope-restored thread-local store (negligible). Not an ABI change for a header-only library; see CR8 for mixed versions.

## Findings

### CR1 (blocking): cross-thread frame frees pile up in the worker's pool

`detail::frame_pool::deallocate` (`task.hh:187-196`) pushes any frame onto the freeing thread's lists without a bound,
and the file documents it (`task.hh:166`). `sync_wait` makes the one-way flow the normal case: the task frame and the
driver frame are allocated on the calling thread (`task.hh:455`, the caller's `make()`), freed on whichever thread the
task finishes on, and the caller's pool never receives them back, so it keeps calling `operator new` while the worker's
lists grow. Nothing returns them until that worker thread exits.

Probe `.scratch/review/p_pool_growth.cc`: one caller thread, one worker thread, a task with a 500-byte local that hops
to the worker, 1,000,000 `sync_wait`s:

```
after 250000 calls: maxrss 177720 KB (+174636)
after 500000 calls: maxrss 352184 KB (+349100)
after 750000 calls: maxrss 526628 KB (+523544)
after 1000000 calls: maxrss 695492 KB (+692408)
```

Linear, about 700 bytes per call. A worker that allocates frames of the same size classes itself reuses them, which
hides this in the in-tree tests; D13's pattern (an offload thread builds the seam task, strata's threads finish it)
does not guarantee that. This is the memory ratchet's "no unbounded growth".

Fix direction (verified as a mutant, `.scratch/review/mutants/cap/task.hh`): cap each size class's free list (1024
entries; excess goes to `::operator delete`). The same probe on the mutant stays flat: +932 KB at 250k, +936 KB at
1M. The cap's cost on the hot path is not measured (no benchmarks run); the designer should measure it against
`bench/bench.cc` and justify the number against the largest working set of a pool worker. Add the probe (or a
bounded version asserting RSS stays under a limit) as a regression test.

### CR2 (blocking): the drain is unconditional and cannot stop

`task.hh:492`: `if (auto* s = detail::draining_scheduler) { s->run(); }` runs after every `sync_wait` inside
`inline_scheduler::run()`, whether or not the task has finished, and `run()` drains until the queues are empty. Probe
`p_semantics.cc` step 4 (queue A, B, C; A calls `sync_wait(answer())`, a task that completes synchronously):

```
drain order (task already done): A:before B C A:after
```

A plain FIFO simulation, and the semantics of calling a synchronous task, give `A:before A:after B C`. B and C ran
nested inside A, before A resumed, for a call that never needed them. `inline_scheduler` exists "for tests and
simulation" (`task.hh:124`), where order is the point. When the task does hop to the same scheduler, B and C are
ahead of it in FIFO and running them first is right (step 4b prints the same order); but the drain then also runs
everything posted after it, including work that only becomes ready because A continues.

Fix: (a) skip the drain when `done` (read under the mutex); (b) drain only until done: add an additive
`run_until(pred)` or make the loop in `run()` a shared helper with a stop predicate, so the drain stops the moment
the driver signals. Document that unrelated queued handles can run, nested, while the caller waits (the caller's own
locals are live across them). The test at `test.cc:351` should assert order, not just the result (45).

### CR3 (blocking): scheduler restore is not asserted

Mutant m5 (`mutants/m5_no_restore/task.hh`: store an empty ref, never restore) passes `test.cc` in all six
configurations (TSan runs at stress 20000 and plain runs at 100000 in the matrix above). After the first
`sync_wait` the calling coroutine would keep running with no current scheduler, so `yield()` and `budget` become
no-ops and a later `co_await` of a child records no home. Probe `p_semantics.cc` steps 2 and 3 fail on m5
(`CHECK(task::current() == before)` at lines 45 and 65). Add them to `test.cc`: capture `task::current()` before
and after `sync_wait` from a task on a `thread_scheduler` worker and inside `inline_scheduler::run()`.

### CR4 (note): a throwing move of T at the tail terminates

`task.hh:471` moves the result into the shared state outside the `try`; a move that throws there reaches
`detached::unhandled_exception`, which calls `std::terminate` (`task.hh:385`). Probe `p_throwing_move.cc`: moves 1 to 3
(inside the task and the `try`) are caught and rethrown to the caller; move 4 (`task.hh:471`) gives `libc++abi:
terminating due to uncaught exception`. Cheap fix: build the value in place inside the `try` (emplace into the
shared state under the lock after the work finishes, or catch around the publish and store the error).

### CR5 (note): T must be move-assignable

`task.hh:471` assigns `std::optional<result>`; a result type that is move-constructible but not move-assignable
(a struct with a reference member) does not compile (`p_nonassignable.cc`). The pre-existing `run` has the same
limit at `task.hh:432`, so this is consistent, not a regression; `emplace` in both would lift it.

### CR6 (note; for the architect): deadlocks are documented for threads, undetected, and the drain contract is silent

The header comment and README say never to block a thread the awaited work needs. Probe `p_deadlock.cc`
(watchdog, exit 3 = hang): mode 1, a `thread_scheduler` worker calls `sync_wait` on a task that hops back to that
worker, hangs; mode 2, inside `X.run()` the task hops to a different `inline_scheduler` Y, hangs. Neither is
detectable without the task's cooperation, because blocking while another thread completes the task is legitimate
(and `sync_inside` in the tests relies on it), so refusal is not available on the current thread-local state. One
detectable slice exists: a debug-build check, in a `sync_wait` call from a thread with a non-empty `current()`, that the
task's first hop does not post to that same scheduler (the first segment runs on the caller, so a thread-local
"blocked on" ref could be checked in `schedule_on`). That is a design choice (hot-path cost, partial coverage) for
the architect; the header comment should at least state the drain case and that only `inline_scheduler` is drained.

### CR7 (note): the CMake target does not link Threads

`CMakeLists.txt:7` says task.hh has no dependencies; it now uses `std::mutex`/`std::condition_variable`. Strata and
Detent link Threads already, and glibc 2.34+ merged libpthread, so this only bites older glibc consumers of
`task::task`. Add `target_link_libraries(task INTERFACE Threads::Threads)` and correct the comment.

### CR8 (note): `inline_scheduler::run` changed in an inline header

Strata and Detent link task.hh into one process; if two libraries in a binary were built against different task.hh
versions, the linker picks one `inline_scheduler::run` (ODR), and an old `run()` never sets `draining_scheduler`,
so `sync_wait` inside it would hang. Pin both to the version carrying this change at the same time.

### CR9 (note): tests

`test.cc:192-193` `sync_inside`'s `inner_home` parameter only feeds `+ (inner_home ? 0 : 1)`, which proves nothing;
drop it. The file header comment (`test.cc:3`) reads "..., and a sync_wait, and a randomized ...". The nested trampoline
swap is exercised only because the driver itself runs inside the loop; add a direct nested test with an explicit
assertion that the inner `sync_wait` made progress on the fresh loop (m4 is killed today only by a hang).

## Frame-pool rules

The driver is a captureless lambda coroutine (`task.hh:455`), so no captured state outlives the closure. Its frame
comes from `detached::promise_type : pooled_frame` and is freed on whichever thread finishes it (documented rule;
see CR1 for its cost). The sized `operator delete` matches the allocation class. No new allocation path outside the
pool.

## Probes and artifacts (all in `.scratch/review/`, kept)

`build.sh`, `all.sh` (the six-configuration repo build), `common.hh`, `p_semantics.cc` (restore, drain order,
nesting, caller-thread completion), `p_asio_pool.cc` (concurrent callers over 4 contexts), `p_pool_growth.cc` (CR1),
`p_throwing_move.cc` (CR4), `p_nonassignable.cc` (CR5), `p_deadlock.cc` (CR6), `run_mutants.sh` with
`mutants/{m1_no_clear,m2_no_drain,m3_late_destroy,m4_no_nest,m5_no_restore,m7_unlocked_publish,cap}/task.hh`,
and logs in `out/`.

## Conditions to land

1. CR1: bound the per-class free lists (or an equivalent), with a regression test; measure the cap against `bench/bench.cc`.
2. CR2: drain only when not done, and stop when done; test the order.
3. CR3: assert `current()` is restored in the tests (plain thread worker and inline drain).
4. CR4 to CR9 as follow-ups; CR6 goes to the architect.

Not run: the default 1,000,000-task stress (100,000 locally; CI ran the default), Linux/FreeBSD/arm64 builds
(CI is green on them), gcc locally, the benchmark suite, any measurement of the CR1 cap's hot-path cost, and
a Detent or strata integration build.
