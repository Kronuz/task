> Status: CURRENT (2026-10-29) code review of `phase7c/l1-task` at task:78e48da; supersedes nothing.

# Code review: phase 7c step L1 (task:78e48da)

Verdict: **APPROVE WITH CONDITIONS**. One blocker (CI red on linux-tsan, a test bug), one
condition worth fixing in the same round (N1). No library lifetime or concurrency defect found.

Probes (kept, `.scratch/review-l1/`): `r8_probe.cc`, `stress.cc`, `misc.cc`, `build.sh`
(`./build.sh asan|tsan`, Homebrew LLVM 23, `-O1 -g`), outputs in `asan/` and `tsan/`.

## Blockers

### L1-B1 CI linux-tsan red: detached thread in `test_primitives` (test bug, not library)
Run 40978716961: race between `pthread_cond_destroy` in `~io_context` and `pthread_cond_signal`
in `post_immediate_completion` from the thread at `test/asio_test.cc:302`.
- That thread is `std::thread(...).detach()` calling `h(41)`. The handler posts the resume to
  `s3` (`resume_on`, task_asio.hh:181) and returns; the t3 reactor resumes the task, sets
  `finished`, and the test's `sleep_for` poll (:309) falls through, `t3.join()`s and leaves the
  block, destroying `io3` while the detached thread is still inside Asio's `post` (after the
  work item is visible, before the signal returns). Nothing orders the detached thread's tail
  with the destruction; the poll is not a synchronization edge.
- Library side checked and clean: after `resume_on` the handler touches nothing of `*self`
  (task_asio.hh:181 comment holds), `start_timer` timers own no executor state, and
  `~asio_scheduler` only asserts `demoted()==0`. An Asio handler invoked on a foreign thread
  against an `io_context` being destroyed is Asio's own contract, not `await_asio`'s.
- Fix: keep the thread as `std::thread th` and `join()` it (after `finished`, before `io3`
  dies; the handler completes first so the join cannot deadlock). Not reproducible on macOS
  TSan (25 runs of `tsan/asio_test` clean); needs the Linux CI to confirm.

## Checks asked for

1. **R8 (binding): PASS.** `r8_probe.cc` is the architect's two scenarios against the real
   `task::async_mutex` with a hand-made scheduler (own `post`/`now`/`start_timer`, movable timer
   object, timer completion consumed after call). Changed from the original: the `node/frame/
   mutex_model` model replaced by `async_mutex` + a waiter task whose frame ends when it
   returns; "io_queue" is the scheduler's queue; order A fires the timer callback first, order B
   queues the callback (uncancellable) before `unlock()` grants. ASan+UBSan: A `res=2`, lock free
   after the holder's unlock; B `res=1`, late timer harmless; zero reports. (My first run of the
   probe was a probe bug: a non-movable fake timer cancelled itself on `emplace`; fixed.)
2. **Waiter block: PASS.** Read: `settle` is the single CAS (task.hh:~762); grant settles under
   `m_`, wakes after dropping it; timeout never touches the mutex; `park`/`wake` exchange gives
   exactly-once continuation; refs: queue + awaiter + one per `lock_timeout`, moved-from
   `lock_timeout` releases nothing. `stress.cc` (asio_scheduler on a 4-thread io_context, 24
   workers x 60 rounds, mixed `lock()` / `lock_for(0-300us)`, a plain-int critical section,
   in-CS counter, mutex destroyed right after the last task, events set from another thread
   twice, 30 iterations): TSan clean (19.6k grants, 23.6k timeouts), ASan+UBSan clean (40 iters).
   `misc p4`: mutex destroyed with a cancelled/pending timer, 200x, clean in both.
3. **R2 slice: PASS.** `misc p2`: a task suspended 30 ms on an event is not exhausted on resume;
   a 20 ms busy run against a 5 ms budget is. `inline_scheduler` re-stamps
   (task.hh:~216). The developer's mutants m2/m2b cover the stamp.
4. **`await_asio` lifetime: PASS with a note (N3).** Cancellation completes the handler
   (tested at asio_test.cc:~289). The completion handler runs only before the frame ends by
   contract; handler stores through `self` then posts and touches nothing after.
5. **`event`: PASS.** FIFO (stack reversed in `set()`, tested incl. mutant m10), `set()` reads
   `next` before resuming, idempotent, no lost wakeup (push CAS against the marker), destructor
   assert (fork test). Stress: 64 waiters split around a concurrent `set()`, TSan clean.
6. **API: PASS.** Additive: `cycles` moved above the schedulers, same names. `residency`
   (pins task:bbb2e14) `test/test.cc` compiles `-fsyntax-only` against the branch headers;
   Xapiand (pin task:aa447e7, `TASK_ASIO OFF`) uses `task<T>` only and has no `event`,
   `async_mutex` or `using namespace task`; its TU check could not run here (missing include
   unrelated to task). Uncontended path: `try_lock` is one `std::mutex` lock/unlock and no
   allocation; the pooled block is allocated only when queueing. Not measured (benchmarks are out of scope for this review).

## Non-blocking notes

- **N1 (fix with B1, UB):** `lock_for(s, nanoseconds::max())` overflows signed `rep`
  (task.hh:947): UBSan `signed integer overflow 2414394469105626 + 9223372036854775807`
  (`misc p1`). "Wait forever" is a natural use. Saturate the sum (and `duration_cast`).
- **N2:** timed-out entries are shed only from the queue head (task.hh:958 and `unlock`). Behind
  a long-held lock with a live head, dead entries accumulate, one pool block each, bounded by
  arrival rate x hold time. Documented; fine for now.
- **N3:** `await_asio` keeps `init_` inside the awaiter and the handler holds a raw pointer; an
  operation that completes inline with no recorded scheduler resumes (and may finish) the frame
  from inside `init_`. Safe unless `init_` uses its captures after calling the handler; worth a
  sentence in the doc comment. A frame destroyed before completion is UB (documented).
- **N4:** a plain `lock()` taken with no current scheduler resumes the next waiter inline in
  `unlock()`, so a chain of such waiters recurses O(n) deep.
- **N5:** `inline_scheduler::run_until` writes `slice_start` without restoring the enclosing
  scope's stamp after each handle; only matters if nested inside another scheduler's resume.
- **N6:** the mutex destructor asserts only `!locked_`; this implies an empty live queue (an
  unlocked mutex has shed its queue), and with `NDEBUG` nothing checks at all.

Not run: benchmarks and the uncontended-cost measurement; the developer's own three test
binaries (their ledger records clean ASan/TSan; I ran my probes and `asio_test` under TSan only);
Linux/FreeBSD builds and the CI TSan failure itself (left to CI); the Xapiand TU build; a
mutant of my own against `r8_probe` (the developer's m1 covers that: a frame-owned block).
