> Status: CURRENT (2026-11-02) the ledger for task's cancellation design (W2a, task half); nothing supersedes it.

# cancellation

- Design: `docs/design/cancellation.md` revision 2 (this branch); revision 1 at task:6d361ca.
  Reviews: architect, http `docs/reviews/task-handlers-rev2-design.md` at http:1ae1390, REJECT
  (2026-11-02, CN-L1, CN-L2, C8-C11; Q1-Q3 ruled (a)); Xapiand lead,
  `docs/reviews/cancellation-xapiand.md` (2026-11-02, XN1-XN3); pointer
  `docs/reviews/cancellation-design.md`.
- Branch / worktree: `design/cancellation` at `~/code/Kronuz/task-wt/cancellation`; step C1 on
  `phase/cancellation-c1` at `~/code/Kronuz/task-wt/cancellation-c1`; head: `git log -1`.
- State: C1 (D1-D4) implemented, code review conditions CR1-CR6 folded in (2026-11-02), ready for the scoped re-check; C2 (D5, D7-D10) and C3 (D6)
  not started. Re-check CLEARED WITH CONDITIONS (2026-11-02, `docs/reviews/cancellation-design.md`).
- Done criteria (set by the lead; contributors tick, never edit; they apply to steps C1-C3):
  - [ ] K1 Every decision's T1 tests pass, and each fails with its fix reverted (the revert and the
    failure recorded here once per decision, D1-D8)
  - [ ] K2 The review's probes rerun clean on the branch headers (T3): teardown probes
    (`crossroot`, `holder` in both orders, `grant_destroyed`, `dispatch_inline`, revision 1's
    `teardown`) ASan clean; race probes (`race`, the cross-thread cancel, `late_fire`) TSan clean;
    `channel`, `composed` unchanged; results in this worktree's `.scratch/` `results.txt`
  - [ ] K3 task's existing suite (`test/test.cc`, `sync_test.cc`, `asio_test.cc`) and benches
    build and pass in `build-rel`, `build-asan`, `build-tsan` and g++ Debug ASan (T2); T4 rows
    reported by the lead
  - [ ] K4 Zero live frames: `live_frames()` back to baseline in every T1 (D8) test, both pool
    paths counted
  - [ ] K5 CI green on every job of `.github/workflows/ci.yml` at the landing commit, Linux ASan
    (LSan) included
  - [ ] K6 Consumer rebuild (T6): strata, prospect, residency build and pass against the branch;
    Xapiand by the Xapiand lead on the C3 pin, with `take_turn` migrated to `lock(token)` and
    `Host` forwarding `roots`, `runs_here`, `post_call`
  - [ ] K7 T7 greps empty (`as_awaitable`; `lock_until`/`lock_for` in task and, at its wave
    commit, Xapiand)
  - [ ] K8 (added per re-check CN-R3) D8.4 unlocks in the awaiter's destructor only when the
    waiter's state is `granted`; T1 (D8) gains `grant_timedout` (a token-bound waiter that timed
    out, then destroyed, leaves the holder's lock held) beside `grant_granted`
  - [ ] K9 (added per re-check NF1) D5.3: before `initiating` is set, a cross-thread cancel only
    records the request (or the early-out and throw paths drop the handler's share and suspend
    while the count is above 2); T1 (D5) gains the re-check's `earlyout` and a throwing `init`,
    ASan clean, each failing with the fix reverted
  - [ ] K10 (added per re-check NF2, NF3) `~event` keeps a Debug count of orphaned waiters and
    unlinks only during teardown; a T1 death test for the ownership rule (`own_a_first`: a mutex
    destroyed with a held guard or granted waiter asserts in Debug)
  - [ ] K11 (added per re-check TH2-L1 note and the lead) D8.7 states `close()` and
    `destroy_roots()` are idempotent and called on the owner thread after phase 1's join; Limit 3
    states that `spawn` onto a closed scheduler destroys the task and never calls `done`, so a
    blocking `run` on it returns rather than hangs (or document the hang and test it)
  - [ ] K12 (added by the lead from http S1) T7 can count task frames against asio's: a build
    switch that bypasses task's per-thread frame pool, or a pool-miss and pool-hit count exposed
    for the bench
- Done: revision 1 (task:6d361ca); Xapiand lead's notes recorded (task:3686135); review pointer
  (task:0585469); revision 2 and this ledger (task:1132c04).
- Done (C1): D1-D4, D8.4's destructor rule with CN-R3's state test (K8), `live_frames()`,
  docs (see "C1 evidence"); commits: `git log design/cancellation..phase/cancellation-c1`.
- Doing: nothing.
- Next:
  1. Scoped re-check of the C1 fixes (CR1-CR5); the lead watches CI.
  2. C2 (D5, D7-D10) against C1's interface: D8.4 and `live_frames()` already exist, so C2 adds
     the `event` orphaning, `root_list`, `close()`, `destroy_roots()`, K9, K10's `~event`
     count, K11, K12, and `~deadline`'s Debug `runs_here() || closed()` assertion (not in C1:
     no scheduler declares either yet).
  3. C3 (D6), then the Xapiand lead's review of the task diff, then landing.
  4. C2's test list also carries two items from the C1 code review: (CR3/X3) a test that destroys a
     frame while a callback runs on another thread against its waiter, killing the mutant that drops the
     explicit `cb_.reset()` before `abandon` in `~cancellable_lock_awaiter` (`task.hh:1238-1241`;
     it survives the C1 suite); (CR6) the precondition that a frame is not destroyed between
     `unlock()`'s grant (`settle(granted)`) and its `wake()` (a destroyed frame there makes `wake()`
     post a dead handle): D8's phase ordering (no concurrent destruction) states it and the T1 (D8)
     set includes it.
  5. T6 for C1 (strata, prospect, residency against this branch) not run: host load (28-74 on
     16 CPUs) and disk (17-20 GiB free) kept it out of this step; it is K6 at the C3 pin.
- Blocked on: nothing.
- Findings folded in: CN-L1/CN-R1, CN-L2/CN-R2, CN-R3, CN-R4 (C8), CN-R5 (C9), CN-R6 (C10),
  CN-R7 (C11), XN1-XN3, Q1-Q3 rulings, TH2-L1 (task's API, D8.7): see the log.
- Last verified: C1 built and tested in four builds (2026-11-02, "C1 evidence"); design
  citations checked (2026-11-02). Citations checked against
  task:8cad661 (identical headers at this head), reactor:ade618d, Xapiand:81c311799.

## C1 evidence (2026-11-02)

Builds, each its own directory, via `.scratch/c1/build.sh KIND` under `quiet light` (4 jobs):
`build-rel` (Apple clang, `-O2 -DNDEBUG`), `build-asan` (Homebrew clang 23, ASan+UBSan,
`-fno-sanitize-recover=undefined`), `build-tsan` (Homebrew clang 23), `build-gcc` (g++ 16,
Debug, ASan+UBSan). `ctest`: 3 of 3 pass in each (task, task_sync, task_asio), no compiler
warnings, no sanitizer report (TSan exits 66 on any report). These are K3's builds at C1; K3
stays open until C3.

T1 tests (D1-D4): `test/test.cc` `test_cancel_tokens`; `test/sync_test.cc` `test_deadline`,
`test_lock_token` (A-K), `test_lock_destroyed`, and the ownership death tests in
`test_destructor_asserts`; `test/asio_test.cc` the real-timer deadline, `late_fire`, and the
former `lock_until` cases on `lock(deadline.token())`. `lock_for`'s clock case is gone with
`lock_for`.

Reverts (K1's D1-D4 part, and K8): each fix broken once by `.scratch/c1/mutants.py`, which
edits `task.hh`, rebuilds and reruns the test, then restores it; output in
`.scratch/c1/mutants.txt`.

| Mutant | Broken | Build | Caught by |
| --- | --- | --- | --- |
| D1a | a later request overwrites the reason | rel | `test.cc` first-wins checks; one winner of 8 racing threads |
| D1b | state from `operator new`, not the frame pool | rel | 10,000 sources add 0 `operator new` |
| D2a | deregistration does not wait for a callback on another thread | tsan | value 42 after `reset()`; TSan data race |
| D2b | a throwing callback is not caught | rel | terminate (uncaught `runtime_error`) |
| D2c | self-deregistration waits for itself | rel | hang (killed at 60 s) |
| D2d | `request()` does not hold the state while callbacks run | asan | use-after-poison in `cancel_state::request` |
| D3a | a linked child does not register on its parent | rel | parent request does not reach the child |
| D3b | a parent requested between the check and registration leaves the child unrequested | rel | the `before_link` hook case (the first run survived; the hook test was added for it) |
| D3b2 | a child of a requested parent is never born requested | rel | born-requested and hook cases |
| D3c | a dying child stays registered on its parent | asan | use-after-poison in `cancel_state::request` |
| D3d | a dying child keeps its parent's state | rel | `live_frames()` and allocation counts |
| D4a | the timer requests without the disarm check | rel | `sync_test.cc` queued expiry (two cases); `asio_test.cc` `late_fire` |
| D4b | a requested token queues on a held lock | rel | case H (`live_frames()`) |
| D4c | `lock(token)` registers no callback | rel | cases A and C, then abort |
| D4d | the callback resumes directly, not through park/wake | rel | crash (SIGBUS) in case E |
| K8a | D8.4 as first worded: unlock whenever `settle(timed_out)` fails | rel | `grant_timedout` |
| K8b | a destroyed waiter only drops its reference (the pre-C1 code) | rel | `grant_granted`, `grant_waiting` |

K4 (C1 part): `live_frames()` counts in `pooled_frame::operator new`/`delete` (both pool
paths; `TASK_COUNT_FRAMES`, opt-in and set by `test.cc` and `sync_test.cc`); every C1 test group
asserts its baseline, cancel states and lock waiters included. The D8 tests are C2's.

K10 (C1 part): `own_a_first` (a mutex local to root A, root B queued, A destroyed first)
aborts on `assert(!locked_)` in Debug; `own_b_first` does not. The `~event` count is C2's.

T3 (`late_fire`, C1 part): `.scratch/arch-review-rev2/probe_c1.cc` ports `late_fire` to a
`deadline` destroyed by the first timer's handler and reruns `grant_destroyed`; results in
`.scratch/arch-review-rev2/results_c1.txt`.

Smoke: `cancel_token::requested()` at `-O2` is a null test and one load (asm in
`.scratch/c1/smoke.txt`); an empty token, its copies and a budget over it call 0
`operator new` (`test.cc`, D1 block). Nothing was timed.

Notes for the reviewer: `lock()` keeps the unchanged `lock_awaiter`; `lock(token)` is an
overload returning `cancellable_lock_awaiter`, so frames that never pass a token do not grow.
`TASK_TEST_HOOKS` adds two test-only interleaving points (`after_enqueue`, `before_link`).
`cancel_source::request(cancel_reason::none)` records `requested`, so a requested token always
has a reason. D8.4 landed in C1 because the new awaiter's destructor had to be written.

## C1 fix round (code review CR1-CR6, 2026-11-02)

Builds rel, ASan+UBSan and TSan (Homebrew clang) via `.scratch/c1/build.sh` under `quiet light`,
each in its own directory, removed after: 3 of 3 ctest pass in each, no sanitizer report.

| ID | Change | Test | Revert (K1) |
| --- | --- | --- | --- |
| CR1 | `grant_then_request` in `sync_test.cc` `test_lock_destroyed` (ports `probe_grantcancel.cc`) | grant, then `request()` before the resume runs: no second post, one resume, guard held | reviewer's X4 (`w->settle(timed_out); w->wake();` in `lock_cancel`, `task.hh:1117`) in the TSan build: `task_sync` dies with SEGV (the double resume); passes on the branch |
| CR2 | `cancel_state::requested()` loads with acquire; contract in design D1, `docs/API.md`, `ARCHITECTURE.md` | `test_cancel_tokens` poller (ports `probe_order.cc`): writes `data`, then `request()`; the poller reads `data` after seeing `requested()` | `requested()` relaxed, TSan build: data race reported in the poller, `task` fails; passes with acquire |
| CR3 | count corrected to 17 (`.scratch/c1/mutants.py`); X3 and CR6 moved to Next 4 | none (C2) | none |
| CR4 | D4.1 text: `lock()` is `lock_awaiter` exactly; `lock(token)` is a separate overload returning the 80-byte `cancellable_lock_awaiter`, an empty token behaving as `lock()` | none (text) | none |
| CR5 | `TASK_COUNT_FRAMES` defaults to 0 in every build type; `test.cc` and `sync_test.cc` define it to 1 | existing `live_frames()` baselines | none |
| CR6 | recorded in Next 4 | none (C2) | none |

CR2 codegen of `poll()` (`.scratch/c1/poll.cc`, `-O2`, `clang++ -target`): x86-64 is unchanged (null test,
one `movq (%rax),%rax`; an x86 load is already an acquire); arm64 is `ldapr x8, [x8]` (the RCpc
acquire load; `ldar` on targets without RCpc). Asm in `.scratch/c1/poll-*.s`. Nothing was timed.

## Log
- 2026-11-02: C1 (D1-D4) on `phase/cancellation-c1`: reasons, callbacks, linked sources,
  `deadline` with the disarm bit, `lock(token)` replacing `lock_until`/`lock_for`, D8.4's
  destructor (K8), `live_frames()`; four builds green; 17 mutants caught (see "C1 evidence").
- 2026-11-02: C1 code review (APPROVE WITH CONDITIONS) folded in: CR1-CR6 (see "C1 fix round").
- 2026-11-02: revision 2 of `docs/design/cancellation.md` folds in the architect's REJECT
  (http:1ae1390) and the Xapiand lead's notes; "Review response" table by finding ID; ledger
  created; State ready for re-check (task:1132c04).
