> Status: CURRENT (2026-11-02) the ledger for task's cancellation design (W2a, task half); nothing supersedes it.

# cancellation

- Design: `docs/design/cancellation.md` revision 2, with C1's and C2's text fixes (NF1-NF3,
  CN-R3 wording, CR6, D8.7, Limit 3: task:1c30b8b); revision 1 at task:6d361ca.
  Reviews: architect, http `docs/reviews/task-handlers-rev2-design.md` at http:1ae1390, REJECT
  (2026-11-02, CN-L1, CN-L2, C8-C11; Q1-Q3 ruled (a)); re-check CLEARED WITH CONDITIONS
  (`docs/reviews/cancellation-design.md`); Xapiand lead, `docs/reviews/cancellation-xapiand.md`
  (XN1-XN3); C1 code review `docs/reviews/cancellation-c1-code.md` (CR1-CR6, X3).
- Branch / worktree: C2 on `phase/cancellation-c2` at `~/code/Kronuz/task-wt/cancellation-c2`
  (from task:b406766, C1 landed); head: `git log -1`.
- State: C2 (D5, D7-D10, K9-K12) implemented and proven (see "C2 evidence"), ready for code
  review; C3 (D6) not started.
- Done criteria (set by the lead; contributors tick, never edit; they apply to steps C1-C3):
  - [ ] K1 Every decision's T1 tests pass, and each fails with its fix reverted (the revert and the
    failure recorded here once per decision, D1-D8): D1-D4 in "C1 evidence"; D5, D7, D8 (and D9,
    K12) in "C2 evidence"; D6 is C3's
  - [x] K2 The review's probes rerun clean on the branch headers (T3): teardown probes
    (`crossroot`, `holder` in both orders, `grant_destroyed`, `dispatch_inline`, revision 1's
    `teardown`) ASan clean; race probes (`race`, the cross-thread cancel, `late_fire`) TSan clean;
    `channel`, `composed` unchanged; results in this worktree's `.scratch/` `results.txt`:
    `.scratch/arch-review-rev2/results.txt` (27 of 27 runs exit 0, no sanitizer report),
    `.scratch/arch-review/results.txt` (`teardown` 1 of 1, `cancel` 0.0-0.1 ms in all three
    builds); the cross-thread cancel is T1 (10,000 iterations, TSan clean)
  - [ ] K3 task's existing suite (`test/test.cc`, `sync_test.cc`, `asio_test.cc`) and benches
    build and pass in `build-rel`, `build-asan`, `build-tsan` and g++ Debug ASan (T2); T4 rows
    reported by the lead: C2's four builds pass (see "C2 evidence"); T4 is the lead's, after C3
  - [x] K4 Zero live frames: `live_frames()` back to baseline in every T1 (D8) test, both pool
    paths counted: every D8 block in `asio_test.cc` `test_teardown`, `test_wrapper`,
    `test_await_token`, and `sync_test.cc` `test_event_teardown`, `test_lock_destroyed` asserts
    its baseline; a 64 KiB frame (operator new path) raises the count and returns to it
  - [ ] K5 CI green on every job of `.github/workflows/ci.yml` at the landing commit, Linux ASan
    (LSan) included: green on all 8 jobs at task:1c30b8b (run 41543246183); the landing commit is later
  - [ ] K6 Consumer rebuild (T6): strata, prospect, residency build and pass against the branch;
    Xapiand by the Xapiand lead on the C3 pin, with `take_turn` migrated to `lock(token)` and
    `Host` forwarding `roots`, `runs_here`, `post_call`: not run (C3 pin)
  - [ ] K7 T7 greps empty (`as_awaitable`; `lock_until`/`lock_for` in task and, at its wave
    commit, Xapiand): both greps empty in task at task:1c30b8b; Xapiand's at its wave commit
  - [x] K8 (added per re-check CN-R3) D8.4 unlocks in the awaiter's destructor only when the
    waiter's state is `granted`; T1 (D8) gains `grant_timedout` (a token-bound waiter that timed
    out, then destroyed, leaves the holder's lock held) beside `grant_granted`: C1 (mutants K8a,
    K8b in "C1 evidence"); both pass in C2's four builds
  - [x] K9 (added per re-check NF1) D5.3: before `initiating` is set, a cross-thread cancel only
    records the request (or the early-out and throw paths drop the handler's share and suspend
    while the count is above 2); T1 (D5) gains the re-check's `earlyout` and a throwing `init`,
    ASan clean, each failing with the fix reverted: the second option; `test_await_token` modes
    1-2 and the throwing-init pair; mutants D5a, D5b (ASan use-after-poison)
  - [x] K10 (added per re-check NF2, NF3) `~event` keeps a Debug count of orphaned waiters and
    unlinks only during teardown; a T1 death test for the ownership rule (`own_a_first`: a mutex
    destroyed with a held guard or granted waiter asserts in Debug): `detail::orphaned_waiters()`,
    `test_event_teardown`; `own_a_first`/`own_b_first` in `test_destructor_asserts`; D8 text
    states the teardown rule; mutants D8d-D8f
  - [x] K11 (added per re-check TH2-L1 note and the lead) D8.7 states `close()` and
    `destroy_roots()` are idempotent and called on the owner thread after phase 1's join; Limit 3
    states that `spawn` onto a closed scheduler destroys the task and never calls `done`, so a
    blocking `run` on it returns rather than hangs (or document the hang and test it): both
    stated; a `done` owning a `std::promise` returns `broken_promise` (test), one waiting on a flag
    hangs (documented; mutant D8b shows the hang when the task is linked instead)
  - [x] K12 (added by the lead from http S1) T7 can count task frames against asio's: a build
    switch that bypasses task's per-thread frame pool, or a pool-miss and pool-hit count exposed
    for the bench: `TASK_FRAME_POOL=0`; `test/pool_off_test.cc`; mutant K12a
- Done: revision 1 (task:6d361ca); revision 2 (task:1132c04); C1 (landed, task:b406766).
- Done (C2): task:213020d (code and tests: D5, D7-D10, K9-K12, X3, CR6), task:1c30b8b (design
  text and docs), and this ledger.
- Doing: nothing.
- Next:
  1. Code review of C2 (the lead launches it); fold findings in.
  2. C3 (D6): inline resume through the count's last drop; T3 `posts`, `nocurrent` (still 6.00
     io handlers per round trip and 19-20 checkpoints at C2, as expected); the on-loop cancel
     then ends a read in 2 `run_one`s instead of 3 (`test_await_token` checks `<= 3`).
  3. T6 (K6) at the C3 pin: strata, prospect, residency; Xapiand by its lead.
- Blocked on: nothing.
- Findings folded in: CN-L1/CN-R1, CN-L2/CN-R2, CN-R3, CN-R4 (C8), CN-R5 (C9), CN-R6 (C10),
  CN-R7 (C11), XN1-XN3, Q1-Q3 rulings, TH2-L1 (D8.7), NF1 (K9), NF2-NF3 (K10), the TH2-L1 note
  (K11), X3 and CR6 (C1 review): see the log.
- Last verified: 2026-11-02, C2 at task:1c30b8b: four builds green, 19 C2 mutants caught,
  probes clean, CI green (see "C2 evidence").

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

## C2 evidence (2026-11-02)

Builds via `.scratch/c2/build.sh KIND` under `quiet light` (4 jobs), each its own directory,
removed after: `build-rel` (Apple clang `-O2`), `build-asan` (Homebrew clang, ASan+UBSan,
`-fno-sanitize-recover=undefined`), `build-tsan`, `build-gcc` (g++ 16, Debug, ASan+UBSan).
`ctest`: 4 of 4 pass in each (task, task_sync, task_pool_off, task_asio), no compiler warning,
no sanitizer report. Output in `.scratch/c2/sanitizers.txt`.

T1 tests: `asio_test.cc` `test_await_token` (D5: requested before start, on-loop and
cross-thread requests between registration and the check, throwing `init` with and without a
posted emit, dispatch and direct handlers inside `init` with and without a token on the loop and
with no scheduler, on-loop cancel of a pending read, a lost race returning 4 bytes, empty token
binds no slot, 10,000 cross-thread cancels on a running loop), `test_wrapper` (D7: a `Host`-shaped
virtual wrapper; a cross-thread cancel through its `post_call`, its spawn a root destroyed at
teardown), `test_teardown` (D8: one loop with roots in `await_asio` with and without a token, an
`event`, two lock waits, a demoted checkpoint, a `when_all`, and a 64 KiB frame; `crossroot`;
`holder` both orders; an `event` set by a dying frame, waiter on another loop, both orders; a
destructor spawning in phase 3; a frame destroyed with a cross-thread emit pending; spawn onto a
closed scheduler with a `std::promise` `done`; closed posts dropped; idempotent phases; the
destructor's own phases); `test_sizes` (T5); death tests `scheduler_with_demoted` (live roots and
an unstopped loop assert), `deadline_off_loop`. `sync_test.cc`: X3 in `test_lock_destroyed`,
`test_event_teardown`, `~event` orphaning in `test_destructor_asserts`. `test.cc`: D9 ownership
(vector and variadic) and 0 `operator new` for 1,000 variadic joins. CR6 is stated in D8.4 and
`ARCHITECTURE.md`; its test is the holder pair (grant and dropped wake on the owner thread in
phase 3).

Reverts (K1's C2 part): `.scratch/c2/mutants.py` (output `.scratch/c2/mutants.txt`,
`mutants-k12a.txt`).

| Mutant | Broken | Build | Caught by |
| --- | --- | --- | --- |
| D5a | NF1: early-out continues in place with a posted emit pending | asan | use-after-poison in the posted emit |
| D5b | K9: throwing `init` rethrows at once with a posted emit pending | asan | use-after-poison |
| D5c | CN-R1: no resume count (handler resumes at once) | asan | `test_await_token` checks; use-after-poison |
| D5d | the check after registration is gone | rel | `init` called for a request after registration; `live_frames()` |
| D5e | a cross-thread request posts no emit | rel | hang (killed at 60 s) |
| D7a | `roots` not detected | rel | `test_wrapper`, `test_teardown` counts |
| D7b | `post_call` not detected | rel | hang (cross-thread cancels never land) |
| D8a | a closed scheduler still posts | rel | `io.poll() == 0` |
| D8b | spawn onto a closed scheduler links | rel | hang (the promise's `done` never runs or dies) |
| D8c | `destroy_roots()` destroys nothing | rel | teardown counts, mutex left locked |
| D8d | a linked `event` waiter does not unlink | rel | `queued() == 2`, SIGSEGV |
| D8e | `set()` does not mark taken | asan | heap-use-after-free |
| D8f | `~event` does not orphan | asan | heap-use-after-free |
| D8g | X3: abandon before deregistering | asan | use-after-poison in `lock_waiter::wake` |
| D8h | `~asio_scheduler` runs no phases | rel | destructor-phases check |
| D8i | `~deadline` asserts nothing | asan | `aborts(deadline_off_loop)` |
| D9a | `when_all` does not own unfinished children | rel | D9 destroyed count, `live_frames()` |
| K12a | `TASK_FRAME_POOL=0` ignored | rel | `task_pool_off` (first try reverted only `operator new` and survived, since pooled frees then went to `operator delete`; reverting both is caught) |

D10 is a removal: T7's `as_awaitable` grep is empty; its test and the bench's yield row spawn the
task on `asio_scheduler` directly.

T3 (K2): the architect's probe directories copied to `.scratch/arch-review/` and
`.scratch/arch-review-rev2/` with the branch's headers in `src/`. Ported: `cancel` to
`await_asio(init, token)` with the request on the loop; `teardown` to the four phases;
`late_fire` adds a `task::deadline` destroyed by the first timer's handler (requested 0; the first
port registered the deadline's timer first and requested 1, correctly, so the order was fixed);
`crossroot`, `holder` (both orders, `holder_ab`) spawned and torn down by the phases (mutex free
afterwards); `dispatch_inline` on the branch's `await_asio` with and without a token. Results:
`cancel` 0.0-0.1 ms (was 489-491); `teardown` 1 of 1 (was 0 of 1); `grant_destroyed` mutex
free; `crossroot`, `holder`, `dispatch_inline` exit 0 and ASan clean; `channel`, `race`,
`composed` unchanged; every run exits 0 in rel, ASan and TSan. `posts` (6.00 handlers) and
`nocurrent` are C3's rows, unchanged.

T5 sizes (printed by `test_sizes`, rel): `scheduler_ref` 16 (24 before), `cancel_callback` for a
lock 40, `await_asio` awaiter 72 without a token and 152 with one, `lock(token)` awaiter 80;
`static_assert` caps 16, 48, 72, 160. 10,000 socket reads after 100 warm-up: 0 `operator new`
with an empty token and 0 with an unrequested one. Nothing was timed.

Notes for the reviewer: `await_asio(init, token)` is a second overload with its own awaiter
type, so awaits without a token keep the 72-byte awaiter. `scheduler_ref::post_call` returns
whether the scheduler declares one. `root_list::link`/`unlink` are public (spawn calls them).
`~deadline` keeps a reference to its scheduler for the assertion; a test that destroys a deadline
off the loop now closes its scheduler first. `when_all` keeps eager freeing: a finished child
clears its owner's slot and destroys itself.

## Log
- 2026-11-02: C1 (D1-D4) on `phase/cancellation-c1`: reasons, callbacks, linked sources,
  `deadline` with the disarm bit, `lock(token)` replacing `lock_until`/`lock_for`, D8.4's
  destructor (K8), `live_frames()`; four builds green; 17 mutants caught (see "C1 evidence").
- 2026-11-02: C1 code review (APPROVE WITH CONDITIONS) folded in: CR1-CR6 (see "C1 fix round").
- 2026-11-02: revision 2 of `docs/design/cancellation.md` folds in the architect's REJECT
  (http:1ae1390) and the Xapiand lead's notes; "Review response" table by finding ID; ledger
  created; State ready for re-check (task:1132c04).
- 2026-11-02: C2 on `phase/cancellation-c2`: `await_asio(init, token)` with the resume count
  (D5, K9), `scheduler_ref` vtable (D7), `root_list`, `close()`/`destroy_roots()` and the event,
  deadline and when_all teardown rules (D8, D9, K10, K11), `as_awaitable` removed (D10),
  `TASK_FRAME_POOL` (K12), X3 and CR6 tests; four builds green, 19 mutants caught, probes clean,
  CI green on 8 jobs (task:213020d, task:1c30b8b).
