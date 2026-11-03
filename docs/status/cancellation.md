> Status: CURRENT (2026-11-02) the ledger for task's cancellation design (W2a, task half); nothing supersedes it.

# cancellation

- Design: `docs/design/cancellation.md` revision 2, with C1's and C2's text fixes (NF1-NF3,
  CN-R3 wording, CR6, D8.7, Limit 3: task:1c30b8b); revision 1 at task:6d361ca.
  Reviews: architect, http `docs/reviews/task-handlers-rev2-design.md` at http:1ae1390, REJECT
  (2026-11-02, CN-L1, CN-L2, C8-C11; Q1-Q3 ruled (a)); re-check CLEARED WITH CONDITIONS
  (`docs/reviews/cancellation-design.md`); Xapiand lead, `docs/reviews/cancellation-xapiand.md`
  (XN1-XN3); C1 code review `docs/reviews/cancellation-c1-code.md` (CR1-CR6, X3); C2 code review
  `docs/reviews/cancellation-c2-code.md` (CR1-CR4, N1-N5).
- Branch / worktree: C3 on `phase/cancellation-c3` at `~/code/Kronuz/task-wt/cancellation-c3`
  (from task:d3d94f3, C2 landed); head: `git log -1`.
- State: C2 landed (task:d3d94f3). C3 (D6) code review APPROVE WITH CONDITIONS
  (`docs/reviews/cancellation-c3-code.md`); fix round done (CR1-CR4, N1-N4, the lead's top-level
  ruling; see "C3 fix round"), ready for the rechecker.
- Done criteria (set by the lead; contributors tick, never edit; they apply to steps C1-C3):
  - [x] K1 Every decision's T1 tests pass, and each fails with its fix reverted (the revert and the
    failure recorded here once per decision, D1-D8): D1-D4 in "C1 evidence"; D5, D7, D8 (and D9,
    K12) in "C2 evidence"; D6 is C3's: D6 in "C3 evidence" (mutants D6a-D6f)
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
- Done (C2 fix round): task:a8eef9a (CR1), task:36c7703 (CR2-CR4, N1-N3, N5), task:a958d7e
  (deadline test, N4); ledger below.
- Done (C3): task:096c4dd (D6 code and tests), task:8d5319e (T4 bench rows, D6.5, `ARCHITECTURE.md`,
  `docs/API.md`), and this ledger.
- Done (C3 fix round): task:d8e1f27 (top-level ruling, CR2-CR4 tests), task:fa09539 (CR1),
  task:81b2175 (N1), task:cc01808 (D6, D8.7, T1 row: N2-N4), task:5a54c52 and this ledger.
- Doing: nothing.
- Next:
  1. The rechecker closes CR1, CR4 and the ruling against `.scratch/review-c3/`; the lead runs T4
     on Linux (the fix round's clang 18 and g++ 14 Linux builds ran only in CI: no lease slot).
  2. T6 (K6) at the C3 pin: strata, prospect, residency; Xapiand by its lead.
- Blocked on: nothing.
- Findings folded in: CN-L1/CN-R1, CN-L2/CN-R2, CN-R3, CN-R4 (C8), CN-R5 (C9), CN-R6 (C10),
  CN-R7 (C11), XN1-XN3, Q1-Q3 rulings, TH2-L1 (D8.7), NF1 (K9), NF2-NF3 (K10), the TH2-L1 note
  (K11), X3 and CR6 (C1 review): see the log.
- Last verified: 2026-11-02, C3 fix round at task:cc01808: four macOS builds green (Apple clang
  rel, clang ASan+UBSan, clang TSan, g++ 16 ASan+UBSan), 42 mutants caught, the reviewer's probes
  clean on clang and g++ (see "C3 fix round"); CI green on all 8 jobs at task:a40bfe3 (run
  41579535675; `headers` skipped, as before). Before: C3 at task:8d5319e: four builds green (Linux lease) and macOS
  Apple clang rel, 33 mutants caught (26 C1-C2 rows, 7 D6 rows), probes clean (see "C3 evidence");
  CI green on all 8 jobs at task:a77c434 (run 41568114093; the `headers` job skipped, as at C2).

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
| D8i | `~deadline` asserts nothing | asan | `aborts(deadline_off_loop)`; retired in the C2 fix round (CR1 removed the assertion) |
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
(The `~deadline` reference and assertion are gone; see the C2 fix round.) `when_all` keeps eager freeing: a finished child
clears its owner's slot and destroys itself.

## C2 fix round (code review CR1-CR4, N1-N5, 2026-11-02)

Builds rel, ASan+UBSan, TSan and g++ Debug ASan via `.scratch/c2/build.sh` under `quiet light` (logs
in `.scratch/c2f/`), each its own directory, removed after: 4 of 4 ctest pass in each, no compiler
warning, no sanitizer report. Mutants (`.scratch/c2/mutants.py`, output `.scratch/c2f/mutants.txt`,
`mutant-cr1.txt`): 25 rows rerun on the final headers plus the new CR1 row, all 26 die; D8i is retired.

| ID | Change | Test | Revert (K1) |
| --- | --- | --- | --- |
| CR1 | `~deadline` asserts nothing and keeps no scheduler: `task.hh:1127-1128`, the `S& sched_` member gone; D4, `docs/API.md`, `ARCHITECTURE.md` say a deadline may be destroyed on any thread | the real-timer deadline in `test_primitives` no longer closes its scheduler; a deadline destroyed from another thread on a running loop, 300 rounds, requests nothing after it (`asio_test.cc:371-390`); `deadline_off_loop` removed | CR1 mutant (the assertion and member back): `Assertion failed: (sched_.runs_here() \|\| sched_.closed())`, abort. **D8i retired**: it asserted what CR1 deletes |
| CR2 | none (the code was right) | `request_in_init` x 4 in `test_await_token` (`asio_test.cc:620-634`): a request inside `init` before and after `async_wait`, on the loop and from another thread | R8 (drop `if (pending_) emit`), rel: `CHECK(done == 1 && ec == operation_aborted)` fails (the await hangs to the 10 s cap) |
| CR3 | none | `race_one`, 20,000 rounds of a cross-thread request against completion (timer, dispatch inside `init`, throwing `init`; `asio_test.cc:736-757`); every task ends once as ok, aborted or thrown, `live_frames()` back to base | R9 (drop the handler's `cb_.reset()`), asan: use-after-poison in `race_one`'s awaiter. The whole ASan ctest takes about 30 s |
| CR4 | `abort_arg_t<R>` (`task_asio.hh:234-237`): R when default-constructible, else `std::optional<R>`; the requested-before-start path builds `abort_arg_t<R>{}`, never `R{}`; the result type of `await_resume` follows | `test_await_token_move_only`: `tcp::acceptor`, `async_accept` as `void(error_code, tcp::socket)`: requested before start (init not called, empty optional), cancelled while pending on the loop and from another thread (`operation_aborted`, engaged, not open), accepted (open, `remote_endpoint` matches the client); `static_assert`s on both result types | CR4a (the handler stores `abort_arg_t<R>{}` for the real arguments), asan: the accept cases and the 4-byte read fail |
| N1 | Debug assert in `cancel()` when `post_call` is missing and did not throw (`task_asio.hh:336`) | death test `cancel_without_scheduler`: `sync_wait` of a token-bound timer, request from another thread | N1a (assert removed), asan: `CHECK(aborts(cancel_without_scheduler))` fails |
| N2 | `post` and `post_call` lambdas return if `closed()` (`task_asio.hh:72`, `:92`); `destroy_roots()` asserts a stopped loop when roots are live; D8.5, API.md, ARCHITECTURE.md state the `io_context` is not run again after phase 3 | `test_teardown`: `io.restart(); io.poll()` after phase 3 with a queued resume and with a queued cancel emit (the reviewer's `poll_after`, `poll_after_emit`); death test `destroy_roots_on_running_loop` | N2a / N2b (a check removed), asan: SEGV / use-after-poison; N3b: the death test fails |
| N3 | `close()` and `destroy_roots()` assert `!runs_here()` (Debug only; no field, no Release code) | death test `close_on_loop` | N3a, asan: `CHECK(aborts(close_on_loop))` fails |
| N4 | sizes below | `test_sizes` prints `deadline` and the `event` awaiter | none |
| N5 | ODR note at `TASK_FRAME_POOL` (`task.hh:367-369`), `docs/API.md`, `ARCHITECTURE.md` | none (text) | none |

Decisions taken, for the re-check: (CR4) the token overload's result for a non-default-constructible
`R` is a `std::optional<R>` in that position, empty only when the token was requested before the
operation started; other signatures keep their types, so `[ec, n]` of a read is unchanged and
reactor's accept loop reads `if (!ec) { use *peer }`. (N3) the owner-thread check is the stateless
`!runs_here()`, not a thread id stamp: the owner may not be the constructing thread, and a stamp
would cost a field. (N2) running the `io_context` after phase 3 stays unsupported: a pending
operation of an I/O object destroyed with its frame completes into the queue with a handler that
refers to the dead awaiter, which no check in the scheduler can reach; the `closed()` checks cover
only what the scheduler posted (resumes and emits). The two `closed()` loads are on the Release
posted-handler path (one acquire load of the flag `post` already read); not timed.

T5 sizes (rel, a probe against e0264bd and against this branch, `.scratch/c2f/sizes.txt`; the test
prints the same rows): `await_asio` awaiter 72 without a token (unchanged); with a token and a
`size_t` argument 152 before, 152 after; with a `tcp::socket` argument 240 (new signature);
`deadline<asio_scheduler>` 136 before, 128 after (the `S&`); `event` awaiter 48 (the state byte was
already in e0264bd); `scheduler_ref` 16. 10,000 reads after warm-up: 0 `operator new` with an empty
and with an unrequested token (unchanged). Nothing was timed.

Mutant builds: D5a-D5c, D8e-D8g, R9, CR1, CR4a, N1a-N3b asan; D5d, D5e, D7a, D7b, D8a-D8d, D8h, R8,
D9a, K12a rel; D8i retired. The CR1 row ran after the deadline test was added; the other 25 ran
before it (a test-only addition).

Reviewer probes rerun on the branch headers (`.scratch/c2f/probes.sh`, output `probes.txt`,
`.scratch/review-c2/probe_c2.cc` built asan, tsan, Debug): `dl_no_hooks`, `dl_hook_only`,
`dl_both_false`, `dl_in_frame_on_loop`, `dl_main_after_run`, `dl_main_after_poll`,
`dl_main_never_run`, `dl_main_after_stop`, `dl_strand_in_frame`, `dl_thread_pool_in_frame`,
`dl_other_thread_while_running`, `dl_closed_in_main`: all survive (no abort), asan; `dl_stress
20000` tsan: 0 reports (287 expired at destruction). `req_in_init` asan and tsan: aborted in
0.1-0.5 ms both ways. `stress 200000` asan: `other=0`, `live_delta=0`; `stress 20000` tsan and Debug:
the same. `nosched` asan and Debug: assertion `a cross-thread cancel of a token-bound await needs a
scheduler with post_call`, exit 134 (was: ends after 1001 ms with `aborted=0`). `poll_after`,
`poll_after_emit` asan and Debug: exit 0, one handler polled and dropped (was SEGV and
use-after-poison). `accept_sig.cc` (`await_asio<void(error_code, tcp::socket)>(init, token)`)
compiles (was two errors).

## C3 evidence (2026-11-02)

Builds, each its own directory, built clean: on the Linux lease (Ubuntu 24.04) via
`.scratch/c3/rbuild.sh KIND` (8 jobs): `build-rel` (clang 18 `-O2`), `build-asan` (clang 18,
ASan+UBSan), `build-tsan` (clang 18, halt on the first report), `build-gcc` (g++ 14, Debug,
ASan+UBSan, the trampoline path); on macOS `build-rel` (Apple clang `-O2`) via `.scratch/c3/build.sh`
under `quiet light`. `ctest` 4 of 4 in each, no sanitizer report; the one compiler warning is
g++ 14's `-Wvolatile` on C2's spin loop (`asio_test.cc:752`). Output: `.scratch/c3/rebased-run.txt`
(Linux, at the rebased head), `.scratch/c3/mac-final.txt`.

D6 (`task_asio.hh:186-214`): `asio_park::drop()` at zero calls `resume`, which runs
`{ running_on scope(s); h.resume(); }` inline when `inline_resume_depth < 4`, `s.runs_here()` and
`!current() || current() == s`, else `resume_on` (posts). No member was added: the depth is a
thread-local.

T1 (D6), `asio_test.cc` `test_inline_resume`: a timer completion on the loop resumes inline in the
scheduler's scope, with and without a token (`io.run() == 2`: the spawn's post and the timer's
handler; 3 at C2); on a loop thread the continuation runs on that thread (TSan); a completion on
another loop's thread is posted (depth 0, the waiting loop's thread); a handler dispatched by a
coroutine of the same scheduler resumes its waiter inside the dispatch, one dispatched from another
scheduler's coroutine on the same thread is posted; a chain of 100 completions, each dispatched by
the link the last one resumed, peaks at depth 4 (nest 5), posts links 4, 9, ..., 99 (20) and ends
all 100; after an inline resume a 1 s budget's first checkpoint does not yield (fresh slice) and a
200 us budget yields 10 times; teardown with a chain in flight (the loop stops inside an inline
resume, one posted resume queued, three links suspended) destroys all 12 frames, `live_frames()` at
base. `test_await_token`: the on-loop cancel ends a pending read in exactly 2 `run_one`s (was
`<= 3`); the 20,000-round cross-thread race counts inline resumes (1,864 at the head, nonzero in
every build) with every task ending once. `test_wrapper`: a task spawned through the `Host`-shaped
wrapper resumes inline in the wrapper's scope (`current() == hr`).

Mutants (`.scratch/c3/mutants.py`, the C2 set plus D6; output `.scratch/c3/rebased-run.txt` at the
rebased head, `.scratch/c3/mutants.txt` before the rebase, `.scratch/c3/mutant-d6b.txt`): all 33 die
in both runs (26 C1-C2 rows and 7 D6 rows, D6d in rel and TSan).

| Mutant | Broken | Build | Caught by |
| --- | --- | --- | --- |
| D6a | no depth bound | rel | chain: depth and posted-count checks |
| D6b | inline resume outside the scope (raw resume: the slice is not counted) | rel | `io.run() == 2`, scope check, `yields == 10`, the wrapper's scope |
| D6c | scope set, enclosing slice kept (slice not stamped) | rel | fresh-slice check (`fresh_yields == 0`) |
| D6d | inline off the scheduler's thread (`runs_here` dropped) | rel, tsan | the other-thread completion (`asio_test.cc:355`) and the other-loop case |
| D6e | inline while another scheduler is current | rel | the two-scheduler dispatch case |
| D6f | no inline resume at all | rel | `ran == 2`, `inlined > 0` |

The 26 C1-C2 rows (D5a-D5e, D7a, D7b, D8a-D8h, R8, R9, CR4a, N1a, N2a, N2b, N3a, N3b, CR1, D9a,
K12a) still die; D5c's text follows `drop()`'s new call.

T3 (K2's C3 part): the architect's probes rerun on the branch headers in rel, ASan and TSan on the
Linux lease (`.scratch/arch-review/results_c3.txt`, `.scratch/arch-review-rev2/results_c3.txt`): 39
of 39 runs exit 0, no sanitizer report; `composed`, `channel`, `race`, `crossroot`, `holder` (both
orders), `grant_destroyed`, `late_fire`, `dispatch_inline`, `cancel`, `teardown` unchanged. `posts`
(io handlers per round trip, 10,000 round trips): task + `await_asio` 4.00, asio's coroutine 4.00;
at C2 6.00 on macOS and 7.00 on the Linux lease (C2 headers, `.scratch/c3/posts-c2-linux.txt`); macOS
at C3 4.00 (`mac-final.txt`); `operator new` 5 per 10,000 on macOS, unchanged from C2 (0 on Linux).
`nocurrent` ports a third row, a task resumed inline by `await_asio`: 19-21 yielding checkpoints in
every build and both hosts, as many as one resumed through the scheduler (19-21); the raw resume 0.
The probe's `operator new` counter is compiled out under Linux TSan, whose runtime defines it.

T4 rows (`bench/bench.cc`, `task_bench N` runs only them): an `await_asio` socket round trip with
inline resume, with a scheduler that always posts, and inline with a deadline linked to a session
token per read. Counts only, Linux rel, 2,000 round trips (`.scratch/c3/bench-counts.txt`): io
handlers 4.00, 7.00 and 5.00 per round trip (the deadline's cancelled timer wait is the fifth);
`operator new` 0, 0 and 0.001. Nothing was timed; the lead runs the rows.

T5 (sizes printed by `test_sizes` and `test_await_token_move_only`, Linux rel, before (C2) and
after): `await_asio` awaiter 72 and 72; with a token 152 and 152; with a socket argument 240 and 240;
`scheduler_ref` 16; `deadline<asio_scheduler>` 128; `event` awaiter 48; 10,000 reads after warm-up:
0 `operator new` with an empty and an unrequested token, before and after.

Notes for the reviewer: (1) under g++'s trampoline a child task's return inside an inline resume
queues on the running trampoline instead of nesting (D6.5), so the D6 tests await in the frame they
observe; a first version that awaited through a child task passed with clang and failed the depth
checks with g++. (2) The tests read `detail::inline_resume_depth` to tell an inline resume from a
posted one. (3) `current() == s` compares the scheduler pointer, so an emit function posted through
a wrapper's `post_call` runs in the inner scheduler's scope and posts its resume through the wrapper
rather than resuming inline: correct, one extra handler on that cancel path.

## C3 fix round (code review CR1-CR4, N1-N4, 2026-11-02)

The lead's ruling on CR2 and CR3 (2026-11-02): inline resume happens only at the top level, when
`!current()` (the completion runs directly under the loop, not inside another frame's resume, a
scheduler drain or a cancel request); the `current() == sched_` clause goes. Reason: the reviewer
measured the top-level-only variant at T3 `posts` 4.00 io handlers per round trip, the same as with
the clause, and it removes CR2 and CR3. Fewer inline paths, same measured gain.

Builds: the Linux lease had no slot (exit 3, twice), so every build ran on macOS under `quiet light`
through `.scratch/c3/build.sh` (`.scratch/c3f/all.sh`, logs `.scratch/c3f/build-*.log`, summaries
`all-2.txt`, `all-3.txt`): `build-rel` (Apple clang `-O2`), `build-asan` and `build-tsan` (Homebrew
clang, ASan+UBSan and TSan), `build-gcc` (g++ 16 Debug ASan+UBSan, the trampoline path). `ctest` 4 of
4 in each, no compiler warning (N1's `-Wvolatile` is gone), no sanitizer report. Not run here: clang
18 and g++ 14 on Linux; CI's Linux jobs cover them.

| ID | Change | Test | Mutant (revert) |
| --- | --- | --- | --- |
| CR2, CR3 (ruling) | `asio_park::resume` resumes inline only when `!current()` (`task_asio.hh:204-216`); D6.1, D6.3, D6.4, `ARCHITECTURE.md`, `docs/API.md` state the rule | `test_inline_top_level` (`asio_test.cc`): `sync_cancel` on the loop (a requester task's `request()`, the operation completes inside the emit: done 1, ran inside `request()` 0, resumed by a post); `sync_cancel` across threads (2,000 rounds, the posted emit drops last: 2,000 done, 0 inline); `pingpong` (2,000 rounds: 2,001 rounds, 2,000 ticks of other loop work, one per round). `test_inline_resume`: a dispatch from a task of the same scheduler now posts (depth 0) | D6g (the clause restored), rel: dies at `asio_test.cc:986` (same-scheduler dispatch), `:1174` (on-loop `sync_cancel`), `:1195` (cross-thread, `inlined == 0`), `:1241` (`pingpong`, ticks 0) |
| CR1 | `detail::fresh_trampoline` (`task.hh:466-481`), shared by `sync_wait` (`:774`) and the inline resume (`task_asio.hh:211`): when the thread's trampoline loop is running, the block swaps in an empty one, so the resumed frame's chain runs inside the scope. No-op under symmetric transfer. D6.5 states what holds on each compiler | `trampoline_scope` in `test_inline_top_level`: a child resumed inline inside a non-task coroutine with no scheduler; its parent continues with `current() == s`, depth 1, and keeps `s` after its next await. Passes in all four builds (clang symmetric, g++ trampoline) | D6h (fresh trampoline removed), gcc: dies at `asio_test.cc:1265`. Evidence that the ruling alone does not close CR1: the reviewer's `probe_c3 trampoline_scope` built with g++ against the ruling-only headers (`.scratch/c3f/hdr-ruling/`) prints `scoped=0 depth=0; after its next await scoped=0` (`probes.txt`), as at C3 |
| CR4 | none (tests) | `test_inline_top_level`: a `bare_scheduler` whose `post_call` runs at the top level (no scope; D7 allows it); the posted emit drops the count last, 100 rounds each in two orders, deterministic on one thread (request made off the loop): the operation completes inside the emit (`operation_aborted`), or completed before the emit ran (the real result); every frame resumes inline from the emit, in the scheduler's scope | RV5 (emit always posts), rel: dies at `:1215` in every round. RV4 (drop before emit), asan: use-after-poison in `emit_posted` from `test_inline_top_level` (the completed-first order) |
| N1 | the race's spin is `spin(ns)` (timed, 0-40 us) instead of a `volatile` counter | g++ builds warning-free | none (text of a test). A first version with a compiler-barrier counter spun too briefly: the request landed before every frame started and `CHECK(st.inlined > 0)` failed (rel: thrown 0, inline 0); the timed spin keeps the C3 distribution |
| N2 | T1 (D6) row text: "inline-resumed timer wait"; the row also names the new tests | none | none |
| N3 | D8.7: no loop runs between the phases; between `close()` and `destroy_roots()` an operation handler would resume its frame inline | none (text) | none |
| N4 | D6.3: a frame that recorded no scheduler is resumed raw and the bound does not count it; pre-existing, no code change (no scheduler to post to) | none | none |

Mutants (`.scratch/c3f/mutants.py`, output `.scratch/c3f/mutants.txt`, detail for D6e, D6g, D6h,
RV4, RV5 in `mutants-detail.txt`): all 42 die. The 26 C1-C2 rows; D6a-D6f and D6d-tsan, rewritten
for the new condition; D6g and D6h (new); the reviewer's RV1-RV7 (RV7's string follows the new
`fresh_trampoline` line). RV5 survived at C3 and dies now; RV4 died only by chance in the race and
now dies deterministically.

Reviewer probes (`.scratch/c3f/probes.sh`: `.scratch/review-c3/probe_c3.cc` and `probe_c2.cc` built on
macOS, asan and tsan Homebrew clang, gcc g++ 16 ASan, rel Apple clang; output `probes.txt`):

| Probe | clang (asan, tsan, rel) | g++ 16 ASan |
| --- | --- | --- |
| `ops_ok` (10 kinds x token) | ok, all inline | ok |
| `sync_cancel` | on-loop: inside `request()` 0, inline 0; cross-thread: 2,000 done, inline 0; `owns_deadline`: inline 1 (asan). TSan reports a race on the probe's own plain `int` counter (`sc_stats::done`, read by main while the posted resume writes it); with the counters atomic (`.scratch/c3f/probe_c3_atomic.cc`) TSan is clean and prints the same values | same as asan |
| `pingpong` | 20,001 iterations, 20,000 ticks, A and B yields 0 (asan, tsan, every checkpoint mode); rel 200,000: 200,001 and 200,000 | same |
| `throw_inline` | ok | ok |
| `trampoline_scope` | scoped 1, depth 1, after next await 1 | **1, 1, 1** (was 0, 0, 0; still 0, 0, 0 with the ruling alone) |
| C2's `ops`, `inline`, `idem`, `req_in_init`, `poll_after`, `poll_after_emit`, `vhost`, `ops_teardown`, `stress 20000`, `topo 2000` | exit 0 | exit 0 |
| C2's `nosched` | aborts on its Debug assertion, as designed | same |

T3 (`.scratch/c3f/t3.sh`, `.scratch/arch-review/probe_loop.cc` against the C3 head headers,
`review-c3/src-branch/`, and the fix round's, `src-fix/`; Apple clang `-O2`; output `t3.txt`): `posts`
task + `await_asio` 4.00 io handlers per round trip before and after (asio's coroutine 4.00),
`operator new` 5 per 10,000 both (asio 9). `nocurrent`, yielding checkpoints of a 200 us budget for
5 ms: through the scheduler 21 before, 18 after; inline by `await_asio` 20 before, 18 after; raw
resume 0 both. One run each; the inline row matches the scheduler row in both.

T5 (`test_sizes`, rel, before at C3 and after): `await_asio` awaiter 72 and 72, with a token 152 and
152, with a socket argument 240 and 240, `scheduler_ref` 16, `deadline<asio_scheduler>` 128,
`event` awaiter 48, `cancel_callback` (lock) 40, `lock(token)` awaiter 80; 0 `operator new` per
10,000 reads with an empty or unrequested token. `fresh_trampoline` adds no member: it lives on the
stack of the inline resume (empty under symmetric transfer).

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
  `TASK_FRAME_POOL` (K12), X3 and CR6 tests; four builds green, 18 mutants caught, probes clean,
  CI green on 8 jobs (task:213020d, task:1c30b8b).
- 2026-11-02: C2 code review (APPROVE WITH CONDITIONS) folded in: CR1-CR4 and N1-N5 (see "C2 fix
  round"); four builds green, 25 mutants caught and D8i retired, the reviewer's probes clean.
- 2026-11-02: C3 on `phase/cancellation-c3` (from task:d3d94f3): `await_asio` resumes inline on the
  scheduler's thread in its scope, depth bound 4 (D6); T1 (D6), T4 rows; four builds green, 33
  mutants caught, `posts` 4.00 io handlers per round trip, `nocurrent` 19-21 (task:096c4dd,
  task:8d5319e).
- 2026-11-02: C3 code review (APPROVE WITH CONDITIONS) folded in: the lead's ruling (inline resume
  at the top level only, CR2, CR3), CR1 (fresh trampoline), CR4 (emit-drops-last tests), N1-N4;
  four builds green, 42 mutants caught, `posts` 4.00, `trampoline_scope` 1/1/1 on g++
  (task:d8e1f27, task:fa09539, task:81b2175, task:cc01808).
