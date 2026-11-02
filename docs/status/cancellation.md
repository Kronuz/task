> Status: CURRENT (2026-11-02) the ledger for task's cancellation design (W2a, task half); nothing supersedes it.

# cancellation

- Design: `docs/design/cancellation.md` revision 2 (this branch); revision 1 at task:6d361ca.
  Reviews: architect, http `docs/reviews/task-handlers-rev2-design.md` at http:1ae1390, REJECT
  (2026-11-02, CN-L1, CN-L2, C8-C11; Q1-Q3 ruled (a)); Xapiand lead,
  `docs/reviews/cancellation-xapiand.md` (2026-11-02, XN1-XN3); pointer
  `docs/reviews/cancellation-design.md`.
- Branch / worktree: `design/cancellation` at `~/code/Kronuz/task-wt/cancellation`; head: `git log -1`.
- State: re-check CLEARED WITH CONDITIONS (2026-11-02, `docs/reviews/cancellation-design.md`); C1 may start; K8-K12 fold into the design at C1 and C2
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
- Doing: nothing.
- Next:
  1. The rechecker's scoped re-check of D5, D6, D8 against `.scratch/arch-review-rev2/`.
  2. On CLEARED, the lead starts C1 (D1-D4); C2 (D5, D7-D10) may start against C1's interface,
     and reactor's step R codes against D8.7.
  3. C3 (D6), then the Xapiand lead's review of the task diff, then landing.
- Blocked on: nothing.
- Findings folded in: CN-L1/CN-R1, CN-L2/CN-R2, CN-R3, CN-R4 (C8), CN-R5 (C9), CN-R6 (C10),
  CN-R7 (C11), XN1-XN3, Q1-Q3 rulings, TH2-L1 (task's API, D8.7): see the log.
- Last verified: design only; nothing built or run (2026-11-02). Citations checked against
  task:8cad661 (identical headers at this head), reactor:ade618d, Xapiand:81c311799.

## Log
- 2026-11-02: revision 2 of `docs/design/cancellation.md` folds in the architect's REJECT
  (http:1ae1390) and the Xapiand lead's notes; "Review response" table by finding ID; ledger
  created; State ready for re-check (task:1132c04).
