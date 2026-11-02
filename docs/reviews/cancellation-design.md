> Status: CURRENT (2026-11-02) pointer to the architect's preliminary review of
> `docs/design/cancellation.md` at task:6d361ca.

# task review: cancellation, deadlines, inline resume and teardown (preliminary design)

The ruling is in http `docs/reviews/task-handlers-rev2-design.md` (branch `design/task-handlers`,
`~/code/Kronuz/http-wt/task-handlers`), which reviews W2a revision 2's two halves together. Its
probes are in that worktree's `.scratch/arch-review-rev2/`.

## Verdict

**REJECT: two defects in the mechanism, each with an exact fix; a scoped re-check of D5, D6 and
D8 follows the revision.**

## Blocking items

- CN-L1 (CN-R1): D5.2 registers the cancel callback after initiating and D6 resumes inline from
  the handler, so a completion that runs inside initiation ends the frame before `await_suspend`
  returns. Register first, skip initiation for a requested token, and park/wake so a handler that
  runs inside initiation never resumes the frame.
- CN-L2 (CN-R2, CN-R3, the Xapiand lead's XN1): D8 orders one scheduler's teardown; frames,
  guards and I/O objects cross schedulers. Four phases (stop all; close all, `post` drops and
  `spawn` destroys; destroy every root; destroy schedulers, then `io_context`s), and a granted but
  unresumed lock waiter unlocks in its destructor.

## Conditions

C8 (deadline disarm flag, CN-R4), C9 (wrappers forward `roots` and `runs_here`, CN-R5), C10
(`live_frames()` counts frames over 2 KiB, CN-R6), C11 (the emit step, the lost race, eager
linked registration, unrooted frames, CN-R7).

## Rulings

Q1 (a), with its premise corrected: Xapiand's `take_turn` calls `lock_until`
(`src/database/pool.cc:350`) and migrates in the wave. Q2 (a). Q3 (a).

Not run: nothing in the design is built; no test, T-row or benchmark; g++; fuzzing.
