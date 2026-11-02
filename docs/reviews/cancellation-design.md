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

## Re-check, 2026-11-02

Scope: `docs/design/cancellation.md` revision 2 (task:1132c04, ledger task:80b65fc) against the
ruling in http `docs/reviews/task-handlers-rev2-design.md` (http:1ae1390): CN-L1/CN-R1,
CN-L2/CN-R2/CN-R3, C8-C11, XN1-XN3, and the two new items. Mechanisms were checked against
`task.hh` and `task_asio.hh` (identical to origin/main task:8cad661 at this head), the Asio
1.36.0 behaviour the architect's probes established, and Xapiand:81c311799 / reactor:76c32b4 for
the consumers. Nothing in the design is built, so there is no fix to revert; instead the revised
mechanisms were modelled in `.scratch/recheck/recheck.cc` (ASan+UBSan and TSan, Homebrew clang
`-O1`, built and run under `quiet light`; output in `.scratch/recheck/results.txt`, build script
`build.sh`, run script `run.sh`). The model uses the real `task.hh` primitives and a stand-in
cancel source with `std::stop_callback`'s protocol; the awaiter is a prototype of D5.3/D5.4, not the
branch's code.

### Verdicts

| Finding | Verdict | Where, and the evidence |
| --- | --- | --- |
| CN-L1 / CN-R1 | CLOSED, with NF1 below | D5.2-D5.4 (`design/cancellation.md:185-215`), D6.3 (`:239-241`). Register-first, a requested token initiates nothing, and the three-party count (park, handler, emit) is `lock_waiter`'s `parked` handshake (`task.hh:767-774`) widened. Model `count_dispatch`, `count_direct`, `count_post`, each with and without a token: the root ends in the continuation after `await_suspend` returned false, exactly 1 completion, ASan+UBSan and TSan clean. The architect's `dispatch_inline` order (ASan use-after-poison at `task:8cad661`) does not occur. T1 (D5) lists the dispatch and direct-call `Init`s with and without a token under ASan, and drives the D6 chain from dispatches (`:420`). |
| CN-L2 / CN-R2 | CLOSED | D8.2-D8.5, D8.7 (`:297-336`, `:349-364`). Model `phases_cross` (root on A, `schedule_on(B)`, a timer on B; stop, close both, destroy roots, destroy schedulers, then `io_context`s): ASan clean, exit 0, where `crossroot` at `task:8cad661` is a heap-use-after-free. Model `phases_holder_ab` / `phases_holder_ba` (holder on A, waiter on B, both orders, mutex owned outside the roots): ASan and TSan clean, mutex free afterwards in both, where `holder` is a use after free in `asio_scheduler::post`. The closing scheduler is what makes both orders safe: every grant made in phase 3 posts to a live object that drops it. |
| CN-R3 | PARTIAL | The destructor rule exists (D8.4, `:315-320`) but its test is wrong for the timed and token forms: "`settle(timed_out)` fails (so it was granted)" also fails when the waiter is already `timed_out` (`settle` is a CAS from `waiting`, `task.hh:763-766`). A token-bound waiter whose callback settled it and whose `wake` was dropped is then treated as granted and `unlock()`s a lock it never held. Model `grant_timedout` with the destructor exactly as D8.4 words it (`-DDESIGN_LITERAL`): the holder's lock is released by the destroyed waiter ("NO: the destructor unlocked a lock it never held"), and, by reading (not run), a Debug build then trips `assert(locked_)` (`task.hh:854`) when the holder's own guard unlocks. With the test read from the state (`compare_exchange` and `expected == granted`, as `timed_awaiter::await_resume` does at `task.hh:927`) the same case is correct, and `grant_granted` (the architect's `grant_destroyed`) leaves the mutex free in both variants. Plain `lock()` has no `timed_out` state, so only the token form is affected. Fix: D8.4 says "its state is `granted`" (read from the failed CAS), not "`settle` fails". |
| C8 (CN-R4) | CLOSED | D4 (`:145-156`): the callback holds the deadline's state and requests only when the `disarmed` bit is clear, `~deadline` sets it first. Both sides run on the loop or after it stopped, so release/acquire is enough; the `late_fire` shape (an expiry already queued) is covered by the bit, and T1 (D4) and T3 name it (`:420`, `:422`). The `start_timer` comment correction is step C1 (`:436`). |
| C9 (CN-R5) | CLOSED | D7 (`:276-281`), D11 (`:385`, `:388`), T1 (D7). Xapiand's `engine::Host` (`src/engine/host.h:64-86`) is a virtual base and `Host::spawn` names `scheduler_ref::of(*this)` with the base type (`:90`), so detection on three virtual members on `Host`, forwarded by `AsioHost`, works as stated. |
| C10 (CN-R6) | CLOSED | D8.6 (`:337-342`): counting in `pooled_frame::operator new`/`delete` sees both paths (`frame_pool::allocate` hands frames over 2 KiB to `::operator new` inside `pool().allocate`, which `pooled_frame::operator new` calls, `task.hh:255,300-303`). The cancel state block must derive from `pooled_frame` to be counted; D8.6 says it is counted, which is enough. |
| C11 (CN-R7.1-4) | CLOSED | R7.1 D5.3 (`:199-202`: a plain function through `post_call`, destroyed unrun by the stopped `io_context`); R7.2 D5.5 (`:216-219`); R7.3 D2, D3 (`:119-122`, `:130-132`); R7.4 D8.6 (`:343-348`, unrooted frames named). The posted emit is sound with the count (see NF1 for the one path where it is not). |
| XN1 | CLOSED | Holder side: `phases_holder_*` above; the event side: `set()` posts to each waiter's scheduler and a closed one drops it, and D8.4 marks the node as taken before resuming it (`:321-324`), so a waiter destroyed later does not touch the event; by reading, not modelled. Waiter side: CN-R3 (PARTIAL above). |
| XN2 | CLOSED | D6.4 (`:242-252`) states the rule and D5.4 enforces its second half for every operation; the hand-written operations are named with the architect's `channel` probe (posts, never inside `async_send` / `try_send`). The 10,000-chunk streamed test is http T4 (j), outside task. |
| XN3 | CLOSED | D4 (`:171-175`) and "Rulings adopted" (`:398-400`) name `take_turn` and `pool.cc:350` and the same-wave migration. |
| TH2-L1 (task's part, D8.7) | CLOSED, with two sentences to add | Enough for `TcpServer::stop()` (`reactor.h:581-600`: after the join, `close()` on every reactor's scheduler, `destroy_roots()` on every one, then the reactors) and `UdpServer` (one loop, the same two calls). The roots on reactor 0 meet reactor 1 alive and closed, which is what `phases_cross` and `phases_holder_*` model. Add to D8.7: `close()` and `destroy_roots()` are idempotent and the owner thread's, after phase 1's join (a `Reactor`'s `scheduler_` destructor repeats them); `closed()` is a plain read after the join. |

### New items

**NF1 (condition on C2, one sentence): D5.3's early-out ignores a posted emit.** D5.3 (`:189-193`)
registers, re-checks the token, and when the request landed in between "destroys the registration
and returns false". A request from another thread in that window runs the callback on that thread,
which adds a count and posts the emit function (`:199-202`); the early-out then continues the
frame in place, the frame ends, and the emit runs against a dead awaiter. The same hole is `init`
throwing after the callback posted (`:204`). Model `earlyout` (the request is made from a second
thread between registration and the second check): ASan use-after-poison in the posted emit
(`executor_op<binder0<...>>`), exit 134; TSan does not see it (the pool poisons only under ASan).
Model `earlyout_fix`, where the early-out gives up the handler's share of the count and suspends
when an emit is pending (so the emit's drop resumes the frame with `operation_aborted`): clean in
both builds. Fix: D5.3 says that before `initiating` is set a cross-thread callback only records
the request (nothing is posted, since no slot holds a handler yet), or that the early-out and the
throw path drop the handler's share and suspend while the count is above 2. T1 (D5) adds the
hook-driven case (request from another thread between registration and the check).

**NF2: `~event` marking waiters orphaned instead of asserting (`:321-324`, `:389`).** Sound: a
waiter is either taken by `set()` (marked, leaves the event alone), still linked (unlinks itself,
single-threaded at teardown), or orphaned (the event died first, so its destructor does not touch
the event), and each of the three orders is a plain write to a frame that is alive. Two cautions.
It removes the only Debug signal of the ownership violation for events, so keep the count in Debug
builds (`live_frames()` shows the leak only if the waiter's frame never dies). And unlinking is
valid only while no thread can `set()`, which phase 3 guarantees but a destroyed-while-suspended
frame outside teardown does not; D8 should say it is a teardown rule.

**NF3: the ownership rule (`:329-332`) is half enforceable, and the text overstates it.** "A
violation is a use after free that ASan reports" is not what happens first.
Model `own_a_first` (a mutex that is a local of root A, root B queued on it, A destroyed first;
A's guard dies and grants B, a dropped wake): `Assertion failed: (!locked_)` in `~async_mutex`
(`task.hh:836`), exit 134, in both the ASan and TSan builds: the Debug assertion catches a mutex
destroyed with a granted waiter or a held guard deterministically, before any use after free.
Model `own_b_first` (B destroyed first): clean. A waiter that is only queued is safe either way
(`~async_mutex` releases its queue references, `task.hh:837-840`, and the awaiter's destructor
only releases its own). So the rule bites for guards and granted waiters, in Release builds
(`NDEBUG` removes the assertion), and for the destructor-unlock of D8.4 (a `m_.unlock()` on a freed
mutex). Say that, and add a T1 death test for the assertion (mutex destroyed with a granted waiter
in phase 3). Not enforceable in Release by task; the Debug assertion plus ASan is the check.

Do Xapiand or http violate it? Not that I found. Xapiand's turn mutex is a `ShardEndpoint` member
(`src/database/pool.h:212`), `Turn` holds only a guard pointer (`pool.h:112-125`); its events are
`shared_ptr` flights (`OpenFlight` `pool.cc:240`, `CommitFlight` `shard.cc:649`, `SingleFlight`
`single_flight.h:48`), which the last user destroys, outside any single root; its loop events
(`control_plane.h:111`, `:227`) are locals awaited by their own root. http's `src/` has no
`async_mutex` or `event` use at `http:679c0f8`'s worktree head. That the endpoint outlives every
`Turn` holder is the lead's claim and was not checked past the declaration.

### One-line notes for the lead (outside this scope)

- `spawn` onto a closed scheduler destroys the task without calling `done` (D8.3): Xapiand's blocking
  `Host::run` (`host.h:232-250`) waits on `done`, so it hangs if called after phase 2, the same as
  Limit 3 for `sync_wait`; worth one line in Limit 3.
- `control_plane.h:111` / `:144`: `wake_` points at a root-local `task::event`; after phase 3
  destroys that root, a later `stop()` would `wake_->set()` through it. Check at the Xapiand wave.

### Verdict

CN-L1, CN-L2, XN1-XN3, C8-C11 and TH2-L1 (task's API) are closed in the design; CN-R3 is PARTIAL
(one wrong test in D8.4) and NF1 is a use after free in D5.3's early-out path. Neither needs a
redesign: both are one-sentence edits, and the models above show the fixed shapes clean. **CLEARED
WITH CONDITIONS: fold CN-R3's state test and NF1 into D8.4 and D5.3 before C2 starts; C1 may start
now.** NF2, NF3 and the two D8.7 sentences are text edits for the same pass.

Not run: the real implementation (none exists); any T-row, benchmark or fuzz; the `rel` (Apple
clang `-O2`) and g++ builds; revision 1's `cancel`, `teardown`, `posts` and `nocurrent` probes (they
need the branch's `await_asio`); `race`, `channel`, `composed` (unchanged mechanisms, run clean at
`http:1ae1390`); the `event` unlink/orphan paths and the dying-`event`-setter case (read, not
modelled); a revert check (no fix commit exists to revert).
