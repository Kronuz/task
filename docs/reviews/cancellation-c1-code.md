> Status: CURRENT (2026-11-02) code review of cancellation step C1 (D1-D4) at `phase/cancellation-c1` head task:04abaa4 (task:86bc779 + one commit on design/cancellation task:6e11929); verdict APPROVE WITH CONDITIONS.

# Code review: cancellation C1 (tokens, callbacks, linked sources, deadline, `lock(token)`)

Reviewer: code-reviewer, 2026-11-02. Read: `docs/design/cancellation.md` rev 2, `docs/status/cancellation.md` ("C1 evidence"), the
three prior reviews, Detent `docs/reviews/calibration.md` (holders that export member addresses; configured bounds reaching
consumers: neither applies, `cancel_state` is heap-only and non-movable, no bound is configured). Host x86-64 (Intel macOS), so
weak-memory bugs are argued from the code, not observed.

## Verdict: APPROVE WITH CONDITIONS

No blocker. The one-word state, the stop_callback protocol, linked sources, the disarm bit and `lock(token)` match the design and
survive every probe I wrote (ASan+UBSan, TSan, rel, g++ Debug ASan). Three small conditions (CR1-CR3) are test or doc work, not code
fixes to the algorithms; a scoped re-check by `rechecker` is enough.

## Findings

**CR1 (condition, proof gap): the grant-then-cancel order is untested; mutant X4 survives the whole suite.**
`lock_cancel::operator()` (`task.hh:1111-1117`) wakes only if `settle(timed_out)` succeeds. Mutant X4 changes it to
`w->settle(timed_out); w->wake();` (wake even when the grant already won). All three test binaries pass with it (rel, sync_test).
The result in production would be a second `resume_on` of the same handle (double resume). `test_lock_token` B avoids the callback
by the disarm bit, `test_lock_destroyed` `grant_granted` never requests the token, and K races only cancels against a held lock.
Probe `.scratch/review-c1/probe_grantcancel.cc`: unlock grants a token waiter (resume posted), then `source.request()`, then the
posted resume runs; expects `posts == 1`, one resume, guard held. It passes on the branch and SEGVs on X4. Add that case to
`test_lock_destroyed` or `test_lock_token` (about 12 lines) and record the revert in the ledger (K1).

**CR2 (condition, design/doc): `requested()` is a relaxed load, so a poller gets no happens-before with the requester's earlier writes.**
`task.hh:689` (`word_.load(relaxed)`), while `reason()` (`:690`) is acquire and callbacks get ordering from the lock bit.
Probe `probe_order.cc` under TSan: thread A writes `data`, then `request()`; thread B spins on `token.requested()` and reads `data`;
TSan reports the race (200 rounds, every run). The pre-C1 code was also relaxed and the design says "one relaxed load" (D1), so this
is not a regression, but C1 adds `reason()` and callers (http's disconnect reason, Xapiand shutdown) will publish a payload before
requesting. An acquire load costs nothing on x86 and one `ldar` on arm64 (the asm for `requested()` stays a null test and one load).
Condition: the architect rules (a) acquire, or (b) the contract is documented as "no synchronization; use a callback or the lock
bit for payloads" in `docs/API.md` and the D1 text. I recommend (a). Not a code defect today.

**CR3 (condition, ledger accuracy): the mutant count and one gap.**
`.scratch/c1/mutants.py` holds 17 mutants (D1a, D1b, D2a-d, D3a, D3b, D3b2, D3c, D3d, D4a-d, K8a, K8b); the ledger and Log say 18.
Fix the number. Separately, mutant X3 (drop the explicit `cb_.reset()` before `abandon` in `~cancellable_lock_awaiter`,
`task.hh:1238-1241`) also survives: without it `abandon` releases the waiter before a callback running on another thread has
finished with it. No test destroys a frame while another thread's callback runs on its waiter. This is a teardown-shaped race, so it
belongs with C2's D8 tests; record it there rather than blocking C1 (the explicit reset is correct and the comment says why).

## Risk checks (each with its probe or file:line)

1. **State word.** `request()` (`task.hh:700-736`): one acq_rel CAS sets requested, lock and reason together, so `requested()` and
   `reason()` cannot disagree; losers return false (D1a mutant dies; `test.cc` 8-thread round x200, plus my `lost_wake`: 20,000
   rounds, a live callback registered against a concurrent request ran exactly once in every round, rel/ASan/TSan). `add()` (`:738`)
   returns false once `requested_bit` is set, even while the lock bit is held, and otherwise takes the lock bit before the request
   CAS can: no lost wake between request and registration. `unlock` is a release `fetch_and`, `lock` an acquire CAS. No ABA: the
   word only moves forward (requested and reason never clear). Polling: `.scratch/review-c1/poll.cc` at -O2 is `mov (%rdi),%rax;
   test; je; mov (%rax),%rax; and $1` (one load behind a null test); an empty token and a budget over it call 0 `operator new`
   (`test.cc` counting block, rel; D1b mutant dies). `sizeof(cancel_token)=8`, `cancel_state=48`, `linked=72` (the design's 48 and
   at most 128).
2. **cancel_callback vs stop_callback.** `remove()` (`:759-782`) waits on the epoch for a callback running on another thread and
   returns at once on the requester thread; no user code runs under the lock bit (`n->run` at `:723` is after `unlock()`; my
   `reentrancy` probe registers a callback, requests the same source and creates a child from inside a callback without deadlock).
   Registration after request runs inline (`test.cc`; probe `regdereg`: 4 threads x 200 register/deregister loops against one
   requester, a stack variable written by the callback is read after deregistration, TSan and ASan clean). A throwing callback is
   dropped and the rest run (`:942` invoke's catch; D2b dies). Lifetime: probe `permutations` destroys {parent source, parent
   token, child source, child token, callback on child, callback on parent} in all 720 orders x 3 request modes (2,160 cases), ASan and
   `live_frames()` back to baseline; probe `child_dies_during_request` destroys a child and its callback on one thread while the
   parent's request runs a slow callback on another (300 rounds, TSan and ASan clean). One limit of the proof: the frame pool
   poisons freed blocks only from offset 16 (`task.hh:242,280`), so a stale read of `word_`, `refs_` or `epoch_` is not caught by ASan;
   I found no such access by reading (`request` retains, a registration holds a reference, `destroy` holds the parent).
3. **Linked sources and deadline.** A parent request reaches child and grandchild with the parent's reason; a child of a requested
   parent is born requested on both paths (`make`, `:806-824`; the `before_link` hook case; mutant D3b dies on rerun). `~deadline`
   sets the disarm bit before the timer dies (`:976`), the fire functor holds a state reference and requests with
   `forbid = disarmed_bit` (`:967`); `late_fire` (`asio_test.cc:329`) is not vacuous (D4a dies in sync_test and asio_test on rerun).
   `.scratch/arch-review-rev2/probe_c1.cc` `late_fire` and `grant_destroyed` rerun clean in ASan and TSan. Timer lifetime: the
   handler's functor owns a state reference, so the io_context destroying a queued handler releases it.
4. **lock(token) and D8.4.** `abandon` (`:1286-1294`) unlocks only on `expected == granted`; `grant_timedout`, `grant_granted` and
   `grant_waiting` pass and K8a/K8b die (ledger; same tests rerun in my builds). `sizeof(lock_awaiter)` is 16, unchanged;
   `cancellable_lock_awaiter` is 80 (CR4). `lock_until`/`lock_for` appear nowhere in code, tests, bench or README; only history docs
   mention them. My `probe_mutex.cc` (3 single-thread schedulers, 600 token waiters per round, one thread cancelling at random and a
   final sweep, one thread taking and releasing the lock with `try_lock`; 30 rounds asan, 30 tsan, 20 g++ ASan): mutual exclusion held
   (`inside` counter), `got + cancelled == N`, the plain counter equals `got`, the mutex is free after, `live_frames()` back to base.
   `probe_inline.cc` (a no-scheduler waiter resumed inline by `request()` on another thread, its frame ending, with its linked source and
   awaiter, inside the callback; 3,000 rounds asan, tsan, g++) is clean: the self-deregistration path works across threads.
5. **Proof quality.** Tests compare values (reasons, `seen`, `value == 42` under TSan, `live_frames()`, `posts`), not only counts.
   Mutants rerun on copies of `task.hh` (never the worktree): D4a, D3b, D2a, D1a, D1b all die as the ledger says (D2a under TSan
   with `CHECK(value == 42)` and a race report). Extra mutants of mine: X1 (drop `epoch_.notify_all()`) dies by hang (120 s timeout),
   X2 (drop `retain()` in `request`) dies by ASan use-after-poison, X3 and X4 survive (CR1, CR3), X5 is equivalent to D4a. The
   four builds are separate directories. `TASK_TEST_HOOKS` only defines two `thread_local` pointers and a macro that is `((void)0)`
   otherwise: no code and no symbol in Release.

## Non-blocking notes

- CR4: `lock(cancel_token{})` returns the 80-byte `cancellable_lock_awaiter` with an inert registration; the design (D4.1) says an
  empty token is `lock_awaiter` exactly. Behavior is identical (test J); the design should name the two overloads, and a
  `static_assert(sizeof(lock_awaiter) == 2 * sizeof(void*))` would pin the "no growth" claim.
- CR5: `TASK_COUNT_FRAMES` defaults to on without `NDEBUG`: every pooled allocation does a global relaxed RMW in Debug and
  contends across cores; and a TU with a different `NDEBUG` than the rest changes `pooled_frame::operator new`'s definition (ODR).
  Fine for tests; make it opt-in (`TASK_COUNT_FRAMES=1`) before Xapiand Debug builds adopt it.
- CR6 (for C2): `abandon` handles a frame destroyed after the grant, but a frame destroyed on one thread while `unlock()` on
  another is between `settle(granted)` and `wake()` still has `wake()` post a dead handle. D8's phase ordering (no concurrent
  destruction) must be a stated precondition; the T1 (D8) set should include it.
- `remove()` spins on the lock bit with `yield` and re-locks per wake-up; fine at this scale, worth a T4 row if a hot path nests it.
- A deep chain of linked sources recurses once per level in `request()` (parent run, child request); depth 2 is the use case.

## Consumers

`scheduler_ref`, `work_pool_executor` and the WorkPool executors are untouched by the diff (`git diff` has no hunk in
`scheduler_ref`, `post`, or `running_on`). `-fsyntax-only` of a TU including strata's `work_pool_executor.hh` and `executor.hh`
against the branch headers, with `source_ = task::cancel_source()`, `task::cancel_token stop`, `const cancel_token&`, `requested()`
(the patterns strata and prospect use): clean (`.scratch/review-c1/consumer.cc`). Full strata, prospect and residency builds were
not run (fetch of dependencies; K6 is C3's anyway).

## Scope

Within the design: `live_frames()`/D8.4 landing in C1 is explained in the ledger and is needed by the new awaiter. `as_awaitable`
stays (D10 is C2). Nothing beyond D1-D4 found.

## Probes kept (`.scratch/review-c1/`)

`probe_cancel.cc` (lost_wake, regdereg, permutations, child_dies_during_request, reentrancy), `probe_mutex.cc`, `probe_inline.cc`,
`probe_grantcancel.cc`, `probe_order.cc`, `poll.cc`, `consumer.cc`, `run_probe.sh KIND SRC` (rel|asan|tsan|dbg), `build.sh KIND`
(copy of the lead's, builds in `.scratch/review-c1/build-KIND`), `mutate.py ID` (mutants on a copy, `mut/ID/`), `mutants-rerun.txt`.
Build dirs and binaries were removed.

## Conditions to land

1. CR1: add the grant-then-request case and its revert; ledger row.
2. CR2: architect ruling (acquire recommended) or the documented contract.
3. CR3: correct "18" to the real count; log X3 as a C2 test.

Blocking items: none.

Not run: benchmarks (T4); strata, prospect, residency full builds (K6); CI on Linux (K5); a TSan run of `probe_mutex` beyond 30 rounds;
real weak-memory hardware (host is x86-64); g++ builds of the full `task_test` and `task_asio_test` (g++ ran `sync_test`, `probe_inline`, `probe_mutex` only).
