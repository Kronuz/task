> Status: CURRENT (2026-11-02) code review of cancellation step C3 (D6, inline resume) at `phase/cancellation-c3` head task:9ea6245 (code task:096c4dd, docs and bench task:8d5319e, ledger task:a77c434, task:9ea6245) on task:d3d94f3 (C1, C2); verdict APPROVE WITH CONDITIONS.

# Code review: cancellation C3 (D6, inline resume)

Reviewer: code-reviewer, 2026-11-02. Read: `docs/design/cancellation.md` D5.4, D6, D8 and the gate rows; the ledger's done criteria and
"C3 evidence"; `docs/reviews/cancellation-c2-code.md` (its probes are rerun here); the diff `d3d94f3..9ea6245`. There is no
`docs/reviews/calibration.md` in task. Everything ran on the Linux lease (Ubuntu 24.04, clang 18, g++ 14). Probes, mutants and output are
in `.scratch/review-c3/`, listed at the end. Build directories were removed on the lease; nothing was built locally.

## Verdict: APPROVE WITH CONDITIONS

The mechanism in `asio_park::resume` (`task_asio.hh:203-214`) is correct where clang compiles it. Ten asio operation kinds end their
frame inside the completion handler and run ASan, TSan and g++ ASan clean. An exception thrown through the inline resume restores the
depth and the scope. The C2 race, teardown and topology probes run clean on this branch. Every D6 mutant dies, and six of my seven do.

Three findings stand against landing:

1. **CR1 (code, condition).** Under g++ (any g++ build, since `TASK_SYMMETRIC_TRANSFER` is 0 for every non-clang compiler,
   `task.hh:425-429`), an inline resume inside a trampoline loop that runs with no scheduler queues the resumed child's parent there. The
   parent continues outside its scheduler's scope, and every await after that records no scheduler. D6.5 says "the rules are unchanged";
   probe `trampoline_scope` shows they are not.
2. **CR2 and CR3 (design, for the architect).** D6.1's `current() == sched_` clause lets a completion triggered from inside a running task
   resume the waiter inside that task's call. That has two consequences. A cancelled task can run to its end inside the requester's
   `request()` (CR2). Two tasks whose operations complete through each other's initiation run in lockstep and never return to the loop
   (CR3: 0 other handlers in 200,000 iterations, against 200,000 with D6 off). Dropping the clause keeps T3 `posts` at 4.00 and removes both
   effects. The architect should rule on it before the B1 pin.

Concurrency is a sensitive area, so after the fix round `rechecker` closes CR1, CR4 and the architect's ruling against these probes. No
second full review.

## Findings

### CR1 (condition): g++'s trampoline runs an inline-resumed child's parent outside its scheduler

`task_asio.hh:203-214` with `task.hh:448-461`. If the thread's trampoline is already active when `resume` runs `h.resume()` in
`running_on scope(s)`, the child's `final_suspend` transfer to its parent only queues (`transfer`, `t.active`). The scope closes when
`h.resume()` returns, so the parent then runs from the outer loop in whatever scope that loop has. When that scope is `s`, which is the
case D6.5 describes, only the order changes. When it is empty, the parent runs with `current()` empty. This happens when a handler with no
scheduler current runs a coroutine that awaits a child (a `sync_wait` on the loop thread, a frame resumed raw because it recorded no
scheduler, or a non-task coroutine type). The `!current()` clause admits the inline resume, and the frame ends up on no scheduler for good:
its next `await_asio` records `sched_` empty and resumes raw, on whatever thread completes it.

Probe `trampoline_scope` (`.scratch/review-c3/probe_c3.cc`, `results.txt`):

| Build | parent after child: `current() == s` | depth | after its next await: `current() == s` |
| --- | --- | --- | --- |
| clang ASan, TSan | 1 | 1 | 1 |
| g++ 14 ASan (trampoline) | **0** | 0 | **0** |
| clang ASan, D6 off (`hdr-d6f`) | 1 | 0 | 1 |

This is new in C3. Before it, the resume was posted and ran from the top of a handler, where no trampoline is active. The D6 tests cannot
see it because they await in the frame they observe (ledger note 1), so nothing proves that the two compilers behave the same for a child
task. Fix (either one, with `trampoline_scope` as a T1 test on both compilers):

1. Under `!TASK_SYMMETRIC_TRANSFER`, run the inline resume on a fresh trampoline, as `sync_wait` does (`task.hh:756-767`). The child's
   chain then finishes inside the scope on both compilers, D6.5's divergence goes away, and the D6 tests can await through a child task.
   This is the recommended fix.
2. Post instead when the local trampoline is active and `!current()`.

### CR2 (design, architect): a cancelled task runs to its end inside the requester's `request()`

D6.1's `current() == sched_` clause, together with D5.3's on-loop emit (`task_asio.hh:350-357`), produces this. A task on `s` calls
`src.request()`. The callback emits on the slot. If the operation's cancellation handler completes it synchronously (a hand-written
operation), the handler's `drop()` reaches zero with `current() == s` and resumes the cancelled frame inside `request()`. Probe
`sync_cancel` reports "ran inside requester's request()" as 1 on the branch and 0 with D6 off. The requester's code after `request()` then
sees the cancelled task already gone: for example, a loop over sessions that cancels each one while each session erases itself on exit. The
two remaining modes are ASan clean. In the cross-thread mode the posted emit drops last and resumes inline (2,000 of 2,000). In
`owns_deadline` the frame owns the `deadline` that cancels it and ends inside the deadline's own timer handler and `request()`.

D6.4's rule (XN2) covers completion inside an operation's *own* initiation and critical section. It says nothing about a cancellation
handler that completes synchronously, or about completions dispatched from another task. asio's own operations post their cancelled
completions, and so do http's two hand-written ones (D6.4), so no known consumer is affected today.

### CR3 (design, architect): a two-task ping-pong never returns to the loop, and the inline side's budget never yields

Probe `pingpong`. Tasks A and B each await an operation whose initiation stores its handler and `asio::dispatch`es the other side's stored
handler. That is legal under D6.4, because neither resumes inside its own initiation. B's dispatch resumes A inline (`current() == s`). A's
next initiation completes B's operation, B continues in place, and the cycle repeats without returning to `io.run()`. A posted ticker counts
the loop's other work:

| Header | checkpoints | iterations | ticker runs | A yields | B yields |
| --- | --- | --- | --- | --- | --- |
| branch, rel | none / A only | 200,001 | **0** | 0 | 0 |
| branch, rel | B only / both | 200,001 | 133 / 150 | 0 | 111 / 128 |
| D6 off (`hdr-d6f`), rel | any | 200,001 | 200,000 | 0 | 0 |

The ASan, TSan and g++ builds give the same shape. The depth bound never trips, because each inline resume returns before the next one
starts, so "bounded at 4" does not bound the loop's latency. A budget in the inline-resumed task never yields because every inline resume
stamps a fresh slice (D6.2). Only the task that continues in place spends its slice. Without any checkpoint this is a livelock, which C2
did not have.

**Resolution for the architect (CR2 and CR3).** Variant `hdr-top` resumes inline only when `!current()` (top-level handlers), with the bound
and `runs_here()` unchanged (`.scratch/review-c3/rbuild-top.sh`, `results-top.txt`):

| | branch | top-level only |
| --- | --- | --- |
| T3 `posts`, task + `await_asio` | 4.00 io handlers per round trip | 4.00 |
| `pingpong`, ticker runs | 0 | 200,000 |
| `sync_cancel`, runs inside `request()` | 1 | 0 |
| `ops_ok` (10 kinds x token) | all inline, clean | all inline, clean |

What the variant costs: a cross-thread cancel's emit runs in `post_call`'s scope, so its resume is posted (one more handler on that path).
A completion dispatched from inside a task is also posted, so the D6 "same scheduler dispatch" and chain tests change, and the depth bound
becomes almost moot. The measured win (6.00 to 4.00) comes entirely from top-level handlers. If the clause is kept, D6.4 needs two more
sentences: hand-written operations complete by posting, both from another task and from a cancellation handler; and a budget's checkpoint
in an inline-resumed task does not bound a lockstep pair. I recommend top-level only. It is simpler, and both hazards go away.

### CR4 (condition): the emit function's inline resume is untested

Mutant RV5 makes `emit_posted` always post its resume (`task_asio.hh:372-376`), and `task_asio_test` still passes. The suite's
20,000-round race (`asio_test.cc:745-760`) counts inline resumes from the handler or the emit together, and the handler is almost always
last. RV4 (drop before emit, a use-after-free once the frame ends inline) dies only by chance, in the race. Add `sync_cancel`'s
cross-thread shape as a T1 (D6) test. It is deterministic: the emit drops last in 2,000 of 2,000 rounds. RV4 and RV5 must both die in it.
If the architect rules top-level only (CR3), the test asserts the posted resume instead.

## Non-blocking notes (follow-ups, not another review)

- N1. g++ 14 `-Wvolatile` at `test/asio_test.cc:752:67` ("using value of assignment with volatile-qualified left operand"). It comes from
  C2's spin loop and is still there at C3. Write the loop as `for (volatile int k = 0; k < n;) { k = k + 1; }` or use an
  `std::atomic_signal_fence`.
- N2. The T1 (D6) row asks for "a budget loop after an inline-resumed *socket read* demotes". The test uses a timer (`asio_test.cc:1018`).
  It exercises the same mechanism, but the gate row's text should match the test.
- N3. The inline path does not check `closed()`, while `asio_scheduler::post` checks it twice (`task_asio.hh:69-74`). This is consistent
  with D8 because loops are joined before phase 2. But if an io_context runs between `close()` and `destroy_roots()`, operation handlers now
  run frames inline where C2 dropped their resumes. D8.7 should state that no loop runs between the phases.
- N4. A frame that recorded no scheduler is still resumed raw (`resume_on` with an empty `s`), and the depth bound does not count those
  resumes. This predates C3 and is unbounded only through such frames.
- N5. Author's open question 2 (wrapper `post_call`): the behavior is correct. The emit runs in the inner scheduler's scope, so the resume
  posts through the wrapper, at the cost of one extra handler on a cross-thread cancel. No change is needed, and under the top-level-only
  variant every scheduler behaves this way.
- N6. `.scratch/arch-review/src/task_asio.hh` differs from the branch head (it lacks C2's `abort_arg_t`/`result_args`). T3's
  `results_c3.txt` was therefore produced against an older header. My `posts` rerun uses the branch headers (`src-branch/`) and gives the
  same 4.00.

## Probe results (Linux lease, clang 18 ASan+UBSan and TSan, g++ 14 ASan+UBSan)

| Probe | Result |
| --- | --- |
| `ops_ok`: timer, `read_some`, `async_read`, `async_write`, `async_read_until`, range `async_connect`, move `async_accept`, `async_resolve`, `async_compose` over a timer, experimental channel `async_receive`; each with and without a token; the root ends and destroys the I/O object inside the handler | 20 of 20 resume inline, exit 0, no sanitizer report, `live_frames()` at base (all three builds) |
| `throw_inline` (a coroutine whose `unhandled_exception` rethrows, resumed inline) | the exception leaves `io.run()`, depth 0, `current()` empty (all three) |
| `trampoline_scope` | CR1: clang 1/1/1, g++ 0/0/0 |
| `sync_cancel` | CR2; ASan, TSan and g++ clean |
| `pingpong` | CR3 |
| C2's `ops`, `inline`, `idem`, `req_in_init`, `poll_after`, `poll_after_emit`, `vhost`, `ops_teardown`, `stress 20000`, `topo 2000` | exit 0 in all three builds; `stress` 0 `other` and `live_delta` 0; `inline` 1 io handler, never resumed inside init |
| C2's `nosched` | aborts on the N1 Debug assertion (`task_asio.hh:368`), as designed |
| Suite (`rbuild.sh` rel, asan, tsan, gcc) | 4 of 4 ctest pass in each; the only warning is N1 |

## Mutants

The author's D6 rows, rerun (`.scratch/c3/mutants.py`, output in `suite-mutants.txt`): D6a-D6f and D6d-tsan all die, at the lines the
ledger names. Mine are in `.scratch/review-c3/mutants_rv.py`:

| Mutant | Broken | Build | Result |
| --- | --- | --- | --- |
| RV1 | the depth is never decremented | rel | dies: `ran == 2`, `io.run() == 2` |
| RV2 | bound `<=` (five nest) | rel | dies: chain depth, `posted == 20` |
| RV3 | token handler drops before `cb_.reset()` | asan | dies: use-after-poison |
| RV4 | emit function drops before emitting | asan | dies: use-after-poison (by chance in the race; CR4) |
| RV5 | emit function always posts | rel | **survives** (CR4) |
| RV6 | `drop()` writes the awaiter after the inline resume | asan | dies: SEGV in `asio_park::resume` |
| RV7 | inline scope is `current()`, not `s` | rel | dies: `io.run() == 2`, scope check |

## Conditions

1. CR1: under g++, the inline resume runs on its own trampoline (or posts when the trampoline is active), and `trampoline_scope` becomes a T1
   test that passes on clang and g++. D6.5 is rewritten to match.
2. CR2 and CR3: the architect rules on D6.1's `current() == sched_` clause (top-level only, recommended, or keep it and extend D6.4 as
   stated above) before the B1 pin. The code and tests follow the ruling.
3. CR4: a deterministic T1 test of the emit function dropping last, in which RV4 and RV5 die.

## Escalation

The architect, for CR2 and CR3. This is a D6 design question (concurrency and fairness on every await of every consumer), and a fix
round cannot settle it.

## Files

`.scratch/review-c3/`:

- `probe_c3.cc`: modes `ops_ok`, `sync_cancel`, `pingpong`, `throw_inline`, `trampoline_scope`.
- `probe_c2.cc`: C2's probes, unchanged.
- `rbuild.sh`: probe builds into `build-bin/`, which `remote.sh`'s rsync leaves alone.
- `run.sh`: writes `results.txt`.
- `hdr-d6f/`: D6 off.
- `hdr-top/`, `src-top/`, `src-branch/`, `rbuild-top.sh`, `results-top.txt`: the top-level-only variant and the `posts` comparison.
- `mutants_rv.py`: RV1-RV7.
- `suite-mutants.txt`: the suite in four builds, the D6 rows and RV1-RV7.

## Not run

macOS builds; CI (run 41568114093 at task:a77c434 is the author's); T4 timing (the lead's); T6 consumers (strata, prospect, residency,
Xapiand); a full-buffer socket read loop (by reading asio, `post_immediate_completion` queues each completion behind other work); the
T3 `nocurrent` probe; strand and thread-pool schedulers (by reading `task_asio.hh:99`, `runs_here()` is false there, so they always post).
