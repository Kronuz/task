> Status: CURRENT (2026-11-02) code review of cancellation step C2 (D5, D7-D10) at `phase/cancellation-c2` head task:d6eb8f7 (code task:213020d, design/docs task:1c30b8b, ledger task:559725f, task:d6eb8f7) on task:b406766 (C1); verdict APPROVE WITH CONDITIONS.

# Code review: cancellation C2 (await_asio tokens, scheduler_ref hooks, root teardown)

Reviewer: code-reviewer, 2026-11-02. Read: `docs/design/cancellation.md` rev 2 (D5, D7-D10, gate rows), the ledger's "C2 evidence", the C1 review (X3, CR6),
the design and Xapiand reviews, Detent `docs/SHARED_LIBRARY_DECISIONS.md` (last sections: item 4 teardown, item 6 source compatibility), and
`docs/reviews/calibration.md` (holders exporting member addresses: `root_list` is non-movable, nothing exports a member address;
configured bounds: none). Host x86-64 Intel macOS, so weak-memory arguments are read from the code. Probes and mutants are in
`.scratch/review-c2/` (list at the end); worktree untouched (`git status` clean), build directories removed.

## Verdict: APPROVE WITH CONDITIONS

No blocker in the mechanism. The count/park protocol, the root list and the four phases survived every probe I wrote in ASan+UBSan and TSan,
and four of the lead's mutants died on my rerun. The conditions are: the `~deadline` assertion is wrong in both directions (CR1, with the
runs_here ruling), two untested paths (CR2, CR3: a mutant of each survives the suite), and one API hole the first consumer will hit (CR4).
Teardown is a sensitive area, so one scoped re-check by `rechecker` follows the fix round; no second full review.

## Ruling asked by the Xapiand lead: runs_here and the `~deadline` assertion

**Rule (a), strengthened: a scheduler that cannot say "wrong thread" gets no assertion, and the assertion should not be keyed on
`runs_here()` at all.** Not (b), not (c).

Evidence:

1. The premise does not hold for the code as written. `~deadline` is `if constexpr (requires { sched_.runs_here(); sched_.closed(); }) { assert(...) }`
   (`task.hh:1127-1130`), so the assertion exists only for a scheduler type that declares both. Probe `dl_no_hooks` (start_timer, neither hook) and
   `dl_hook_only` (runs_here, no closed) survive in Debug; only `dl_both_false` aborts. Xapiand's `BlockingScheduler`/`Host` has no `closed()`
   (`host.h:64-210`; D7 has `AsioHost` forward `roots`, `runs_here`, `post_call`, not `closed`), so it never aborts, with or without
   `runs_here`. The one type that arms the assertion is `asio_scheduler`.
2. On `asio_scheduler` the assertion is wrong in both directions. `runs_here()` answers false for "another thread" and for "cannot tell"
   (`task_asio.hh:99`, executor not an `io_context::executor_type`). Debug aborts (`SIGABRT` at `task.hh:1128`), probes in `probe_c2.cc`:

   | Pattern | Result | Right? |
   | --- | --- | --- |
   | `dl_in_frame_on_loop` (deadline in a frame, destroyed on the loop) | survives | yes |
   | `dl_main_after_run` (deadline in `main`, `io.run()` returned, scope exit) | aborts | no: nothing runs; this is the item-4 pattern |
   | `dl_main_after_poll`, `dl_main_never_run`, `dl_main_after_stop` | abort | no |
   | `dl_strand_in_frame` (scheduler over an `asio::strand`), `dl_thread_pool_in_frame` | abort, on the loop's own thread | no: false positive, `runs_here` is always false there |
   | `dl_other_thread_while_running` | aborts | the case it targets |
   | `dl_closed_in_main` | survives | yes |

3. The race it targets is not there for `asio::steady_timer`. `probe_c2-tsan-nd dl_stress 20000` (TSan, `-DNDEBUG`, so the assertion is
   compiled out): 20,000 deadlines destroyed from a non-loop thread against a running loop, 248 already expired at destruction, 0 reports.
   The protocol is already thread-safe by construction: `disarm()` is an atomic bit set before the timer dies, the fire functor owns a state reference,
   asio's timer queue cancels under the scheduler's mutex. (x86 and TSan, so evidence, not proof.)
4. (b) costs more than it buys. A required `runs_here` makes every scheduler answer; the honest answer for a pool, an inline drain or a strand is
   "false", which is exactly the fallback, so the assertion still fires wherever the answer is a non-answer, and a mandatory hook invites `return true`
   to make it quiet. It also breaks binding item 6 (schedulers stay source-compatible, or every consumer migrates in one wave): the concept gains a
   member for an assertion. For the record, the migration list if the owner still wants (b): task `inline_scheduler` (strata `executor.hh:117`, prospect
   `sliced_test.cc:59,407,423,448`), prospect `pool_scheduler` (`sliced_test.cc:462`), Xapiand `Host` (`host.h:64`), `BlockingScheduler` (`host.h:207`),
   `AsioHost` (`host_asio.h:47`), `engine::Run` (`host_asio.h:105`), `SpyScheduler` (`test_pool.cc:1321`), the `post`-only fixtures at
   `test_blocking_guard.cc:59` and `test_control_plane.cc:428`; strata's `work_pool_executor` and the `stepping_executor`/`reclaim_*` fixtures are
   strata `executor`s (they record `current()`; no `post(handle)` found in the strata or residency checkouts), and I found no `mt_sched` in the
   strata checkout (it may live on a branch; grep again there). That is 8 to 10 types for no change in behavior.
5. (c) documents an assertion that aborts valid code.

Consumer cost of (a): none. The fix is to delete the assertion and the `S& sched_` member (that also restores `sizeof(deadline)`: the member costs 8 bytes in
Release for a Debug-only check, and in Debug makes `~deadline` touch a scheduler that may be gone), and to drop `deadline_off_loop` and mutant D8i. If the
owner wants a check, key it on the one fact `asio_scheduler` knows, `runs_here() || closed() || !io_` and still not on "stopped"; I would not.

## Findings

**CR1 (condition, code): `~deadline` assertion, above.** Remove it (or reduce it to the `asio_scheduler` case with a known `io_context`, which still
aborts the item-4 pattern `dl_main_after_run`, so I do not recommend that). Update D4/D8 text ("Runs on the deadline's loop, or after its scheduler was
closed", `task.hh:1126`) and the death test. Probes: `dl_*` modes, `dl_stress`.

**CR2 (condition, proof gap): mutant R8 survives.** Removing `if (pending_) { signal_.emit(...) }` (`task_asio.hh:262`, "requested during initiation") still passes `asio_test` (exit 0, rel; the only binary I ran for it). The path is D5.3's second bullet: a request on the loop's own thread after the second check but inside
`init`. Without the emit the operation is never cancelled and the await hangs. My probe `req_in_init` (request before and after `async_wait` inside `init`):
branch `ec=operation_aborted` after 0.1 ms both ways; mutant R8 ends at the 500 ms cap with the op uncancelled (`ec` never set). Add the two cases to
`test_await_token` (about 20 lines) and record the revert.

**CR3 (condition, proof gap): mutant R9 survives; the suite does not hit the cancel-versus-completion window.** Removing the handler's
`self->cb_.reset()` (`task_asio.hh:233`, the line that makes "no callback starts after it, so the count can no longer grow" true) passes `asio_test` in ASan.
With it removed a cross-thread callback can add to a count that already reached zero and post `emit_posted` for a frame that has finished: my `stress` probe
(a root awaiting a 100-450 us timer with a token, a second thread requesting after a random spin, three init shapes: timer, dispatch inside init, throwing
init) dies on mutant R9 in ASan (`use-after-poison` in `emit_posted`, within 200,000 rounds) and passes the branch (200,000 rounds ASan, 20,000 TSan and Debug:
`other=0`, `live_delta=0`). The branch code is correct; the lead's "10,000 cross-thread cancels on a running loop" does not race completion. Add the `stress_one`
shape to `test_await_token` at about 20,000 rounds and record R9 in the ledger.

**CR4 (condition, API): `await_asio<void(error_code, R)>(init, token)` does not compile for a non-default-constructible `R`.** The requested-before-start path
does `result_.emplace(operation_aborted, R{}...)` (`task_asio.hh:244`, `:265`'s sibling). `asio::ip::tcp::socket` has no default constructor, so
`await_asio<void(asio::error_code, tcp::socket)>([&](auto h){ a.async_accept(std::move(h)); }, token)` fails deep in the template
(`.scratch/review-c2/accept_sig.cc`, two errors). reactor's accept loop is exactly that call today (`reactor.h:429`, `:469`, `async_accept` returning the socket), and it
is the loop shutdown must cancel. The empty-token overload is fine. Either support it (store `std::optional<R>`s, or build the aborted result lazily for default-constructible
`R` and `static_assert` with a message for the rest) or state the workaround in `docs/API.md` and the header comment (`async_accept(peer, h)` with `void(asio::error_code)`) and
add a clear `static_assert`. Reactor's step R should know before it codes against the interface.

## Non-blocking notes (follow-ups, not another review)

- N1 (D5.6 / Limit 1): a token-bound await with no current scheduler, or one without `post_call`, silently drops a cross-thread cancel. Probe `nosched`: `sync_wait` of a
  token-bound 1 s timer with a request at 50 ms ends after 1001 ms with `aborted=0`; a socket read would hang. D7 accepts "no post_call, no cross-thread cancel", but Debug is silent. Add
  `assert(posted)` (or `assert(sched_)`) in `cancel()`'s fallback (`task_asio.hh:293`) so the Xapiand `BlockingScheduler` case fails loudly in tests.
- N2 (D8 precondition not stated): resumes and cancel emits posted before `close()` stay in the `io_context`'s queue and run on freed frames if the owner runs the loop again after
  `destroy_roots()`. Probes `poll_after` (SEGV, pc 0) and `poll_after_emit` (ASan `use-after-poison` in `emit_posted`): `io.restart(); io.poll()` after phase 3. The design says phase 4 destroys the
  `io_context`, so this is unsupported, but the text never says "never run it again" and `io.restart(); io.poll()` is a common flush idiom. Cheap hardening: `if (closed()) { return; }` at the top of the
  `post` and `post_call` lambdas (one acquire load), and an `assert` that `io_`'s context is stopped in `destroy_roots()` (today only the destructor asserts, `task_asio.hh:62`).
- N3: `close()`/`destroy_roots()` are "owner thread only" by text; nothing asserts it. A debug `thread::id` stamp would catch a signal-handler caller.
- N4: `event::awaiter` gained a state byte and `deadline` a reference; the sizes in T5 print the awaiters but not `deadline`/`event::awaiter`. Fine; state them when CR1 removes the reference.
- N5: `TASK_FRAME_POOL` and `TASK_COUNT_FRAMES` change inline functions' definitions per TU (ODR if two TUs of one program differ). `pool_off_test` is its own executable, so fine; say so in the comment.

## Risk checks (each with probe or file:line)

1. **D5 `await_asio` with a token.** Register-before-start and never starting on a requested token: `test_await_token` (lead), mutants D5a, D5c rerun and die (below); my `stress` kind 1/2
   (dispatch inside init, throwing init) with random cross-thread requests: 200,000 rounds ASan, 20,000 TSan, 20,000 Debug, every task finishes once, `other=0`, `live_delta=0`.
   Park/wake count: probe `inline`, ASan and TSan, `token` 0/1 x dispatch/direct inside `init`: `done=1`, the frame resumed after `init` returned (0 resumed inside `init`), exactly 1 io handler
   (the root's own post), so completion inside initiation costs no extra post or park. NF1 (cross-thread request before the start only records and posts): by reading, the cross-thread callback never touches
   `started_`/`pending_` (it posts), the same-thread callback does, and the early-out and throw paths subtract the handler's share exactly when no handler ran (`task_asio.hh:249-265`); D5a/D5b die on rerun.
   Cancellation slot per operation type, probe `ops`, ASan and TSan, cancel on the loop and from another thread, all end `operation_aborted` in under 1 ms and frames return to baseline: `steady_timer::async_wait`,
   `async_read_some`, composed `asio::async_read`, composed `asio::async_write` blocked on a full pipe (64 MiB; returns the partial 540-548 KB with `operation_aborted`), `async_accept(peer, h)`, `signal_set::async_wait`.
   A cancel racing completion returns the real result: probe `stress` kind 0 (`ok` 6,000-10,000 per run, `aborted` the rest, never another code); `race` in the architect's probes.
2. **D8 teardown.** `close()`/`destroy_roots()`/destructor idempotent: probe `idem` (double close, double destroy, spawn after close destroyed unrun, destructor repeats; `live_delta=0`). Random topology fuzz, probe `topo`:
   400 rounds ASan, 150 TSan, each with 2-3 schedulers on separate `io_context`s, 6-13 roots drawn from {mutex holder blocked on an event, token and plain lock waiters, an event set by a dying frame, a frame waiting on another loop's timer},
   a random partial run, a possible token request, random order for close, destroy_roots, scheduler destruction, mutexes and events owned outside the roots: every round `alive == 0`, both mutexes free afterwards,
   `live_frames()` back to base. Crossroot, holder both orders, `grant_destroyed`, destructor spawning in phase 3 are the lead's tests plus the architect's probes ported (results files read, ctest 4 of 4 in my ASan build).
   Pending operations of every type at teardown, probe `ops_teardown`: the six `ops` operations outstanding, roots destroyed before the sockets and sockets before the roots, then scheduler, then `io_context`: ASan clean, no leak count.
   CR6's precondition holds by the phases (every `unlock` in phase 3 runs on the owner's thread, after the join); X3 mutant D8g dies on rerun. `~event` orphan count: mutant R4 (no `fetch_add`) dies at `sync_test.cc:949`.
   Gaps found: N2, N3.
3. **D7 forwarding.** Host-shaped wrapper with a virtual base and multiple inheritance (`vhost : m1, m2 : virtual vbase`), probe `vhost`: cross-thread cancel through its `post_call`, spawn as a root, torn down by the phases,
   `sizeof(scheduler_ref) == 16`, `of(vhost&) == of(vbase&)` here (a single-offset layout; with a non-zero base offset the two would differ, pre-existing behavior of `==`, relevant to C3's `current() == sched_`, not C2).
   Consumers, `clang++ -fsyntax-only` against the branch headers (`.scratch/review-c2/consumer.cc`): strata `executor.hh` and `work_pool_executor.hh`, prospect's `pool_scheduler` shape, and Xapiand `host.h` with `engine::BlockingScheduler::run`
   instantiated (so `scheduler_ref::of(Host)`): clean. Full strata, prospect, residency, Xapiand builds not run.
4. **`~deadline` assertion.** Ruling above (CR1).
5. **K11, K12, D9, D10, proof quality.** K11: `idem` and mutants D8a/D8b; mutants R1 and R2 (closed check removed from `post_call`, `post_demoted`) both die at `asio_test.cc:935`; R3 (`destroy_roots` keeps the demoted handles) dies at `:791`; R5 (destroy under the list lock) dies by hang; R6 (`emit_posted`
   keeps its count) dies by hang; R10 (a finished root does not unlink) dies in ASan in `destroy_all`. K12: mutant K12a dies in `pool_off_test` (rel); the test asserts `>= 1000` frame `operator new`s with the pool off. D9: mutant D9a dies at `test.cc:668-669` (`destroyed` and `live_frames()`).
   D10: `git grep as_awaitable` and `lock_until|lock_for` over code, tests, bench, README, API.md, ARCHITECTURE.md: empty. Values, not counts: the checks compare `ec`, byte counts, `destroyed`, `live_frames()`, `try_lock()`. Builds are separate directories (the lead's four; mine ASan per mutant). Survivors: R8 (CR2), R9 (CR3).

Mutants rerun by me, on copies in `.scratch/review-c2/mut/` (`mut.py ID`, results `mut-results*.txt`):

| Mutant | Build | Result |
| --- | --- | --- |
| D8g (X3) | asan | dies: use-after-poison in `lock_waiter::wake` |
| D8e (`set()` marks nothing) | asan | dies: heap-use-after-free |
| D5c (no resume count) | asan | dies: `asio_test.cc:530`, `:550`, use-after-poison |
| D5a (NF1 early-out continues in place) | asan | dies: use-after-poison |
| D9a | rel | dies: `test.cc:668-669` |
| K12a | rel | dies: `task_pool_off FAILED` |
| R1, R2, R3, R4, R5, R6, R10 (mine) | rel/asan/dbg | all die (lines above) |
| R8 (mine) | rel | **survives** (CR2) |
| R9 (mine) | asan | **survives** (CR3); dies under my `stress` probe |

## Scope and design questions

Within the design; nothing beyond D5, D7-D10, K9-K12 found. For the architect: (1) D4's "deadline destroyed on its loop" rule should go with CR1; (2) D8 should state "the `io_context` is not run again after phase 3" (N2);
(3) the D5 token overload's result type for non-default-constructible `R` (CR4) belongs in the interface before reactor's step R.

## Probes kept (`.scratch/review-c2/`)

`probe_c2.cc` (modes `ops`, `ops_teardown`, `inline`, `stress N`, `nosched`, `idem`, `poll_after`, `poll_after_emit`, `topo N`, `vhost`, `req_in_init`, `dl_*` incl. `dl_stress N`), `build.sh KIND SRC [HDRDIR] [flags]`
(rel|dbg|asan|tsan; `TAG=x` suffixes the binary), `mut.py ID...` (the lead's table plus R1-R10), `consumer.cc`, `accept_sig.cc`, logs `build*.log`, `mut-results*.txt`, `consumer.log`, `accept_sig.log`. Binaries removed.

## Conditions to land

1. CR1: remove the `~deadline` assertion and the `sched_` member; update the death test, mutant D8i and the D4/D8 text.
2. CR2: add the two `req_in_init` cases; ledger row for R8.
3. CR3: add the `stress_one` cross-thread-versus-completion test; ledger row for R9.
4. CR4: support, or `static_assert` and document, a non-default-constructible `R` in the token overload.

Blocking items: none. Re-check: `rechecker`, scoped to CR1-CR4 and the N2 text, rerunning `req_in_init`, `stress`, `dl_*` and mutants R8, R9. No security review needed; no escalation.

Not run: the project's full ctest under TSan, rel and g++ (the lead's); full strata, prospect, residency, Xapiand builds (K6, C3); Linux CI; benchmarks (T4); `mt_sched` (not found); real weak-memory hardware; leak checking (LSan is not available on this macOS toolchain; `live_frames()` and ASan poisoning stood in).
