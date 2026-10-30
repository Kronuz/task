> Status: CURRENT (2026-10-29) the ledger for phase 7c step L1 (Kronuz/task); nothing supersedes it.

# phase7c-l1

- Design: Xapiand `docs/design/phase7c.md` (D2, D3, D5, D14, D16, section 5 L1) at `Xapiand:f3bd508f8`;
  reviews: `docs/reviews/phase7c-design.md` (R2 closed by this step; R8 blocks landing and is fixed here; C7 not touched).
- Branch / worktree: `phase7c/l1-task` at `~/code/Kronuz/task-wt/phase7c-l1`; head: `git log -1`.
- State: in review (awaiting CI and the code review)
- Done criteria (from the lead's brief):
  - [x] `event`, `async_mutex` with `lock_until`/`lock_for`, `await_asio`, per-resume slice, `demoted()`, destructor asserts (event, async_mutex, asio_scheduler): `task.hh`, `task_asio.hh`
  - [x] tests for every primitive: `test/sync_test.cc` (slice, event, mutex, lock_until orders, asserts), `test/asio_test.cc` (demoted, await_asio, fresh slice, real timers, 4-reactor contention)
  - [x] a mutant per claim, killed (below)
  - [x] ASan+UBSan and TSan builds clean, Homebrew LLVM 23 (`/usr/local/opt/llvm`): all three test binaries, each build
  - [x] R8: `test_lock_until` cases A (timeout first) and B (grant first, timer completion queued) run ASan-clean; the same cases with a block owned by the frame alone report use-after-poison in `unlock()` (m1)
  - [x] `cpp-libraries/tools/preflight.sh`: mac-hardened, mac-asan-ubsan, linux, freebsd all ok
- Done: implementation and tests (`task:60b386c`), docs and CI trigger for `phase*/**` (`task:e8349d8`), late-timer test (see log).
- Doing: nothing.
- Next: CI on the pushed branch (lead watches), then the code review.
- Blocked on: nothing.

## Design points the reviewer should know

- Every queued waiter, timed or not, is one pool-allocated refcounted `lock_waiter` (queue + awaiter + timer callback), not a node in the frame as D5 words it for the plain lock. One mechanism for both; no allocation after warm-up (frame pool).
- Settling is one compare-exchange; a timed-out entry stays queued until a later `unlock` or `lock` sheds it, so the timer callback never touches the mutex (which may be destroyed once the last holder leaves).
- `await_suspend` ends with a `park`/`wake` handshake, so a grant or timeout that lands before it returns continues the coroutine in place, once.
- `lock_until(S&, steady_clock::time_point)` is a member template: `S` supplies `post`, `now` and `start_timer(time_point, F)`; `asio_scheduler` does. The core header still includes no Asio. `lock_for(S&, ns)` adds the timeout to `S::now()`.
- The frame pool poisons cached blocks under ASan (offset 16 on), so use after free of any pooled frame or waiter is reported.
- `running_on` stamps the slice start; `inline_scheduler` re-stamps per handle. A host that wraps a whole loop in one `running_on` must wrap each resume instead (the test `thread_scheduler` did, and was changed).
- `post_demoted` stays executor-thread only (C7/C10.1: chores resume through `post`); not asserted.

## Mutants (copies of the headers in `.scratch/mut/<name>/`, run with `test/sync_test.cc` under ASan+UBSan)

| Mutant | Result |
|---|---|
| m1 no shared waiter state (frame alone owns the block; queue and timer hold no reference) | killed: use-after-poison in `async_mutex::unlock()` from `test_lock_until` |
| m2 slice not reset on resume (no stamp in `running_on` or `inline_scheduler`) | killed: `suspended_demotions == 0`, the trace check, the nesting check |
| m2b stamp removed from `inline_scheduler` only | killed: the trace check |
| m3 lock granted after a timeout (grant skips the compare-exchange) | killed: CHECK at the free-lock probe, then a crash on the double resume |
| m4 no `park`/`wake` handshake | killed: use-after-poison (cases E/F) |
| m5 event resumes waiters inline in `set()` | killed: `order.empty()` check |
| m6 destructor asserts removed | killed: the fork-and-abort checks |
| m7 late timer stores `timed_out` without the compare-exchange | SURVIVED the first tests; killed after adding `ms.ready() == 0` to case B |
| m8 queue LIFO | killed: FIFO trace |
| m9 unlock releases instead of handing over | killed |
| m10 event waiters LIFO | killed |

## Log
- 2026-10-29: started; read design D2/D3/D5/D14/D16, re-check 1 R2/R8, task sources.
- 2026-10-29: built and tested (`task:60b386c`); docs and CI trigger (`task:e8349d8`); mutants m1-m10; m7 survived and got a stronger test; preflight green on mac, linux, freebsd (the first freebsd run failed on a missing `<csignal>`, then the VM's ssh timed out once; both fixed or retried).
