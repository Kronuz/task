# task cancellation: notes from the Xapiand lead

> Status: CURRENT (2026-11-02) consumer review of `docs/design/cancellation.md` at task:6d361ca for
> strata, prospect, residency and Xapiand. Not the architect's review.

Verdict: the design works for all four consumers with two gaps fixed (findings 1 and 2) and one
factual correction (finding 3). strata, prospect and residency only poll tokens or name
`scheduler_ref`, as D11 says (checked at strata:ed17f37, prospect:465ee7d, residency:38cf5a4);
they need a rebuild and T6, nothing else.

## Findings

1. **D8.4 covers the waiter side of a lock, not the holder side.** Destroying a root that holds an
   `async_mutex` guard runs `unlock`, which wakes the next waiter by posting to *its* scheduler. In
   Xapiand that is the common case: a request holding an index's turn on loop A, another waiting
   for it on loop B. If B's scheduler is destroyed before A's roots, the wake posts to a dead
   scheduler; if B is mid-destructor, it posts into a list being torn down. D8.3's "stop every
   scheduler first" does not settle the order between destructors. Proposal: a two-phase teardown
   in the owner's contract (stop all, then each scheduler enters a closing state in which `post`
   and `post_demoted` drop the handle, since its roots die with it anyway, then destroy roots, then
   destroy schedulers), or an equivalent rule. T1 gains: holder on A, waiter on B, destroyed in
   both orders under ASan; the same for an `event` set by a dying frame.
2. **D6's depth bound stops stack growth, not reentrancy.** An inline resume runs the coroutine's
   continuation inside the completion handler of whatever initiated the operation. For asio's own
   operations that is safe (the handler is invoked once the I/O object's state is consistent). For
   a hand-written operation it may not be: Xapiand's `Request::read_chunk`
   (`search_views.cc:2944`) awaits http's `BodyStream::async_read`, and its continuation calls
   `async_read` again at once. If `BodyStream::push` invokes the waiting handler synchronously
   while mid-update, the inline resume re-enters the stream. Proposal: state the rule in D6 (an
   operation's completion handler must be invoked with its object's state consistent and outside
   its own critical section) and check every hand-written operation in task, reactor and http
   against it; T1 gains a streamed-body loop (push 10,000 chunks, read each with an inline resume)
   under ASan and TSan.
3. **Q1's "nothing calls them yet" is wrong for Xapiand.** `ShardEndpoint::take_turn` already
   calls `turn.lock_until(*host, deadline)` (`src/database/pool.cc:350`, landed in phase 7c X2).
   That does not change the answer: **Q1 (a)**. I migrate it to `lock(deadline.token())` in the
   http wave, and the turn wait gains shutdown cancellation by linking the deadline to the
   request's token. Please correct the text so C1 does not land on a false premise.

## Questions

- **Q1: (a).** One timeout mechanism; Xapiand migrates its one call site (finding 3).
- **Q2: (a).** Every Xapiand await is loop-local: `signal_loop` (`manager.cc:473`) and
  `read_chunk`. Keep the Debug assertion.
- **Q3: (a).** strata's `inline_executor` drains in its destructor; root destruction there would
  reach dead objects in its tests.

## Xapiand's side

- `await_asio` call sites: `signal_loop` relies on no queue trip (it runs `signal_sig_impl` on the
  control loop either way); `read_chunk` is finding 2.
- Teardown order, owed by Xapiand in the http wave: the index chores' `WorkGroup`s are cancelled
  and drained before any loop's scheduler is destroyed, since a chore completing later resumes on
  that scheduler (`pool.cc` `ChoreAwaiter`). I add a shutdown test that asserts `live_frames()`
  returns to baseline with requests suspended on a turn, a chore and a body read.
- `AsioHost` wraps the reactor's scheduler (http D2, reactor step) and loses its own.
- T6 for Xapiand: ctest, `async_check --strict`, the docs E2E (641 assertions), `stall_load.py` and
  the Lua flood harness, run by me on the C3 pin.

## On cost

T3's `posts` row targets asio's own counts (4.00 io handlers per round trip, `operator new` no
higher than asio's), and frames already come from task's pool (`task.hh:236-298`). That is the
owner's preference: parity, with the "considerable" threshold only as the case that comes back to
the owner with numbers.
