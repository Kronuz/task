> Status: CURRENT (2026-11-02) how `async_mutex` and `lock(cancel_token)` work, as built in step C1 of `docs/design/cancellation.md` (phase/cancellation-c1); supersedes the phase 7c L1 `lock_until` note (task:9a85e89).

# async_mutex and lock(token)

`task::async_mutex` is a FIFO mutex for coroutines that run on several schedulers at once.
A task that cannot take it suspends, and the unlocker resumes it through the scheduler it
suspended on. Nothing blocks a thread. `lock(token)` gives up when a `cancel_token` is
requested; a timed lock is `lock(deadline.token())`.

## State

```
async_mutex
  m_      short std::mutex over the three fields below (no I/O, no waiting inside)
  locked_ the lock is held
  head_ ... tail_   FIFO queue of lock_waiter blocks (intrusive, singly linked)

lock_waiter  (pool-allocated, refcounted)
  handle, sched     the suspended coroutine and the scheduler to resume it on
  state             waiting -> granted | timed_out   (one compare-exchange)
  refs              queue entry + awaiter
  parked            handshake with await_suspend
```

## Lock and unlock

- `lock()` takes the lock if `locked_` is clear. Otherwise it appends a `lock_waiter` and
  suspends.
- `unlock()` pops waiters until one settles as `granted`, leaves `locked_` set (the lock is
  handed over, so a late `lock()` queues behind it), drops the mutex's internal lock and
  posts the waiter to its scheduler. If none is left it clears `locked_`.
- A guard may be released from any thread; the next holder need not share a thread with the
  last.

## lock(token)

1. A free lock is taken whatever the token (`await_ready` tries it).
2. A requested token that finds the lock held returns an empty guard without queueing.
3. Otherwise the awaiter queues a `lock_waiter`, then registers a `cancel_callback` on the
   token whose body settles the waiter as `timed_out` and wakes it. An empty token registers
   nothing, so `lock(cancel_token{})` behaves as `lock()`.
4. `await_resume` destroys the registration first (it waits for a callback running on
   another thread), then reads the outcome: a guard if granted, an empty guard otherwise.

The callback borrows the awaiter's reference to the waiter instead of holding its own: the
registration is destroyed before the awaiter releases that reference, and destroying it waits
for a callback still running, so the callback never meets a freed waiter.

A `deadline` is a cancel source whose timer requests `cancel_reason::deadline`. Destroying a
timer does not stop an expiry Asio has already queued, so the timer callback holds the
deadline's cancel state, and `~deadline` sets a `disarmed` bit in the state word before
destroying the timer; the callback's request is one compare-exchange that fails when the bit
is set. A deadline destroyed after a grant therefore never requests its token, queued
expiry or not.

The race between a grant and a cancel ends in either order without meeting freed state:

- *Cancel first.* The callback settles `timed_out` and posts the waiter. The frame finishes
  and ends. The entry is still queued, and holds the block alive; the next `unlock` pops it,
  sees it is not `waiting`, and drops it. The callback never touches the mutex, which may be
  gone by then.
- *Grant first.* `unlock` settles `granted` and posts the waiter. The frame runs its turn and
  deregisters the callback; a request that comes later runs no callback, and one that comes
  while the callback is running finds its compare-exchange failing on the settled block.

## await_suspend handshake

A grant or a cancel can land before `await_suspend` has returned: on another thread, or on
this one between queueing and registering (a token requested in that window runs the
callback inside the `cancel_callback` constructor). `park` (end of `await_suspend`) and `wake`
(the settler) each swap `parked` to true. Whoever swaps second acts: if `wake` is second it
resumes the waiter; if `park` is second it returns false and the coroutine continues in
place, exactly once.

## A frame destroyed while waiting

An awaiter destroyed before `await_resume` (its frame destroyed while suspended) reads the
waiter's state with one compare-exchange from `waiting` to `timed_out`:

- `waiting`: now `timed_out`, so the holder's `unlock` sheds it.
- `granted`: the lock was handed to this frame, which will never run, so the destructor
  `unlock`s it and the lock passes to the next waiter.
- `timed_out`: a cancel already settled it; the holder still holds the lock, and nothing is
  unlocked.

The cancellable awaiter destroys its registration before reading the state.

## Lifetime rules

- The mutex must outlive its waiters and be unlocked when destroyed (asserted in Debug). A
  mutex local to one root and awaited by another breaks this when the first root dies first:
  its guard grants the other root's waiter and the mutex dies locked, which the assertion
  reports.
- Timed-out entries are shed from the queue's head on each new `lock`, and at every `unlock`;
  between those they cost one pool block each.

Tests: `test/sync_test.cc` (`test_lock_token` forces both orders, the handshake between
queueing and registering through the `after_enqueue` test hook, a requested token, a request
from another thread at every point of the await; `test_lock_destroyed` destroys waiting,
granted and cancelled waiters; `test_deadline` the disarm bit and linking;
`test_destructor_asserts` the ownership rule) and `test/asio_test.cc` (real timers, a
queued expiry, four reactors contending with random deadlines).
