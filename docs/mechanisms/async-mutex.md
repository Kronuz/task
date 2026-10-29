> Status: CURRENT (2026-10-29) how `async_mutex` and `lock_until` work, as built in phase 7c step L1 (phase7c/l1-task).

# async_mutex and lock_until

`task::async_mutex` is a FIFO mutex for coroutines that run on several schedulers at once.
A task that cannot take it suspends, and the unlocker resumes it through the scheduler it
suspended on. Nothing blocks a thread.

## State

```
async_mutex
  m_      short std::mutex over the three fields below (no I/O, no waiting inside)
  locked_ the lock is held
  head_ ... tail_   FIFO queue of lock_waiter blocks (intrusive, singly linked)

lock_waiter  (pool-allocated, refcounted)
  handle, sched     the suspended coroutine and the scheduler to resume it on
  state             waiting -> granted | timed_out   (one compare-exchange)
  refs              queue entry + awaiter (+ one per timer callback)
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

## lock_until

`lock_until(sched, deadline)` is `lock()` plus a timer that `sched` starts
(`asio_scheduler::start_timer` is a `steady_timer`). The timer callback owns one reference
to the waiter and, at the deadline, tries to settle it as `timed_out`. The deadline is
ignored when the lock is free.

The waiter's block has three holders that end in any order:

1. The queue entry, until `unlock` pops it or the mutex is destroyed.
2. The awaiter in the waiting frame, until `await_resume` (or the frame's destruction).
3. The timer callback, until it runs or the timer is destroyed unrun.

Whichever drops last frees the block, so neither order of the race meets freed state:

- *Timeout first.* The callback settles `timed_out` and posts the waiter. The frame finishes
  and ends. The entry is still queued, and holds the block alive; the next `unlock` pops it,
  sees it is not `waiting`, and drops it. The timer never touches the mutex, which may be gone
  by then.
- *Grant first.* `unlock` settles `granted` and posts the waiter. The frame runs its turn and
  ends, and cancels its timer. A callback that Asio had already queued still runs later: its
  compare-exchange fails on the settled block, which its reference keeps alive.

## await_suspend handshake

A grant or a timeout can land on another thread before `await_suspend` has returned, while the
frame is still suspending. `park` (end of `await_suspend`) and `wake` (the settler) each swap
`parked` to true. Whoever swaps second acts: if `wake` is second it resumes the waiter; if
`park` is second it returns false and the coroutine continues in place, exactly once.

## Lifetime rules

- The mutex must outlive its waiters and be unlocked when destroyed (asserted).
- A waiter runs to completion; nothing destroys a suspended frame. The awaiter's destructor
  only drops its reference.
- Timed-out entries are shed from the queue's head on each new `lock`, and at every `unlock`;
  between those they cost one pool block each.

Tests: `test/sync_test.cc` (`test_lock_until` forces both orders, the handshake inside
`start_timer`, a failing timer) and `test/asio_test.cc` (real timers; four reactors contending
with random deadlines). The probe that found the original lifetime bug is the architect's
`lock_until_probe.cc` (phase 7c re-check 1, R8); its orders A and B are cases A and B of
`test_lock_until`.
