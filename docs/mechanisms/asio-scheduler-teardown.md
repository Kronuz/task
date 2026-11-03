> Status: CURRENT (2026-11-03) how an `asio_scheduler` and its spawned tasks are torn down, read from `task_asio.hh` and `task.hh` at task:d080786.

# asio_scheduler teardown

`spawn` on an `asio_scheduler` makes the task a root: it stays linked in the scheduler's
`root_list` until it ends. Roots that are still suspended when the program stops (waiting on
a timer, an event, a lock, another scheduler) hold frames, and those frames hold awaiters other
threads may touch. Teardown destroys them in an order that never frees a frame a running
handler can still reach.

## The four phases

Across every scheduler and pool a root may be suspended on (`task_asio.hh:52`):

1. **Stop.** Stop and join every loop thread. The owner does this; the library does not.
2. **Close.** `close()` on every scheduler (`:104`). `root_list::close` (`task.hh:647`) sets the
   closed flag under the list's lock. From then on `post`, `post_demoted` and `post_call` drop
   what they are given, `spawn` destroys its task (and the `done` callback) unrun instead of
   linking it, and a handler already queued checks `closed()` before touching its frame (`:72`,
   `:92`).
3. **Destroy roots.** `destroy_roots()` on every scheduler (`:110`), with every `io_context`
   still alive. It pops one root at a time under the list's lock, destroys it outside the lock
   (`task.hh:658`), then forgets the demoted handles. A root owns the chain it awaits, so
   destroying it destroys every frame beneath it; each primitive's destructor leaves its state
   consistent (a granted lock waiter unlocks, a registered callback deregisters, an event
   waiter unlinks, `when_all` destroys unfinished children).
4. **Destroy.** The schedulers, then the `io_context`s. An operation still pending in a stopped
   `io_context` is dropped unrun when it is destroyed.

`close()` and `destroy_roots()` are idempotent and run on the owner's thread after the join,
never on a loop thread (`assert(!runs_here())`). `destroy_roots()` asserts that a scheduler with
live roots has a stopped `io_context`. The destructor runs phases 2 and 3 itself, so a stopped
loop that the owner did not tear down is still safe. The `io_context` is not run again after
phase 3; if it is, the handlers queued earlier find `closed()` and do nothing.

```cpp
loop_a.stop(); loop_b.stop();            // 1: join every loop
sched_a.close(); sched_b.close();        // 2: nothing new is linked or resumed
sched_a.destroy_roots(); sched_b.destroy_roots();  // 3: a root may wait on the other's timer
```

## What each phase guarantees

- After 1: no thread is running a handler, an `unlock`, or a `cancel_callback` against any
  frame, so phase 3 races with nothing.
- After 2: a resume or cancel emit queued before the close is dropped, and nothing new links.
  A destructor that runs in phase 3 and spawns onto the closed scheduler has its task destroyed
  at once, unrun, and its `done` never called.
- After 3: every root and every frame it awaited is destroyed exactly once, whichever scheduler
  it was suspended on; a root on one scheduler waiting on another's timer is covered because
  every `io_context` is alive.

## Zero live frames

Frames, lock waiters and cancel states come from `pooled_frame`. With `TASK_COUNT_FRAMES`
defined, `detail::live_frames()` counts the blocks alive (`task.hh:421-443`). The tests record it
before building the scenario and assert it equals the baseline after phase 4 (plus the states of
sources the test itself still holds), which shows the phases leak nothing and destroy nothing
twice under ASan. Every translation unit of a program must define `TASK_COUNT_FRAMES` the same.

## Invariants

1. Phases 2 and 3 run after every loop has joined, on the owner's thread.
2. Close on every scheduler precedes `destroy_roots` on any.
3. A closed scheduler never resumes a frame or runs a posted function; `spawn` on it destroys the
   task unrun and never calls `done`.
4. `destroy_roots()` destroys each root outside the list's lock.
5. The `io_context` outlives `destroy_roots()` on every scheduler that has roots on it.

## Tests

`test/asio_test.cc` `test_teardown`: a loop with roots suspended in `await_asio` (with and
without a token), an event, a lock wait, a demoted checkpoint, a `when_all`, and a frame over
2 KiB; `live_frames` back to baseline (1, 3, 4, zero live frames). A root on A suspended on B's
timer; a lock holder and waiter on two loops destroyed in both orders; an event set by a dying
frame (2, 5). Spawn, post, post_demoted and post_call on a closed scheduler (3). The
`io_context` run again after phase 3 drops queued resumes and emits (3). A destructor that spawns during phase 3. The destructor running
the phases. `test_inline_resume` tears down with a chain in flight. `close_on_loop` and
`destroy_roots_on_running_loop` abort in Debug (1).
