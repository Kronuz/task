# task::timer_wheel

The one timer wheel of the Kronuz libraries (Detent PLAN.md D70, D71): a single-owner
hierarchical wheel with a 64-stripe intrusive inbox for arms from other threads, 32-byte
intrusive entries and no allocation per arm. It is candidate (f) of Detent's
`docs/design/wheel-evidence.md`, with upper levels for far deadlines. `task::deadline` runs on it
on `inline_scheduler` and `asio_scheduler`.

## Shape

```
entry (32 B, inside its owner): next | pprev | deadline (ns) | fire(entry&, bool expired)

level 0   4,096 slots x 1 ms        ticks [cursor, cursor + 4,096)
level 1      64 slots x 4.1 s       \
...                                   one 64-bit occupancy word per level, heads in one array
level 6      64 slots x 142 years   /
inbox     64 stripes, a cache line each, and one word of stripe bits
```

- **Arm** is O(1): an entry within a round of the cursor goes in its 1 ms slot (one already due in
  the cursor's slot); a farther one goes in the lowest upper level whose 64 slots reach it, in a
  slot after the cursor's. Cancel unlinks through `pprev`; the slot's bit clears when it empties.
- **Expire(now)** first files what other threads queued, then moves the cursor. Each upper slot
  whose span the cursor entered files its entries again, relative to the new cursor, so they move
  down; an entry moves at most six times in its life, and no tick walks an entry that is not due.
  The due level-0 entries move to a detached list before any fire runs, so a fire may cancel or
  re-arm any entry, itself included; an entry armed during the fires for a time already due fires
  on the next call. An entry fires at most one tick after its deadline and never before.
- **next()** is when `expire` next has work: the end of the next occupied 1 ms slot, or the start
  of the next occupied upper slot's span; 0 while the inbox holds arms; `never` when empty.
  A loop arms one timer at it. A far deadline costs at most one early wake per level it crosses.
- **Destruction** unlinks what is still armed or queued and calls its `fire(e, false)`.
- The clock is the owner's: nanoseconds on `steady_clock` for a reactor, the manual counter of
  `inline_scheduler`. The wheel never reads a clock.

## Threading contracts

1. **Other threads arm only an unarmed entry**, with `arm_remote(e, deadline)`: it links the entry
   into the thread's stripe through `e.next` (a CAS on that stripe's head, no allocation), and the
   push that finds its stripe empty sets the stripe's bit. Each thread takes a stripe on first use;
   past 64 threads stripes are shared. Waking an owner that sleeps is the caller's part.
2. **Only the owner arms, re-arms, cancels and expires.** The first such call binds the owner
   thread and every later one is asserted in Debug; `release()` unbinds it once the loop is
   stopped and closed, so teardown's thread may destroy the wheel. An owner `arm` or `cancel` of an
   unarmed entry files the inbox first when it holds anything, so an entry another thread just
   armed is re-armed or cancelled exactly.

## Deadlines on the wheel

`deadline<S>` takes `S::clock::time_point` when S declares a clock, else
`steady_clock::time_point`. Its cancel state carries the entry (`detail::timed_cancel_state`), so
a deadline allocates only that pooled state, as before; the wheel holds a reference while the
entry is armed. `start_timer` on a scheduler with a wheel files that entry:

- `inline_scheduler` files it in place; `advance(ns)` moves the manual clock and expires, so a
  five-second deadline fires with no real time passing.
- `asio_scheduler` files it at once on its loop thread and posts the filing from any other
  thread; one `asio::steady_timer` wakes the loop at `next()`. Destroying the deadline unfiles it
  at once on the loop and posts the unfile elsewhere; a deadline that empties the wheel cancels
  the wake, so `run()` returns. `close()` releases the wheel; after it nothing touches the wheel
  until the scheduler's destruction drops what is left (teardown phases 2 to 4,
  `docs/mechanisms/asio-scheduler-teardown.md`). The contract is unchanged: a deadline may be
  created and destroyed on any thread, and once destroyed it requests nothing.
- `scheduler_ref::wheel()` returns the scheduler's wheel, or null when it declares none.

## Cost (A1)

Pending the rows.
