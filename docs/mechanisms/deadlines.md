> Status: CURRENT (2026-11-12) how `task::deadline` runs on the schedulers' timer wheels, read from `task.hh` and `task_asio.hh` on Kronuz/wheel at wheel:bfd72bc.

# Deadlines on Kronuz/wheel

`inline_scheduler` and `asio_scheduler` each own one `wheel::timer_wheel`, from
[Kronuz/wheel](https://github.com/Kronuz/wheel). `CMakeLists.txt` fetches it at a pinned commit,
and the `task` target links `wheel::wheel`. The wheel's shape, threading contracts and cost are in
its `docs/timer-wheel.md`. task keeps no wheel code of its own.

## The entry and its references

A deadline's cancel state is a `detail::timed_cancel_state` (`task.hh:1013`). It derives from
`wheel::timer_entry`, so the entry is 32 bytes inside a pooled state, and arming allocates
nothing more. The wheel holds one reference to the state while the entry is filed:

- `start_timer` takes the reference from the `deadline_fire` it is given (`task.hh:1209`) and
  files the entry.
- The entry's fire, `on_wheel`, requests `cancel_reason::deadline` unless the deadline is
  disarmed, then releases the reference.
- Destroying the deadline sets the disarm bit, then its `wheel_timer` (`task.hh:1225`) hands the
  state to the scheduler's `drop_timer`, which unfiles the entry and releases the reference.

## On each scheduler

- `inline_scheduler` files in place (`task.hh:1269`). `advance(ns)` moves the manual clock and
  expires the wheel, so a five-second deadline fires with no real time passing. `drop_timer`
  cancels an armed entry in place.
- `asio_scheduler` (`task_asio.hh:140`) files at once on its loop thread and posts the filing from
  any other thread. One `asio::steady_timer` wakes the loop at the wheel's `next()`. Destroying a
  deadline unfiles it at once on the loop and posts the unfile from elsewhere. An unfile that
  empties the wheel cancels the wake, so `run()` returns. `close()` releases the wheel's owner
  binding. After it, nothing files or unfiles, and destroying the scheduler destroys the wheel,
  which drops what is still filed (teardown phases 2 to 4, `asio-scheduler-teardown.md`).
- `scheduler_ref::wheel()` returns the scheduler's wheel, or null when it declares none.

## The wheel's rules, kept

Kronuz/wheel states three rules for its users. task keeps each one:

1. **A `false` fire means the wheel is dying.** `on_wheel(e, false)` requests nothing and only
   releases the reference. Nothing arms or cancels that entry again. Only `asio_scheduler`'s
   wheel dies with entries filed (their unfiles were posted to a stopped loop), and the scheduler
   is closed first, so it neither files nor unfiles. `inline_scheduler` outlives its deadlines,
   and each deadline cancels its own entry.
2. **A fire may destroy its own entry.** When the deadline is already gone, the wheel holds the
   last reference, and the release in `on_wheel` frees the state the entry lives in. The wheel
   never touches an entry after its fire.
3. **Entries outlive the wheel or are cancelled.** The entry lives as long as its state, and the
   wheel's reference keeps the state alive while the entry is filed. A deadline destroyed before
   the wheel cancels its entry; a wheel destroyed first fires each entry `false`.

Every cancel is exact. task never calls `arm_remote`. Off-loop filings and unfilings travel
through `asio::post`, so only the owner thread touches the wheel, and `armed()` there is the
truth.

## The contract

A deadline requests `cancel_reason::deadline` at most 1 ms after its time and never before. It
may be created and destroyed on any thread, before or after its expiry, and once destroyed it
requests nothing. Its `time_point` is on `S::clock` when the scheduler declares one, else on
`steady_clock`. Its `wheel_timer` points at the scheduler, so the scheduler outlives the deadline.

## Tests

- `test/deadline_test.cc`: `manual` (`advance` fires a deadline with no real time; one destroyed
  early leaves the wheel), `chain` (a parent's fire destroys a linked child whose entry waits on
  the due list), `ref` (`scheduler_ref::wheel()`).
- `test/deadline_asio_test.cc`: `offloop` (deadlines created and destroyed on and off the loop,
  none requests after its destructor returns), `teardown` (the phases with deadlines armed: the
  destroyed scheduler's wheel fires the last references `false` and frees them), `idle` (an
  unfile that empties the wheel lets `run()` return).
- `test/deadline_alloc_test.cc`: no allocation per deadline once the pool is warm.
- `test/asio_test.cc` and `test/sync_test.cc` run the deadline contract unchanged.
