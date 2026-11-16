> Status: CURRENT (2026-11-15) how `task::deadline` and timers armed through a scheduler run on its timer wheel, read from `task.hh` and `task_asio.hh` on quest/detent/drivers-task-wheel, on Kronuz/wheel at wheel:bfd72bc.

# Deadlines on Kronuz/wheel

`inline_scheduler`, `blocking_scheduler` and `asio_scheduler` each own one `wheel::timer_wheel`, from
[Kronuz/wheel](https://github.com/Kronuz/wheel). `CMakeLists.txt` fetches it at a pinned commit,
and the `task` target links `wheel::wheel`. The wheel's shape, threading contracts and cost are in
its `docs/timer-wheel.md`. task keeps no wheel code of its own.

## Arming a timer

The wheel itself is private. Each scheduler has one way to arm, on its own thread:
`arm(entry, at)` files a `wheel::timer_entry` at `at` (nanoseconds on the scheduler's clock, as
`now()`), or moves it if armed, and `cancel(entry)` unfiles it, returning whether it was armed.
On `asio_scheduler`, `arm` also wakes the loop by the end of the entry's slot, and a `cancel`
that empties the wheel cancels the wake. `scheduler_ref` forwards `now()`, `arm` and `cancel`
(found by detection, like `roots()`); its `arm` returns false and files nothing on a scheduler
without timers. Deadlines file their entries through the same `arm` and `cancel`, so an entry
armed by a driver and a deadline's entry wake the loop alike.

## The entry and its references

On a scheduler with a wheel, a deadline's cancel state is a `detail::timed_cancel_state`
(`task.hh:1212`). It derives from `wheel::timer_entry`, so the entry is 32 bytes inside a pooled
state, and arming allocates nothing more. On any other scheduler (one whose `start_timer` does not
return a `wheel_timer`), the state carries no entry and links only when there is a parent. The
wheel holds one reference to the state while the entry is filed:

- `start_timer` takes the reference from the `deadline_fire` it is given (`task.hh:1412`) and
  files the entry with `arm`.
- The entry's fire, `on_wheel`, requests `cancel_reason::deadline` unless the deadline is
  disarmed, then releases the reference. When the request loses to one another thread is still
  delivering (a cross-thread cancel between its claim and its hop), the fire waits for its
  callbacks (`settle`), so `blocking_scheduler::drive` sees the hop before deciding it stalled.
- Destroying the deadline sets the disarm bit, then its `wheel_timer` (`task.hh:1431`) hands the
  state to the scheduler's static `drop_timer`, which unfiles the entry with `cancel` and releases
  the reference. On `inline_scheduler` and `blocking_scheduler` it reads the scheduler only while
  the entry is armed, and their destruction fires every entry off the wheel, so a deadline may
  outlive them.

## On each scheduler

- `inline_scheduler` files in place (`task.hh:1475`), so its deadlines are created and destroyed
  on the thread that runs it. `advance(ns)` moves the manual clock and
  expires the wheel, so a five-second deadline fires with no real time passing. `drop_timer`
  cancels an armed entry in place. A fire's callback may `post_call` a hop to the scheduler; it
  runs in the next `run()`, in order with posted resumes.
- `blocking_scheduler` files in place too (`task.hh:1489`), on its driving thread (`arm` and
  `cancel` assert it), and `drive()` expires the wheel on the steady clock after each pass over
  what was queued when the pass began, so a task re-posting itself cannot hold off a fire. With
  nothing queued it sleeps while `may_wait()` holds, toward `next()` or, with no timer armed,
  until a post; a `post` or `post_call` from any thread wakes it early. `drive(done)` waits only
  while a timer is armed. Destroyed, it releases the wheel's owner binding, so the wheel fires
  what is still filed `false` on the destroying thread.
- `asio_scheduler` (`task_asio.hh:161`) files at once on its loop thread and posts the filing from
  any other thread. One `asio::steady_timer` wakes the loop at the wheel's `next()`. Its executor
  runs on one thread, an `io_context` run by one thread: with a strand over a multi-threaded
  `io_context` the posted filings change threads and the wheel's Debug owner check fires. Destroying a
  deadline unfiles it at once on the loop and posts the unfile from elsewhere. An unfile that
  empties the wheel cancels the wake, so `run()` returns. `close()` releases the wheel's owner
  binding. After it, deadlines neither file nor unfile; `cancel` works on teardown's thread
  (`arm` does not), so an entry a driver armed (a group's) unlinks in place in phase 3, after
  `destroy_roots()`. Destroying the scheduler destroys the wheel, which drops what is still
  filed (teardown phases 2 to 4, `asio-scheduler-teardown.md`).

## The wheel's rules, kept

Kronuz/wheel states three rules for its users. task keeps each one:

1. **A `false` fire means the wheel is dying.** `on_wheel(e, false)` requests nothing and only
   releases the reference. Nothing arms or cancels that entry again. `asio_scheduler`'s wheel
   dies with deadlines' entries filed (their unfiles were posted to a stopped loop, or their
   frames were destroyed by `destroy_roots()`), and the scheduler is closed first, so no deadline
   files or unfiles. On `inline_scheduler` and `blocking_scheduler` a deadline cancels its own
   entry while the scheduler lives; one outliving it finds its entry fired `false` and touches
   nothing of the scheduler.
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

A deadline requests `cancel_reason::deadline` never before its time and at most one 1 ms tick
after it, plus, on `asio_scheduler`, the loop's wake latency: the correctness review measured 0.5
to 2.2 ms late in all (Debug and ASan+UBSan, macOS). On `asio_scheduler` it may be created and
destroyed on any thread, before or after its expiry; on `inline_scheduler` and
`blocking_scheduler`, on the thread that runs it. Once
destroyed it requests nothing. Its `time_point` is on `S::clock` when the scheduler declares one,
else on `steady_clock`. Its `wheel_timer` points at the scheduler: an `asio_scheduler` outlives
its deadlines; an `inline_scheduler` or `blocking_scheduler` may die first.

## Tests

- `test/deadline_test.cc`: `manual` (`advance` fires a deadline with no real time; one destroyed
  early leaves the wheel), `chain` (a parent's fire destroys a linked child whose entry waits on
  the due list), `ref` (`scheduler_ref::arm`, `cancel` and `now` on `inline_scheduler`; `arm`
  false without timers), `reentry` (a fire cancels or re-arms another due entry, re-arms itself,
  or arms one already due), `model` (20,000 rounds of random arm, cancel and re-arm, from fires
  too, against a reference model), `long` (deadlines from 4,095 ms to 40 days, never early),
  `post_call` (same-thread `post_call` in order with `post`; a deadline's cancel hops through it).
  `test/wheel_mutant.py rearm-fires-early` builds it against a wheel whose `arm` during an
  `expire` fires the entry in that same `expire`; `reentry` and `model` must fail.
- `test/deadline_asio_test.cc`: `offloop` (deadlines created and destroyed on and off the loop,
  none requests after its destructor returns), `teardown` (the phases with deadlines armed: the
  destroyed scheduler's wheel fires the last references `false` and frees them), `idle` (an
  unfile that empties the wheel lets `run()` return), `direct` (an entry armed through
  `scheduler_ref::arm` with no deadline armed wakes the loop and fires on time; one cancelled
  through it never fires), `far` (an entry past the clock's range does not spin the loop),
  `loops` (a parent on one loop, linked children made and destroyed on another while it fires),
  `roots` (`destroy_roots()` of frames holding deadlines, then a group's entry cancelled on
  teardown's thread). `offloop-arm` (a cancel off a running loop) and `closed-arm` (an arm on a
  closed scheduler), not in the default run, must abort on the scheduler's assertion; each exits
  0 only without one.
- `test/blocking_test.cc`: `blocking_scheduler` under the blocking entry's cases: a cross-thread
  cancel wakes a sleep toward a 30 s deadline, a 200 ms deadline fires on the steady clock, a
  lost wake returns at once with or without a live token, 300 cross-thread cancels, and `post`
  and `post_call` from another thread cut a sleep short. `wakes-unarmed`: with nothing armed and
  `may_wait()` true, a post wakes it, 5,000 rounds with the post around the decision to sleep;
  `starve`: a call re-posting itself does not hold off a 20 ms timer; `late`: a deadline outliving
  its scheduler touches nothing of it; `race` (1,000 cross-thread cancels against a deadline
  firing at the same time) and `race-forced` (the cancel's callback held between its claim and
  its hop while the deadline fires) end cancelled, never stalled; `window-forced`: a post made
  between drive's queue check and its sleep runs at once. `offthread-cancel`, not in the default
  run, must abort.
- `test/wheel_mutant.py NAME` builds a test against one mutant and exits 0 when it is killed:
  `rearm-fires-early` (wheel.hh; `deadline_test reentry`, `model`), `unarmed-returns`
  (`wakes-unarmed`), `fire-skips-settle` (`race-forced`), `blocking-lost-wake` (no recheck and no
  wait predicate; `window-forced`), `expire-when-idle` (`starve`), `drop-calls-dead-scheduler` (`late` under
  ASan), `blocking-cancel-unchecked` (`offthread-cancel`), `closed-arm-allowed`
  (`deadline_asio_test closed-arm`).
- `test/deadline_alloc_test.cc`: no allocation per deadline once the pool is warm, on
  `inline_scheduler`, `blocking_scheduler` and `asio_scheduler`.
- `test/asio_test.cc` and `test/sync_test.cc` run the deadline contract unchanged.
