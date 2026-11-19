# Lifecycle and housekeeping

How `sleep_until`, `group`, `periodic`, `debounce`, the stop sources and `lifecycle` work. The
design and the daemons' mapping are in [service-shape](../design/service-shape.md).

## Sleeps

A `sleep_awaiter` is a `wheel::timer_entry` in the waiting frame. `await_suspend` arms it through
the current scheduler's `arm` and registers a `cancel_callback` on the token. One
compare-exchange on its state settles it `fired` or `cancelled`:

```text
wheel expire (loop)  -> CAS waiting->fired     -> post(handle)
token request (any)  -> CAS waiting->cancelled -> post_call(hop) -> hop on the loop: cancel(entry), post(handle)
dying wheel          -> CAS waiting->dead      -> frame stays suspended for teardown
```

The loser does nothing, so the frame resumes once, except that a fire losing to a cancel
settles the token (`cancel_state::settle`, as a deadline's fire does): it waits until the
request delivering the callback on another thread has posted the hop, so a driver that finds its
queue empty after `expire()` never misses it. The hop is how a cancel from another thread
unfiles an entry that only the loop may touch; a scheduler without `post_call` asserts. `sleep_until(never, token)` arms nothing and waits
for the token alone. A frame destroyed while sleeping deregisters its callback (which waits for
one running elsewhere), then cancels its entry.

## group

A counter and a list of joiners under one mutex. `spawn` wraps the task in a frame whose `ended`
guard drops the count however the frame ends, a teardown's destroy included. The last drop takes
the joiner list under the lock and resumes each through the scheduler it suspended on. A join's
token unlinks its waiter under the same lock, so a waiter is resumed by exactly one of the two.

## periodic and debounce

`periodic` is a loop over `sleep_for(period, stop)`: the period runs from the end of the previous
tick, a stop ends the pending sleep at once, and a throwing tick goes to `on_throw`. `debounce`
keeps an entry per key under a mutex: a target, a force time, the OR of the touches' flags, and
whether a fire is pending. A touch that creates an entry spawns one watcher into the group, tagged
with the entry's generation so a key never has two. The watcher sleeps until the target, rereads
it (touches only move it later) and, once it has passed, takes the flags and clears `pending`
before it fires, so a touch from then on starts the next fire. Without a throttle it erases the
entry; with one it keeps it, throttled until `throttle` after the fire, and a touch meanwhile
targets no earlier than that end. The watcher stays with the entry and erases it once a throttle
passes untouched, so entries are bounded by the pending keys and the keys fired in the last
`throttle`. A throwing fire drops a throttled entry and hands a touched one to a new watcher.

A stop cancels each sleep; the watcher erases a throttled entry and leaves a pending one for
`fire_pending()`, which takes every pending entry under the mutex, waits for a fire in progress on
the loop to end (a count of fires and a condition variable), and fires the taken keys on its own
thread. A key the loop is firing is no longer pending, so it fires once, and every touch before
`fire_pending()` has fired when it returns: the third signal's flush.

## Stop sources

`stop_source` holds two `cancel_source`s, stop and hurry (a hurry also stops), the first
`fail()`, and the flushes the third signal runs. A nested source links both to its parent's
tokens. `run_flushes()` walks the flushes newest first with no lock held while each runs (a
flush may call `fail()`); a removal waits until no run is in progress, so a lifecycle's set
outlives its flush. `stop_signals` is a `stop_source` whose `sigaction` handler counts itself in,
loads the pipe's fd, writes the signal number to the non-blocking pipe, and counts itself out; its
thread reads it: the first stop signal requests stop, the second hurry, the third runs every
flush, restores `SIG_DFL` and raises the signal. Its destructor restores the old handlers, stores
-1 as the fd, waits for the count to reach zero, writes a zero byte to end the thread, and only
then closes the pipe, so no handler writes to a reused descriptor.

## lifecycle

`components` erases each component to a row of function pointers, so the set iterates forward to
start and backward to stop, drain and flush. It counts the started ones; only those are stopped,
drained and flushed. A start that throws records the failing label and rethrows; a nested set
whose start threw still counts as started, so its started children unwind in its parent's order.
`drain_all` returns lines and the first failure together; a drain that leaves lines ends the
drains, since what it left may still use the earlier components.

The driver starts the set on its own stop token, linked to the source's, calls `ready`, and
sleeps on that token; a start or `ready` failure requests it, so housekeeping a started component
runs on it ends before its drain. At the stop it spawns a bound timer: `sleep_until(now + bound -
grace)` on a source linked to the hurry, then requests `b.cancel`, sleeps `grace` and requests
`b.abandon`; a `finish` source ends both sleeps once the drains return. It then runs every
`stop()` (each throw caught, the first kept), `drain_all`, joins the timer through an `event`,
and runs every `flush()` on a thread of its own (`off_loop`), which resumes the driver through
`post_call`; `inline_scheduler` takes a `post_call` from another thread into an inbox that each
step moves behind the ready handles, before any demoted one, and `queued()` counts it. When the
thread cannot start (`std::system_error`), the flushes run inline on the loop, so every path still
ends with them, and `stopped.flushed_inline` holds the error; a `flush()` must therefore never
wait on the loop, or that path deadlocks. The flushes are also registered with the stop source
for the third signal, and removed when the driver returns. `lifecycle` holds the loop's work
guard when the loop has one (`asio_scheduler::work_guard()`), so `io.run()` returns only after the
lifecycle.

`stopped.failure` ranks a start or `ready` error first, then the `fail()` that caused the stop
(`stop_source` records whether its first `fail()` came before any other stop request, read by
`cause()` as the teardown begins), then the first `stop()`, drain or flush error, then a later
`fail()`. `stopped.abandoned` is the `abandon` token's state once the timer ended: the timer
requests it only while the drains still run, so it flags a drain abandoned at its deadline even
when it left no lines.
