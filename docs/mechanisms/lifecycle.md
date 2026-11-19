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

The loser does nothing, so the frame resumes once. The hop is how a cancel from another thread
unfiles an entry that only the loop may touch. `sleep_until(never, token)` arms nothing and waits
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
keeps a target and a force time per key under a mutex. The first touch spawns one watcher into the
group, which sleeps until the target, rereads it (touches only move it later) and fires once it
has passed, erasing the key first so a later touch starts a new fire. A stop cancels each sleep,
and the watcher erases its key without firing.

## Stop sources

`stop_source` holds two `cancel_source`s, stop and hurry (a hurry also stops), the first
`fail()`, and the flushes the third signal runs. A nested source links both to its parent's
tokens. `stop_signals` is a `stop_source` whose `sigaction` handler writes the signal number to a
non-blocking pipe; its thread reads it: the first stop signal requests stop, the second hurry, the
third runs every flush under the source's lock, restores `SIG_DFL` and raises the signal. Its
destructor restores the old handlers and writes a zero byte to end the thread.

## lifecycle

`components` erases each component to a row of function pointers, so the set iterates forward to
start and backward to stop, drain and flush. It counts the started ones; only those are stopped,
drained and flushed. A start that throws records the failing label and rethrows; a nested set
whose start threw still counts as started, so its started children unwind in its parent's order.
`drain_all` returns lines and the first failure together; a drain that leaves lines ends the
drains, since what it left may still use the earlier components.

The driver starts the set, calls `ready`, and sleeps on the stop token. At the stop it spawns a
bound timer: `sleep_until(now + bound - grace)` on a source linked to the hurry, then requests
`b.cancel`, sleeps `grace` and requests `b.abandon`; a `finish` source ends both sleeps once the
drains return. It then runs every `stop()`, `drain_all`, joins the timer through an `event`,
and runs every `flush()`. The flushes are also registered with the stop source for the third
signal, and removed when the driver returns.
