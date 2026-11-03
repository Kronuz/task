> Status: CURRENT (2026-11-03) how `cancel_source`, `cancel_token`, `cancel_callback`, `deadline` and `budget::checkpoint` work, read from `task.hh` at task:d080786.

# Cancellation

A `cancel_source` requests; any number of `cancel_token`s observe, and any number of
`cancel_callback`s run once when the request lands. Nothing is cancelled for the code: a
primitive that takes a token (`lock(token)`, `await_asio(init, token)`, `budget`) polls it or
registers a callback and decides what ending early means. `task::cancelled` is the exception a
budget checkpoint throws.

## State

```
cancel_state  (pooled, refcounted; task.hh:835)
  word_     requested | locked (spin) | disarmed | linked bits, reason in the high 32 bits
  head_     intrusive list of cancel_node registrations
  running_, requester_, epoch_   the callback being run, its thread, a counter bumped after each
  parent_   a linked state's parent, while registered on it
```

`cancel_source` and `cancel_token` are counted handles on one state. An empty token (the
default) has no state: it is never requested, has reason `none`, and registers nothing.

## Request

- `request(reason)` is first-wins: one compare-exchange sets `requested`, the reason and the
  lock bit together; later calls return false. `none` is stored as `requested`
  (`task.hh:852`, `:1055`).
- `requested()` and `reason()` are one acquire load of the same word, so a token never reads
  requested with reason `none`. The request is the release: a thread that sees `requested()`
  sees every write made before `request()`.
- Reasons are `requested`, `deadline`, `shutdown`; a library defines its own from `user` (64).
- The requester pops callbacks one at a time and runs each with the lock bit clear, on the
  requesting thread, holding a reference of its own so a callback may drop the last outside one.

## Callbacks

`cancel_callback cb(token, f)` runs `f()` or `f(reason)` once (`task.hh:1072`).

- Registered before the request: runs on the requester. After: runs in the constructor. Empty
  token or destroyed registration: never runs. Registering allocates nothing (the node lives in
  the callback object).
- The destructor unlinks. If the callback is running on another thread it waits for it to
  return (`remove` waits on `epoch_`); if it is running on this thread, the callback is removing
  itself and `remove` returns at once. So a callback may destroy its own registration and
  the source being requested.
- An exception from `f` is dropped; later callbacks still run.

## Linked sources

`cancel_source(parent_token)` registers a node on the parent whose callback requests the child
with the parent's reason (`task.hh:958`). A child's request does not reach the parent. A
child of an already-requested parent is born requested, without a link; one that loses the
race with a request registers, fails, and requests itself. The child holds its parent's state;
destroying the child unlinks it (waiting for a parent request running it on another thread).

## deadline

`deadline<S>(s, at, parent)` is a linked source plus a timer from `s.start_timer(at, fire)`
(`task.hh:1118`; `asio_scheduler::start_timer`, `task_asio.hh:130`). `fire` holds a reference
to the state and calls `request(deadline, forbid = disarmed_bit)`.

- `~deadline` sets the `disarmed` bit, then the timer dies. Destroying an Asio timer does
  not stop an expiry Asio already queued; that `fire` still runs, holds the state alive, and
  its request fails on the bit. So a deadline may be destroyed on any thread, before or after
  its expiry, and never requests afterwards.
- `expired()` is `reason == deadline`. A deadline linked to a parent that was requested first
  reports the parent's reason.
- A timer that cannot start throws from the constructor and frees the state.

## budget

`budget(slice, token)` pairs a time slice with a token (`task.hh:1153`).
`co_await b.checkpoint()` is ready (no suspension) when the token is requested, the slice is
not spent, or no scheduler is current. `await_resume` throws `cancelled` when the token is
requested, whether the checkpoint suspended or not; a spent slice on a scheduler requeues the
coroutine as demoted first. Cancellation is therefore seen at the next checkpoint, not inside
straight-line code.

```cpp
task::task<void> scan(task::budget& b) {
    for (auto& item : items) {
        process(item);
        co_await b.checkpoint();   // throws task::cancelled once the token is requested
    }
}
```

## Invariants

1. The first request sets the reason and no later one changes it.
2. `requested()` and `reason()` come from one word; a poller sees the requester's earlier
   writes.
3. A callback runs at most once, never after its registration's destructor returns, and never
   with the state's lock held.
4. A callback may destroy its registration and the last outside reference to the source.
5. A disarmed deadline requests nothing, whatever Asio has queued.
6. Steady-state sources, linked sources, registrations and requests call no `operator new`;
   an empty token allocates nothing (state blocks come from the frame pool).

## Tests

- `test/test.cc` `test_cancel_tokens`: first-wins and reason (1), eight racing requests (1, 2),
  a poller sees earlier writes under TSan (2), registered/after/destroyed callbacks, waiting for
  a callback on another thread, self-destroying callback, throwing callback (3, 4), parent and
  child links, born-requested children, parent/child lifetime under ASan, concurrent
  link and request under TSan, no `operator new` (6). `main` checks that cancellation is
  observed at the next checkpoint.
- `test/sync_test.cc` `test_deadline`: reason, destroyed first, destroyed after a queued expiry,
  linking, a timer that cannot start (5). `test_slice`: slices.
- `test/asio_test.cc` `test_primitives`: deadline on a real timer, destroyed on any thread
  before and after expiry while the loop runs, the late-fire handler.
