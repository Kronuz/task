> Status: CURRENT (2026-11-03) how `task::await_asio` awaits an Asio operation, with and without a `cancel_token`, read from `task_asio.hh` at task:d080786.

# await_asio

`await_asio<Sig>(init)` starts an Asio operation from a coroutine and resumes the coroutine
when the operation's handler runs. `await_asio<Sig>(init, token)` also ends the operation early
when the token is requested. Both overloads live in `task_asio.hh` and share one resume count.

```cpp
auto [ec, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
    [&](auto h) { socket.async_read_some(buffer, std::move(h)); }, token);
```

`init(handler)` starts the operation. The handler takes the arguments of `Sig`; the result is
`void`, the single argument, or a `std::tuple` of them (`asio_result`, `task_asio.hh:170`).

## The resume count

`asio_park` (`:192`) holds an atomic count that starts at 2: `await_suspend`'s `park()` holds one,
the handler holds the other. A cross-thread cancel adds a third. `drop()` resumes the frame
when it takes the count to zero; `park()` returns the `await_suspend` result, so the frame
continues in place, once, when `park()` drops last.

- A handler that runs inside `init` (a dispatch on the loop, a direct call) stores its result and
  drops its count; `park()` then drops last and `await_suspend` returns false. The frame is
  never resumed while it is still suspending.
- A handler that runs later drops last and resumes the frame; nothing touches the awaiter after
  that `drop()`.
- Every exit of `await_suspend` goes through `park()`, including the early-out and a throwing
  `init`.

## Cancellation (token overload)

The signature must begin with `asio::error_code` (a `static_assert` otherwise).

1. `await_ready`: a requested token returns true with `operation_aborted`; `init` is never called.
2. An empty token starts the operation with the handler unbound: no slot, no callback.
3. Otherwise the `cancel_callback` is registered first, the token is checked again, then `init`
   runs with the handler bound to a `cancellation_slot` (`:293`). A request seen at the re-check
   gives up the handler's count, resets the callback, and suspends on the remaining one.
4. A request while the operation is pending: on the loop's thread (`sched_.runs_here()`) it emits
   `cancellation_type::all` on the slot, or, during `init`, sets `pending_` and `await_suspend`
   emits once `init` returned. From another thread it adds one to the count and posts
   `emit_posted` through `post_call`; that function emits and drops the count (`:351-377`).
5. The handler resets the callback before dropping, which waits for a callback still running on
   another thread; none starts afterwards. `~awaiter` resets it too.
6. A throwing `init` resets the callback, stores the exception, gives up the handler's count if no
   handler ran, and `await_resume` rethrows it after a pending posted emit has run.

A cancel that loses the race to the operation returns the operation's real result; the caller
checks `token.requested()` to know whether to stop. The I/O object must run on the awaiting
scheduler's loop (asserted in Debug when the await began on it). A cross-thread cancel needs a
scheduler with `post_call`; without one it is dropped, and Debug asserts.

## Result arguments

`result_args<error_code, R...>` (`:229`) turns an `R` without a default constructor (a socket)
into `std::optional<R>` through `abort_arg_t` (`:222`). The aborted result for a request that
came before the start is built from value-initialized arguments, so those optionals are empty.
Both overloads use it, so one signature has one result type with or without a token. Signatures
not beginning with `error_code` are returned as written.

## Inline resume

`asio_park::resume` (`:204`) resumes the frame inside the handler when

```cpp
inline_resume_depth < inline_resume_bound && s.runs_here() && !current()
```

and otherwise posts it through `resume_on`.

- `runs_here() && !current()`: the handler runs directly under the loop, not inside another
  frame's resume, a scheduler's posted call, or a cancel request. A cancelled frame therefore
  never finishes inside its requester's `request()`, and two tasks completing each other's
  operations return to the loop each round.
- The bound is 4 (`inline_resume_bound`, `:186`); `inline_resume_depth` counts inline resumes
  nested on the thread. Past it, the resume is posted and the chain restarts at depth 0.
- The inline resume runs in a `running_on` scope: a fresh budget slice, `current()` is the
  waiting scheduler for later awaits, and a trampoline loop of its own (see `transfers.md`), so
  the chain of the resumed frame finishes inside the call.
- Code after the `co_await` may run inside the completion handler, on the same thread.

## Invariants

1. A frame is resumed exactly once per await, by whoever drops the count to zero.
2. A requested token never calls `init`.
3. The callback is registered before the operation starts and never runs after the handler
   returned.
4. An inline resume only happens at the top level, at depth below the bound.
5. The result type depends on the signature, not on the overload.

## Tests

`test/asio_test.cc`:
- `test_await_token`: requested before and between registration and re-check (2, 3); `init`
  throwing after a posted emit (6); requests inside `init`; a handler inside `init` with and
  without a token (1); empty token binds no slot; cross-thread requests racing completion under
  TSan and ASan (1, 3).
- `test_await_token_move_only`: an accepted socket as `std::optional`, empty when requested
  first (5).
- `test_inline_resume`: inline on the loop, on a loop thread, posted from another loop, a
  dispatch from inside a coroutine posts, the depth bound over 100 chained completions (4),
  a fresh slice.
- `test_inline_top_level`: cancel requested inside a task, cross-thread cancel, emit dropping
  last, lockstep tasks, a parent continuing in the scheduler's scope (4).
- `test_wrapper`: a wrapper forwarding `runs_here`/`post_call`/`roots`.
