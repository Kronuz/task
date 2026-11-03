> Status: CURRENT (2026-11-03) how a coroutine continues into another without growing the stack, and why every `running_on` scope has its own trampoline, read from `task.hh` at task:d080786.

# Transfers and the per-scope trampoline

A task that completes synchronously resumes its awaiter from its final suspend point, and the
awaiter may await the next task at once. A million such steps must run in constant stack.
`detail::transfer(h)` (`task.hh:241`) is what an `await_suspend` returns to continue with `h`
without recursing. Its form depends on the compiler.

## Two forms

`TASK_SYMMETRIC_TRANSFER` (`:216-222`) defaults to 1 on Clang and 0 elsewhere; define it to
override.

- **Symmetric transfer (1).** `transfer(h)` returns `h`. Clang compiles the resume as a tail
  call at every optimization level. GCC does so only when optimizing without sanitizers, which is
  why it uses the other form.
- **Trampoline (0).** `transfer(h)` appends `h` to the thread's trampoline queue
  (`local_trampoline()`, `:234`). If a loop is already draining that queue (`active`), it
  returns `noop_coroutine()` and the loop resumes `h` after the current resume returns. If
  none is, the caller becomes the loop: it resumes queued handles until the queue is empty,
  then clears it and returns `noop_coroutine()`. A queue, not one slot, is needed because
  `when_all` resumes several children from one `await_suspend`.

Users of `transfer`: the awaiter of `task<T>` starts the awaited task (`:578`), the final awaiter
returns to the continuation when it is on the same scheduler or none was recorded (`:511`;
otherwise it posts the continuation to its scheduler), and the last `when_all` child resumes the
parent (`:1565`).

## Per-scope trampoline

A transfer made while a trampoline loop is active only queues; the chain it starts runs when that
loop gets control back. That is wrong when the code that made the transfer is a driver that
blocks its thread: a `sync_wait`, a nested run loop, an inline Asio resume. It resumes a chain
from inside another loop's resume, the chain's transfers queue behind the outer loop, and the
driver waits for work that only the outer loop, which it is blocking, can run.

`running_on` (`:282`) therefore carries a `fresh_trampoline` member (`:259`). On entry, if the
thread's trampoline is active, `fresh_trampoline` swaps the thread's trampoline with an empty one
saved in the scope, so transfers in the scope run on a loop of the scope's own; on exit it swaps
the enclosing trampoline back. If none is active, nothing is swapped and the scope's first
transfer becomes the loop. With symmetric transfer the member is empty
(`[[no_unique_address]]`) and costs nothing.

Every place that resumes a frame in a scope gets this without asking: `sync_wait` (`:751`),
`inline_scheduler::run_until`, `asio_scheduler::post`/`post_call`/`drain`, the inline Asio resume
(`await-asio.md`), and a custom scheduler that wraps its resumes in `running_on`.

```cpp
for (auto h : ready) {
    task::running_on scope(task::scheduler_ref::of(*this));  // own trampoline loop
    h.resume();                                              // the chain finishes in here
}
```

## Invariants

1. A chain started inside a `running_on` scope finishes inside that scope: its transfers do not
   wait on a loop outside it.
2. Leaving a scope restores the enclosing trampoline, its queue and `active` flag unchanged.
3. Chains of synchronous completions run in constant stack in both forms.
4. A scope that resumes frames must be a `running_on`; a bare `h.resume()` from inside a
   trampoline loop queues behind that loop.

## Tests

- `test/test.cc`, `main` (the block "A blocking driver nested inside another's resume"; helper
  `blocking_loop`, `nested_outer`, `nested_leaf` at `:570-592`): a blocking loop driven from a
  task another blocking loop runs; it reports stuck on g++ without the per-scope trampoline (1, 2).
- `test/test.cc` `main`: `many_awaits(1000000)` and `chain(100000)` in constant stack (3).
- `test/asio_test.cc` `test_inline_top_level`: a task resumed inline inside a coroutine with no
  scheduler continues in the scheduler's scope and keeps it across its next await, on Clang and
  g++ (1).
- `test/test.cc` `sync_inside` and the `sync_wait` helpers (`:149-215`): `sync_wait` called from
  a task, and the caller's current scheduler restored after it (the scope's other half).
