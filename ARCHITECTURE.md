# task architecture

## Tasks

`task<T>` suspends at creation. Its awaiter records the awaiting coroutine and the
current scheduler in the promise, then returns the task's handle, so the task starts
by symmetric transfer. At the final suspend point the task returns its continuation
when the awaiter's scheduler is the one running now (or none was recorded), which
keeps chains in constant stack. Otherwise it posts the continuation to the awaiter's
scheduler and returns `noop_coroutine()`. Values and exceptions are stored in the
promise and delivered by `await_resume`.

## Transfers

Constant stack for chains of synchronous completions needs every resume-by-return to
become a tail call. Clang guarantees it; GCC emits it only when optimizing without
sanitizers, and otherwise recurses until the stack overflows. `detail::transfer(h)`
hides the difference: with `TASK_SYMMETRIC_TRANSFER` (the default on Clang) it returns
`h`; without it, it appends `h` to a per-thread queue and either returns to the loop
already draining that queue or becomes that loop, resuming queued handles one after
another. A queue rather than a single slot is needed because `when_all` resumes
several children from one `await_suspend`.

## Schedulers

`scheduler_ref` erases a scheduler to its address plus two function pointers
(`post`, `post_demoted`). A thread-local `current` reference says which scheduler is
running; schedulers set it around every resume with `running_on`. Affinity checks,
`yield()`, and budget checkpoints all go through it, so code written against `task`
never names a concrete scheduler.

`inline_scheduler` keeps a ready queue and a demoted queue and runs demoted handles
only when nothing is ready: least-attained service, where a task that spent its
slice waits behind tasks that have not. `asio_scheduler` posts ready handles to its
executor and keeps demoted handles in a local queue drained one per executor pass.

## Budgets

`budget` converts its slice to cycle-counter ticks once (`rdtsc` on x86-64,
`cntvct_el0` on AArch64, calibrated against `steady_clock` over 2 ms per process).
`checkpoint()` is ready, and costs nothing beyond a counter read and a relaxed load
of the cancel flag, until the slice is spent; then it posts the coroutine as
demoted.

A slice is one resume. `running_on` stamps a thread-local slice start from the cycle
counter when it is entered and restores the enclosing stamp when it leaves, and
`exhausted()` compares the counter with that stamp plus the slice. Schedulers wrap each
resume in a scope, so a task resumed after a lock, an event or an Asio completion starts
fresh, and only a run longer than the slice is demoted. A budget used with no scheduler
current falls back to a deadline from its construction.

## Events and locks

`event` is an atomic stack of waiters whose nodes are the awaiters in the waiting frames. A
waiter pushes itself unless the stack holds the "set" marker; `set()` swaps the marker in,
reverses the stack for arrival order and resumes each waiter through the scheduler it
suspended on (inline when it had none). It reads a node's link before resuming it.

`async_mutex` keeps a locked flag and a FIFO queue under a short internal mutex. `unlock`
hands the lock to the first queued waiter without releasing it, so a late arrival cannot
barge in, and resumes that waiter through its own scheduler. A queued waiter is a
`lock_waiter`: a refcounted block shared by the queue, the awaiter and, for `lock_until`, a
timer callback. One compare-exchange on its state settles it as granted or timed out; the
loser finds it settled. A timed-out entry stays queued until a later `unlock` or `lock`
sheds it, so the timer never touches the mutex. `await_suspend` ends with a handshake
(`park` against `wake`) so a grant or a timeout that lands before it returns continues the
coroutine in place instead of resuming a frame that is still suspending. See
`docs/mechanisms/async-mutex.md`.

## Frames

Promise types inherit `operator new`/`operator delete` from `pooled_frame`, which
uses a thread-local array of free lists in 64-byte classes up to 2 KiB. Allocation
pops a list; deallocation pushes onto the freeing thread's list.

## when_all

Each input task runs inside a small child coroutine that stores its result (or the
first exception) and decrements a shared counter at its final suspend point; the
last child to finish resumes the parent by symmetric transfer. The counter starts at
`n + 1`, and the parent drops its own count after starting every child, so a child
that finishes during start-up never resumes the parent early.
