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

`scheduler_ref` erases a scheduler to its address plus a pointer to a per-type table
(`post`, `post_demoted`, and the optional `post_call`, `runs_here`, `roots`, filled by
detection). A thread-local `current` reference says which scheduler is
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
`checkpoint()` is ready, and costs nothing beyond a counter read and an acquire load
of the cancel flag, until the slice is spent; then it posts the coroutine as
demoted.

`requested()` is an acquire load and `request()` a release: a poller that sees the request also
sees the requester's earlier writes (a disconnect reason, a shutdown payload).

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
suspended on (inline when it had none). It reads a node's link and marks the node taken before
resuming it. At teardown a waiting frame's awaiter unlinks itself, and an event destroyed with
waiters marks them orphaned (counted in Debug), so a node taken, linked or orphaned is each a
plain write to a live frame.

`async_mutex` keeps a locked flag and a FIFO queue under a short internal mutex. `unlock`
hands the lock to the first queued waiter without releasing it, so a late arrival cannot
barge in, and resumes that waiter through its own scheduler. A queued waiter is a
`lock_waiter`: a refcounted block shared by the queue and the awaiter; `lock(token)` adds a
cancel callback, registered after the waiter is queued and destroyed before the awaiter drops
its reference. One compare-exchange on its state settles it as granted or timed out; the
loser finds it settled. A timed-out entry stays queued until a later `unlock` or `lock`
sheds it, so the callback never touches the mutex. `await_suspend` ends with a handshake
(`park` against `wake`) so a grant or a timeout that lands before it returns continues the
coroutine in place instead of resuming a frame that is still suspending. See
`docs/mechanisms/async-mutex.md`.

## Asio operations

`await_asio` keeps one resume count, 2 at the start: `await_suspend`'s park and the handler.
The handler stores the result and drops its count; `await_suspend` drops its own after `init`
returns, and whoever reaches zero resumes the frame (`await_suspend` by returning false, so a
handler that runs inside `init` never resumes a frame that is still suspending). With a token,
the cancel callback is registered before `init` runs and the token is checked again after; a
request on the loop emits `cancellation_type::all` on the operation's slot (or, during
initiation, leaves it for `await_suspend` to emit), and a request from another thread adds one
to the count and posts a plain function through `post_call` that emits and drops it. Every exit
of `await_suspend` (the early-out, a throwing `init`) suspends while that count is pending.

A handler or emit function that drops the count to zero resumes the frame inline when it runs
on the waiting scheduler's thread (`runs_here()`), no other scheduler is current, and fewer than
four inline resumes nest on the thread (`detail::inline_resume_depth`); otherwise it posts. The
inline resume runs in a `running_on` scope, so it stamps a fresh slice and later awaits record
the scheduler, as a posted resume would. Each such completion costs the loop one handler
instead of two. The bound only limits handlers invoked from inside another inline resume (a resumed
coroutine that dispatches or emits a cancel that completes at once); past it the next resume is
posted and its chain starts again at depth 0. Code after the await may therefore run inside the
completion handler, on the same thread and in the same scope. The token
overload returns an argument without a default constructor (a socket) as a
`std::optional`, empty when the token was requested before the operation started.

## Teardown

`spawn` links its frame into the scheduler's `root_list` when the scheduler has one, and the
frame unlinks itself when it ends. The owner's four phases: stop and join every loop; `close()`
every scheduler (posts drop, `spawn` destroys its task); `destroy_roots()` on every one, which
pops one root at a time and destroys it outside the list's lock; then destroy schedulers and
`io_context`s. A root owns its awaited chain, so destroying it destroys every frame it awaits.
Each primitive stays consistent: a lock waiter granted but not resumed unlocks in its awaiter's
destructor, a token's callback deregisters, a pending Asio operation goes to its stopped
`io_context`, `when_all` destroys unfinished children. No frame is destroyed while another thread
runs a handler or an `unlock` against it: the phases run after the join. The `io_context` is
not run again after `destroy_roots()`; the scheduler's posted functions check `closed()` before
touching a frame, so one that does run drops its work. A `deadline` holds no scheduler
reference and may be destroyed on any thread: its disarm bit is set before the timer dies.

## Frames

Promise types inherit `operator new`/`operator delete` from `pooled_frame`, which
uses a thread-local array of free lists in 64-byte classes up to 2 KiB. Allocation
pops a list; deallocation pushes onto the freeing thread's list. `TASK_FRAME_POOL=0` sends
every block to `operator new`; it and `TASK_COUNT_FRAMES` change inline functions, so every
translation unit of one program must agree (ODR).

## when_all

Each input task runs inside a small child coroutine that stores its result (or the
first exception) and decrements a shared counter at its final suspend point; the
last child to finish resumes the parent by symmetric transfer. The counter starts at
`n + 1`, and the parent drops its own count after starting every child, so a child
that finishes during start-up never resumes the parent early. The parent owns its children
(a `std::array` for the variadic form): a finished child destroys itself and clears its slot,
and destroying the parent destroys any child that has not finished.
