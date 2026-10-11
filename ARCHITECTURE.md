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
of the cancel flag, until the deadline passes; then it posts the coroutine as
demoted and starts a new slice on resume.

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
