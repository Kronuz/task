# Source layout

Where the code lives, for the files that were split from one large file. `docs/SPLITTING.md` in
cpp-libraries is the rule: a piece is a unit someone opens alone, and a file over 1,000 lines
stays whole only when it is one coherent unit. No hand-written file here is over 1,000 lines.

## task.hh and task/

`task.hh` is an umbrella: it keeps its path and includes the pieces in file order, so no user
changes an include. File order is dependency order, so each piece includes the one before it and
compiles alone; the standard headers and `wheel.hh` come in through `task/scheduler.hh`.

| Piece | Holds |
|---|---|
| `task/scheduler.hh` | the cycle counter, the `scheduler` concept, `scheduler_ref` and its vtable, symmetric transfer and the trampoline, `running_on` |
| `task/drivers.hh` | the schedulers a caller drives: `inline_scheduler` (manual clock) and `blocking_scheduler` (one driving thread), with their timer wheels |
| `task/frame_pool.hh` | the per-thread frame pool, `pooled_frame`, the frame counters, the AddressSanitizer poisoning macros and the test hooks |
| `task/coroutine.hh` | `task<T>` and its promise, `detached`, `root_list`, `spawn`, `run`, `sync_wait` |
| `task/cancel.hh` | `yield`, `schedule_on`, `cancelled`, `cancel_state`, `cancel_token`, `cancel_source`, `cancel_callback` |
| `task/timing.hh` | `deadline`, the drivers' `start_timer`, `sleep_until` and `sleep_for`, `budget` |
| `task/sync.hh` | `event`, `async_mutex` (with `lock(token)`), `when_all` |

## Tests

Each split test stays one executable (the same CMake target, the same `ctest` test); a support
header holds what its pieces share, and a test function used by another piece has a prototype
there.

| File | Holds |
|---|---|
| `test/asio_test.cc` | `main`, the `operator new` counter, the fork-based assert tests, `test_sizes` |
| `test/asio_test_support.hh` | the includes, `CHECK`, `sock`, `destroy_mark`, `loop`, prototypes |
| `test/asio_test_primitives.cc` | tasks on an `asio_scheduler`: budgets, slices, locks, deadlines |
| `test/asio_test_token.cc` | `await_asio` with a token, and with a move-only completion |
| `test/asio_test_inline.cc` | the inline resume of a completion, and where it stays top-level |
| `test/asio_test_teardown.cc` | the `scheduler_ref` wrappers (the Host shape) and the teardown phases |
| `test/sync_test.cc` | `main`, the per-resume slice, `event`, event teardown, the destructor asserts |
| `test/sync_test_support.hh` | the includes, `CHECK`, `thread_scheduler`, `manual_scheduler`, prototypes |
| `test/sync_test_mutex.cc` | `async_mutex`, `deadline`, `lock(token)` races, a frame destroyed while it waits |

## Kept whole over 1,000 lines

None.
