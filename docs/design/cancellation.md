> Status: DRAFT (2026-11-02) designing (ready for preliminary review). The task half of W2a
> revision 2; the reactor and http halves are http `docs/design/task-handlers.md` (revision 2,
> branch `design/task-handlers`).

# task: cancellation, deadlines, inline resume and teardown

Reads task task:8cad661 (`task.hh`, `task_asio.hh`, `ARCHITECTURE.md`, `test/`, `bench/bench.cc`),
Asio 1.36.0, and the consumers at their heads: strata strata:c3ee63f, prospect prospect:465ee7d,
residency residency:38cf5a4, Xapiand Xapiand:81c311799, reactor reactor:ade618d, http http:679c0f8.
Line numbers are at these commits. Binding input: Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "task
cancellation, inline resume and reactor sessions (November 2)", items 1-6, and the architect's
review of http-on-task (`http/docs/reviews/task-handlers-design.md` at http:88f1275, TH-R2,
TH-R3, TH-R4) with its probes (`http-wt/task-handlers/.scratch/arch-review/`).

## Summary

`task::task<T>` is the family's one coroutine type, and asio stays the reactor. Three gaps keep it
from carrying a connection loop, all found by the architect's probes on task:8cad661:

| Gap | Probe result (rel, ASan+UBSan, TSan builds) | Decision |
| --- | --- | --- |
| A pending asio operation cannot be cancelled: `await_asio` binds no cancellation slot (`task_asio.hh:163-178`), and `cancel_token` is a polled flag with no callbacks (`task.hh:621-641`) | `cancel`: a socket read under a reactor-style cancel ends 489-491 ms after it, at the fallback close; asio's own coroutine ends in 0.0-0.1 ms | D1-D5 |
| Every socket completion costs an extra queue trip: the handler always resumes through `scheduler.post` (`task_asio.hh:177`, `task.hh:682-684`) | `posts`: 6.00 io_context handlers per round trip against asio's 4.00 | D6, D7 |
| Frames suspended at teardown leak: the handler holds a raw awaiter pointer and nothing owns the frame (`task_asio.hh:173-178`) | `teardown`: frames destroyed with the io_context, asio 1 of 1, task 0 of 1 | D8, D9 |

The result is one cancellation mechanism (a token with a reason and `std::stop_callback`-style
callbacks), deadlines as token sources, `await_asio` that cancels its operation and resumes inline
on its own loop, and an owner rule that destroys what is left after `stop()`. `as_awaitable`, the
last bridge to `asio::awaitable`, goes (D10). Consumers that only poll tokens or name
`scheduler_ref` build unchanged (D11).

## What this depends on and changes

| Document | Relation |
| --- | --- |
| Detent `SHARED_LIBRARY_DECISIONS.md`, November 2, items 1-4 and 6 | Implements them; item 5 (reactor) and item 7 (gate) are in http `docs/design/task-handlers.md` |
| `docs/mechanisms/async-mutex.md` | D4 proposes replacing `lock_until`/`lock_for` with `lock(token)` (Q1); the note is rewritten if Q1 lands |
| Xapiand `docs/design/phase7c.md` D2, D5 (`:234`) | D5 plans `lock_until` for the turn lease (Q1); D2's admission count reads `demoted()`, unchanged |
| `ARCHITECTURE.md`, `docs/API.md` (`:78` shows `as_awaitable`) | Rewritten in step C3 |

## Decisions

### D1. One cancel state: a pooled block, a reason, one load to poll

`cancel_source` and `cancel_token` keep their names and members (`task.hh:621-641`) and gain a
reason. The shared state becomes one intrusively counted block taken from the frame pool
(`task.hh:236-298`) instead of `make_shared<atomic<bool>>` (`:634`), so steady-state creation calls
no `operator new`, exactly as frames do.

```cpp
enum class cancel_reason : std::uint32_t { none = 0, requested = 1, deadline = 2, shutdown = 3, user = 64 };
class cancel_token {   // one pointer; empty = never requested
  bool requested() const noexcept;          // one relaxed load of the state word
  cancel_reason reason() const noexcept;    // none until requested
};
class cancel_source {  // copies share the state, as today
  cancel_source();
  explicit cancel_source(const cancel_token& parent);                         // D3
  cancel_token token() const noexcept;
  bool request(cancel_reason r = cancel_reason::requested) noexcept;          // first wins; true if this call did
  bool requested() const noexcept;
  cancel_reason reason() const noexcept;
};
```

The state word holds the requested bit, a lock bit and the reason, so `requested()` and `reason()`
read one atomic and never disagree. The reason set is open: task names the four cases every
host has, and a library defines its own from `user` (http's disconnect, in its design). Estimated
size, not measured: 48 bytes for a source without a parent (one 64-byte pool class), at most 128
with one (D3). `budget` (`task.hh:648-675`) is unchanged and its checkpoint stays one load (`:661`).

### D2. Callbacks with `std::stop_callback` lifetime

```cpp
template <class F> class cancel_callback {   // non-copyable, non-movable; lives in the waiting frame
  cancel_callback(const cancel_token& t, F f);   // f(cancel_reason) runs now if already requested
  ~cancel_callback();                            // deregisters; waits for f running on another thread
};
```

The node is the object itself, so registering allocates nothing; an empty token makes it inert.
`request()` sets the bit, then repeatedly takes the lock, unlinks one node, records it as running
with the calling thread, releases the lock and invokes it: no user code runs under the lock.
Deregistration unlinks a waiting node; for a running node it waits on the node's done flag
(`atomic::wait`) unless the runner is the calling thread (a callback destroying its own
registration), which it marks so `request()` does not touch the node again. This is the libstdc++
`stop_token` protocol. A callback that throws is caught and dropped, and the remaining callbacks
still run: L3-B2's rule for http's `on_cancel` (`http_message.h:143`) moves into the library that
runs callbacks. Callbacks run on the thread that calls `request()`.

Registration is lazy: it happens where a waiter suspends (D4, D5), never when a token is created
or passed down, so a budget loop that only polls pays nothing new.

### D3. Linked sources

`cancel_source(parent)` registers one node, embedded in the child's state, on the parent; a
parent request requests the child with the parent's reason. The child holds a reference to the
parent's state and deregisters when its own state dies; a parent already requested yields a child
born requested. This is how a connection's shutdown token and a per-read deadline make one token
(http D3), with no second mechanism.

### D4. Deadlines are token sources

```cpp
template <class S> class deadline {   // S provides start_timer(time_point, F) and now(), the lock_until contract (task.hh:898-902)
  deadline(S& s, std::chrono::steady_clock::time_point at, const cancel_token& parent = {});
  cancel_token token() const noexcept;
  bool expired() const noexcept;         // reason() == cancel_reason::deadline
};
```

A deadline is a linked source (D3) plus a timer whose callback requests `deadline`; destroying it
cancels the timer (`asio_scheduler::start_timer`, `task_asio.hh:83-90`, destroys the callback
unrun). There is no `when_any`: a race is "arm a deadline, pass its token to the operation", the
shape `expire_read` already has (`http_asio.h:546-555`). Proposed in the same step, pending Q1:
`async_mutex::lock(cancel_token = {})` replaces `lock_until`/`lock_for` (`task.hh:898-951`); the
token's callback settles the `lock_waiter` as timed out (`:763-774`), so a deadline, a shutdown or
any source ends a lock wait through one path.

### D5. `await_asio` takes a token and cancels the pending operation

```cpp
template <class Sig, class Init>
[[nodiscard]] auto await_asio(Init&& init, cancel_token token = {});
```

1. An empty token takes today's path exactly: no slot, no node.
2. Otherwise the awaiter holds an `asio::cancellation_signal` and a `cancel_callback`.
   `await_suspend` initiates with `asio::bind_cancellation_slot(signal.slot(), handler)`, then
   registers the callback, so the slot holds the operation's handler before any emit (an emit
   into an empty slot is lost). A token already requested emits at once; the operation completes
   with `operation_aborted` through its normal handler.
3. The callback emits `cancellation_type::all` (reactor's choice, `reactor.h:300`) when it runs on
   the waiting scheduler's thread (D7's `runs_here()`). From another thread it posts a one-shot
   emit step to that scheduler (a pooled frame, created only on this path) and counts it; a
   completion that finds an emit pending stores its result and leaves the resume to the emit step,
   the park/wake handshake of `lock_waiter` (`task.hh:767-774`). The frame therefore never ends
   under a posted emit.
4. The completion handler destroys the registration (D2 waits for a callback running elsewhere),
   then resumes (D6).
5. Rule: a token-bound operation's I/O object runs on the waiting scheduler's loop, so completion
   and registration share a thread; a Debug assertion checks it in the handler. Every reactor and
   http use is loop-local. Q2 asks whether to lift the rule.

Asio takes the slot's handler memory from its per-thread recycling allocator
(`asio/impl/cancellation_signal.ipp:52-72` in 1.36.0), so steady state adds no `operator new`.

### D6. Inline resume inside the scheduler's scope

The completion handler resumes inline when all three hold: `sched_.runs_here()`, no other
scheduler is current (`!current() || current() == sched_`), and the thread's inline depth is
below 4. It runs `{ running_on scope(sched_); h_.resume(); }`; otherwise it posts as today.

- `running_on` stamps a fresh slice (`task.hh:164-168`), so the inline resume counts as a resume:
  a budget that spends its slice demotes at the next checkpoint (`:654`, `:661-662`), the
  property probe `nocurrent` shows a raw resume loses (0 yielding checkpoints against 19-20).
- Awaits after it record `current() == sched_` (`task.hh:444`, `:724`, `:882`), so continuations
  come home to the loop, not to whatever thread completes them.
- The depth bound (4, a constant) stops nested completions (an immediate completion, a dispatch)
  from growing the stack or starving the loop; past it the resume is posted.

Expected effect, to be measured (T3, T4): io_context handlers per socket round trip fall from 6.00
to asio's 4.00.

### D7. `scheduler_ref` becomes two pointers with optional hooks

`scheduler_ref` (`task.hh:123-149`) holds `{self, const vtable*}` with `post`, `post_demoted`,
`runs_here` and `roots` entries. `of(S&)` fills the optional ones by detection, as it does
`post_demoted` today (`:132-136`): no `runs_here` means never inline, no `roots` means no
teardown ownership. Its members and `==` keep their meaning; `runs_here()` is added. Size falls
from 24 to 16 bytes in every promise (`:366`) and waiter (`:719`, `:754`). `asio_scheduler`
resolves `runs_here` once at construction through `executor_.target<asio::io_context::executor_type>()`
and its `running_in_this_thread()` (the check `reactor::Reactor::on_loop` uses, `reactor.h:196`);
another executor type answers false and keeps posting.

### D8. Teardown: the scheduler's owner destroys what is left after `stop()`

1. **Roots.** `spawn` (`task.hh:499-503`) links its frame (`detail::detached`, `:471-480`) into the
   scheduler's root list when the scheduler has one, and unlinks at completion: one uncontended
   lock each, per spawn, since spawn may run on any thread. `asio_scheduler` has a list;
   `inline_scheduler` does not (Q3).
2. **Destruction.** `~asio_scheduler` destroys every live root. A root owns its awaited chain:
   each parent frame holds its child `task`, whose destructor destroys it (`task.hh:431-433`).
   It then drops the demoted handles, which belonged to those roots. Posted resumes and pending
   asio handlers still in the io_context hold raw pointers; `~io_context` destroys them unrun.
   This replaces the assertion at `task_asio.hh:54` and the test expecting it
   (`test/asio_test.cc:204-210`, `:379-380`).
3. **Preconditions.** The io_context is stopped, no thread runs it, and it outlives the scheduler
   (the owner declares the scheduler after it). Debug asserts "live roots imply a stopped
   context". Destroying an io_context without `stop()` is unsupported, as the decision says.
   An owner stops every scheduler a root may be suspended on (another loop, a WorkPool chore)
   before destroying any.
4. **Primitives.** A frame destroyed while suspended leaves each one consistent: `await_asio`
   deregisters its node and leaves a handler that never runs; an `event` waiter unlinks itself
   (teardown is single-threaded, so `~event`'s assertion, `task.hh:696`, holds); a `lock_waiter`
   settles as abandoned so `unlock` skips it (`:855-863`); `when_all` children die with their
   parent (D9).
5. **Check.** `task::detail::live_frames()` counts pooled frames when `TASK_COUNT_FRAMES` is on
   (default without `NDEBUG`); tests assert it returns to its baseline. Linux ASan runs LSan too.

Xapiand relies on this: destroying a frame releases its shard uses and turn leases.

### D9. `when_all` owns its children without a vector

The variadic `when_all` keeps its children in a `std::array` instead of a `std::vector`
(`task.hh:1068-1069`), so http's streamed join (http D6) allocates nothing, and both forms destroy
unfinished children with their frame (today a child's handle is not owned, `:996-1016`).

### D10. `as_awaitable` goes

`as_awaitable` (`task_asio.hh:116-133`) awaits a task from `asio::awaitable` code. No consumer
calls it: Xapiand's bridge is its own copy, `engine::Run::await` (`host_asio.h:131-145`), deleted
in the http wave. Its test (`test/asio_test.cc:382-398`) becomes task-native.

### D11. Compatibility and consumers

| API | Change | Consumers (grep at heads) | Action |
| --- | --- | --- | --- |
| `cancel_source`, `cancel_token`, `budget` | members kept; `reason`, linking, callbacks added | strata `work_pool_executor.hh`, `executor.hh`, `index.hh`, `merge.hh`, `rate_limiter.hh`; prospect `bisection.hh`, `reorder.hh`, `values_writer.hh`, tests; Xapiand `search_views.h:195`, `test_lua.cc`, `test_reactor_requests.cc` | rebuild; none required |
| `scheduler_ref`, `scheduler` concept, `spawn`, `running_on` | layout only (D7); concept unchanged | strata `executor.hh:98`; prospect `sliced_test.cc`; Xapiand `host.h`, `host_asio.h`, `pool.cc:69`, tests | rebuild |
| `work_pool_executor` (strata), WorkPool chores (Xapiand `pool.cc` `ChoreAwaiter`) | none: they use tokens and record `current()` | strata, prospect, Xapiand | rebuild; Xapiand cancels WorkGroups before loop teardown (D8.3) |
| `await_asio` | token parameter; inline resume | Xapiand `manager.cc:473`, `search_views.cc:2944` | the code after the await now runs inside the completion handler, same thread and scope; the Xapiand lead checks nothing relied on a queue trip |
| `asio_scheduler` destructor | destroys roots instead of asserting | reactor (new owner), Xapiand `AsioHost` (moves to the reactor's) | per the http wave |
| `as_awaitable`; `lock_until`/`lock_for` (Q1) | removed | none in code; Xapiand phase7c D5 plans `lock_until` | Q1 |
| residency | pins task, uses no symbol above | none | none |

The Xapiand lead reviews this diff once for strata, prospect, residency and Xapiand (decision
item, "Detent owns the task and reactor changes").

## Gate rows

Builds in the step's worktree, each a separate directory: `build-rel` (Apple clang `-O2`),
`build-asan` (`/usr/local/opt/llvm/bin/clang++ -fsanitize=address,undefined
-fno-sanitize-recover=undefined`, `-stdlib++-isystem /usr/local/opt/llvm/include/c++/v1`),
`build-tsan` (same compiler, `-fsanitize=thread`), and g++ Debug ASan (g++ takes the trampoline
path, `task.hh:314-320`). Every build: `quiet light task-build 20 "task <dir>" -- nice -n 19
cmake --build <dir> -j4`, then `ctest --test-dir <dir> --output-on-failure`. Delete build
directories when done.

| Row | Target | Procedure |
| --- | --- | --- |
| T1 Unit tests | each passes, and fails with its fix reverted (shown once in the ledger) | `test/test.cc`, `sync_test.cc`, `asio_test.cc` gain: (D1) reason first-wins across 8 racing threads; `requested()` and `reason()` agree; source construction in a loop of 10,000 adds 0 `operator new` (counting `operator new`, as `bench/bench.cc:37-46`). (D2) callback before and after request; deregistration during a callback on another thread waits (TSan); self-deregistration inside the callback; a throwing callback is dropped and the next still runs. (D3) parent request reaches a child; a child of a requested parent is born requested; child state freed after parent dies. (D4) a deadline requests `deadline` at its time; destroying it first never requests; linked deadline takes the parent's reason. (D5) a socket read with a token requested on the loop ends with `operation_aborted` within one `run_one`; requested from another thread, same result and the frame outlives the posted emit (TSan, 10,000 iterations); empty token binds no slot. (D6) a budget loop after an inline-resumed socket read demotes (C1 of the review); a continuation awaited after an inline resume runs on the loop's thread (TSan); a chain of 100 nested completions stays within depth 4. (D8) stop, destroy scheduler, destroy io_context with roots suspended in `await_asio`, an `event`, a lock wait, a demoted checkpoint and a `when_all`: `live_frames()` back to baseline, ASan clean |
| T2 Sanitizers | 0 reports | T1 and the existing suites in `build-asan`, `build-tsan` and g++ Debug ASan; the Linux CI ASan job (LSan) clean |
| T3 Architect's probes | `cancel`: read ends within 1 ms of the request; `teardown`: task 1 of 1 after `stop()`; `posts`: task 4.00 io handlers per round trip (asio 4.00), `operator new` ≤ asio's 9 per 10,000; `nocurrent`: an inline-resumed loop yields at as many checkpoints as one resumed through the scheduler (19-20 at task:8cad661) | copy `http-wt/task-handlers/.scratch/arch-review/` to this worktree's `.scratch/arch-review/` (the original read only); replace `src/task.hh` and `src/task_asio.hh` with the branch's; port `cancel` to `await_asio(init, source.token())` with the request posted to the loop, and `teardown` to `io->stop(); sched.reset(); io.reset();`; `quiet light probes 10 "arch probes" -- .scratch/arch-review/build.sh rel` (and `asan`, `tsan`); `bin/probe_loop-KIND MODE` for the four modes in each build; results in `.scratch/arch-review/results.txt` |
| T4 task bench | reported by the lead, never gating alone: no-op task and yield-to-resume within the A/A band of task:8cad661; new rows for an `await_asio` socket round trip, posted against inline, with io handlers and `operator new` per round trip | `bench/bench.cc` gains the round-trip rows (a socket pair on one io_context, 200,000 round trips, best of 5); the lead runs A (task:8cad661), A' (a copy) and B in one `quiet exclusive` hold, 7 rounds, order rotated, `cpp-libraries/tools/ab_compare.py` |
| T5 Idle waiter cost | bytes stated by a test, 0 `operator new` when the token is empty or never requested, 0 per await in steady state when used | a test prints and `static_assert`s caps: `sizeof(cancel_callback<…>)`, the `await_asio` awaiter with and without a token, the state block with and without a parent; estimated, to be replaced by the printed values: node 40 bytes, awaiter growth about 56 bytes, state 48/≤128 bytes; plus asio's slot handler block while an operation is pending (recycled). 10,000 idle reads with a token, then 10,000 completed ones: `operator new` count 0 after warm-up |
| T6 Consumers | build and pass at the branch | `FETCHCONTENT_SOURCE_DIR_TASK=<worktree>` for strata, prospect, residency (`ctest` under `quiet light`, 4 jobs); Xapiand by the Xapiand lead |
| T7 API | no bridge, no duplicate timeout | `git grep -n 'as_awaitable' -- '*.hh' test bench` empty; with Q1 (a), `git grep -n 'lock_until\|lock_for'` empty |

## Open questions for the owner

**Q1. `lock_until`/`lock_for` beside deadline tokens.** Options: (a) replace them with
`lock(cancel_token)` and `deadline` (D4); (b) keep both; (c) keep the names as one-line wrappers
over `lock(deadline.token())`. Recommendation: (a). Two timeout mechanisms for one thing is what
the one-shape rule removes, nothing calls them yet, and Xapiand's turn wait (phase7c D5) gains
shutdown cancellation for free; the Xapiand lead decides with the owner. Not blocking: step C1
leaves the mutex alone if Q1 is still open.

**Q2. Token-bound `await_asio` only on the waiting scheduler's loop.** Options: (a) the rule and a
Debug assertion (D5.5); (b) allow cross-executor operations with a handshake on every completion.
Recommendation: (a). Every planned use is loop-local, and (b) charges each completion for a case
nobody has.

**Q3. Root ownership on `inline_scheduler`.** Options: (a) only `asio_scheduler` destroys roots;
(b) `inline_scheduler` too. Recommendation: (a). strata's `inline_executor` already runs its queue
to completion in its destructor (`executor.hh:63`), and tests declare the objects their tasks
touch after the scheduler, so destroying roots there would reach dead objects.

## Steps

Each step is one developer on one branch (`phase/w2a-task-*` from task `origin/main`), with its
own ledger, under the order in http `docs/design/task-handlers.md`, "Steps".

1. **C1 Tokens (D1-D4).** State block, reason, callbacks, linked sources, `deadline`; the mutex
   change only if Q1 is (a). T1 (D1-D4), T2, T6.
2. **C2 Operations and teardown (D5, D7-D10).** `await_asio` with a token, `scheduler_ref` vtable,
   root ownership and the destroyed-frame rules, `when_all` array, `as_awaitable` removed, docs.
   T1 (D5, D8), T2, T3 (`cancel`, `teardown`), T5. Its last commit is http's B0 pin.
3. **C3 Inline resume (D6).** T1 (D6), T2, T3 (`posts`, `nocurrent`), T4 rows. Its last commit is
   B1's pin. Then the Xapiand lead's review of the task diff, and the lead lands C1-C3.

## Limits

1. A token-bound operation must belong to its scheduler's loop (Q2); a violation is caught only in
   Debug.
2. Teardown needs the owner's order (D8.3). A root suspended on a scheduler that still runs while
   its owner destroys it is a use after free, and nothing in task detects it.
3. `sync_wait` frames are not roots: a `sync_wait` blocked on a stopped loop never returns.
4. Callbacks run on the requesting thread; a slow callback delays every later one on that
   request, silently.
5. Nothing here is built or measured. Sizes in D1 and T5 are estimates from reading the code;
   the probe numbers are the architect's at task:8cad661.

## What to review

1. D2 and D5: is the stop_callback protocol plus the posted emit enough to keep a frame alive
   across a cross-thread request?
2. D8: are the preconditions the right contract, and is any primitive missing from D8.4?
3. Q1-Q3.
