> Status: DRAFT (2026-11-02) designing (ready for re-check). Revision 2: folds in the architect's
> REJECT (http `docs/reviews/task-handlers-rev2-design.md` at http:1ae1390, CN-L1, CN-L2, C8-C11)
> and the Xapiand lead's notes (`docs/reviews/cancellation-xapiand.md`, XN1-XN3); revision 1 is
> task:6d361ca. The task half of W2a; the reactor and http halves are http
> `docs/design/task-handlers.md` (branch `design/task-handlers`).

# task: cancellation, deadlines, inline resume and teardown

Reads task task:8cad661 (`task.hh`, `task_asio.hh`, `ARCHITECTURE.md`, `test/`, `bench/bench.cc`;
`task.hh` and `task_asio.hh` are unchanged at this branch's head), Asio 1.36.0, and the consumers
at their heads: strata strata:c3ee63f, prospect prospect:465ee7d, residency residency:38cf5a4,
Xapiand Xapiand:81c311799, reactor reactor:ade618d, http http:679c0f8. Line numbers are at these
commits. Binding input: Detent `docs/SHARED_LIBRARY_DECISIONS.md`, "task cancellation, inline resume
and reactor sessions (November 2)", items 1-6; the architect's revision-2 review (above) with its
probes (`http-wt/task-handlers/.scratch/arch-review-rev2/`, `probe_rev2.cc`, `results.txt`) and the
revision-1 probes (`.scratch/arch-review/`).

## Summary

`task::task<T>` is the family's one coroutine type, and asio stays the reactor. Three gaps keep it
from carrying a connection loop, all found by the architect's probes on task:8cad661:

| Gap | Probe result (rel, ASan+UBSan, TSan builds) | Decision |
| --- | --- | --- |
| A pending asio operation cannot be cancelled: `await_asio` binds no cancellation slot (`task_asio.hh:163-178`), and `cancel_token` is a polled flag with no callbacks (`task.hh:621-641`) | `cancel`: a socket read under a reactor-style cancel ends 489-491 ms after it, at the fallback close; asio's own coroutine ends in 0.0-0.1 ms | D1-D5 |
| Every socket completion costs an extra queue trip: the handler always resumes through `scheduler.post` (`task_asio.hh:177`, `task.hh:682-684`) | `posts`: 6.00 io_context handlers per round trip against asio's 4.00 | D6, D7 |
| Frames suspended at teardown leak: the handler holds a raw awaiter pointer and nothing owns the frame (`task_asio.hh:173-178`) | `teardown`: frames destroyed with the io_context, asio 1 of 1, task 0 of 1 | D8, D9 |

The result is one cancellation mechanism (a token with a reason and `std::stop_callback`-style
callbacks), deadlines as token sources, `await_asio` that registers before it starts, never starts
a cancelled operation and resumes inline on its own loop, and a four-phase teardown across every
scheduler a frame can touch. `as_awaitable`, the last bridge to `asio::awaitable`, goes (D10).
Consumers that only poll tokens or name `scheduler_ref` build unchanged; Xapiand's `Host` gains two
forwarding members and migrates its one `lock_until` call (D11).

## Review response

| Finding | Ruling | Where it is closed |
| --- | --- | --- |
| CN-L1 / CN-R1 (blocking) | accepted as stated | D5.2-D5.4: register before initiating; a requested token never initiates; one resume count with the park/wake shape of `lock_waiter`, so a handler that runs inside initiation never resumes the frame; D6.3 depth bound restated; T1 (D5) cases |
| CN-L2 / CN-R2 (blocking) | accepted as stated | D8.1-D8.3: four phases across every scheduler, `close()` and `destroy_roots()`; `spawn` onto a closed scheduler destroys the task; T1 (D8) crossroot, holder in both orders, dying `event` setter, spawning destructor; ASan |
| CN-R3 (blocking) | accepted | D8.4: a granted, unresumed waiter unlocks in its awaiter's destructor; T1 (D8) `grant_destroyed` |
| XN1 (the Xapiand lead's finding 1) | accepted, as refined in CN-R2 | D8.1-D8.4 |
| CN-R4 / C8 | accepted | D4: the timer callback holds the deadline's state and checks a disarm bit `~deadline` sets; `start_timer`'s comment corrected (step C1); T1 (D4) |
| CN-R5 / C9 | accepted | D7: a wrapper forwards `roots`, `runs_here` and `post_call`; D11: Xapiand's `Host` declares them; T1 (D7) |
| CN-R6 / C10 | accepted | D8.5: `live_frames()` counts in `pooled_frame::operator new`/`delete`, both pool paths, waiters and cancel states |
| CN-R7.1 / C11 | accepted | D5.3: the cross-thread emit is a plain function posted through `post_call`, not a frame; T1 (D8) "frame destroyed with an emit pending" |
| CN-R7.2 / C11 | accepted | D5.5: a cancel that loses the race returns the operation's real result |
| CN-R7.3 / C11 | accepted | D2, D3: a linked child registers on its parent at construction (two lock pairs per linked deadline); T4 measures it |
| CN-R7.4 / C11 | accepted | D8.6: unrooted frames named |
| XN2 (finding 2) | accepted as a rule | D6.4: completion handlers are invoked with the object consistent and outside its critical section, and never resume the frame inside their own initiation (D5.4); hand-written operations checked; the streamed test is http T4 (j) (TH2-R5) |
| XN3 (finding 3) | accepted | Q1 text corrected: Xapiand `ShardEndpoint::take_turn` calls `turn.lock_until(*host, deadline)` (`src/database/pool.cc:350`) and migrates to `lock(token)` in the wave |
| TH2-L1 / TH2-R1 (reactor) | task's part | D8.7: the teardown API reactor codes against |
| Q1, Q2, Q3 | ruled (a) each | D4, D5.6, D8.2; "Rulings adopted" |

Nothing else changed: D1, D9 and D10 are as in revision 1.

## What this depends on and changes

| Document | Relation |
| --- | --- |
| Detent `SHARED_LIBRARY_DECISIONS.md`, November 2, items 1-4 and 6 | Implements them; item 4 ("the owner of a scheduler destroys the frames still queued after `stop()`") becomes D8's four phases across schedulers, which the owner contract needs once frames cross loops; item 5 (reactor) and item 7 (gate) are in http `docs/design/task-handlers.md` |
| http `docs/design/task-handlers.md` R2 | Applies D8's phases across `TcpServer`'s reactors and in `UdpServer` (TH2-L1), against D8.7 |
| `docs/mechanisms/async-mutex.md` | Rewritten in step C1: `lock(cancel_token)` replaces `lock_until`/`lock_for` (Q1 (a)); the destructor rule of D8.4 |
| Xapiand `docs/design/phase7c.md` D5 (`:234`) | Its turn wait moves from `lock_until` to `lock(deadline.token())` linked to the request token; D2's admission count reads `demoted()`, unchanged |
| `ARCHITECTURE.md`, `docs/API.md` (`:78` shows `as_awaitable`) | Rewritten in step C2 |

## Decisions

### D1. One cancel state: a pooled block, a reason, one load to poll

`cancel_source` and `cancel_token` keep their names and members (`task.hh:621-641`) and gain a
reason. The shared state becomes one intrusively counted block taken from the frame pool
(`task.hh:236-298`) instead of `make_shared<atomic<bool>>` (`:634`), so steady-state creation calls
no `operator new`, exactly as frames do.

```cpp
enum class cancel_reason : std::uint32_t { none = 0, requested = 1, deadline = 2, shutdown = 3, user = 64 };
class cancel_token {   // one pointer; empty = never requested
  bool requested() const noexcept;          // one acquire load of the state word
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

Ordering contract: `request()` is a release (acq_rel CAS) and `requested()` and `reason()` are acquire
loads, so a poller that sees `requested()` true sees every write the requester made before
`request()`; a callback gets the same ordering from the state's lock bit. On x86-64 the load is
unchanged; on AArch64 it is one `ldapr` (or `ldar`).

The node is the object itself, so registering allocates nothing; it holds a reference to the
state, so a source may die first; an empty token makes it inert. `request()` sets the bit, then
repeatedly takes the lock, unlinks one node, records it as running with the calling thread,
releases the lock and invokes it: no user code runs under the lock. Deregistration unlinks a
waiting node; for a running node it waits on the node's done flag (`atomic::wait`) unless the
runner is the calling thread (a callback destroying its own registration, directly or by resuming
a frame that ends), which it marks so `request()` does not touch the node again. This is the
libstdc++ `stop_token` protocol. A callback that throws is caught and dropped, and the remaining
callbacks still run: L3-B2's rule for http's `on_cancel` (`http_message.h:143`) moves into the
library that runs callbacks. Callbacks run on the thread that calls `request()`.

Registration is lazy where a waiter suspends (D4, D5): never when a token is created or passed
down, so a budget loop that only polls pays nothing new. A linked source (D3) is the one eager
registration: it registers on its parent when constructed and deregisters when its state dies,
one lock pair each (CN-R7.3).

### D3. Linked sources

`cancel_source(parent)` registers one node, embedded in the child's state, on the parent at
construction; a parent request requests the child with the parent's reason. The child holds a
reference to the parent's state and deregisters when its own state dies; a parent already
requested yields a child born requested. This is how a connection's shutdown token and a per-read
deadline make one token (http D3), with no second mechanism. Cost, stated rather than hidden: a
per-read deadline linked to the session token takes the session state's lock twice per read
(register, deregister); http's T6 and T7 measure it through the `httpt` kind (TH2-R2).

### D4. Deadlines are token sources; `lock(token)` replaces the timed locks

```cpp
template <class S> class deadline {   // S provides start_timer(time_point, F) and now()
  deadline(S& s, std::chrono::steady_clock::time_point at, const cancel_token& parent = {});
  ~deadline();                           // disarms, then destroys the timer
  cancel_token token() const noexcept;
  bool expired() const noexcept;         // reason() == cancel_reason::deadline
};
```

A deadline is a linked source (D3) plus a timer whose callback requests `deadline`. Destroying a
timer does not cancel an expiry asio has already queued: the handler still runs with success
(probe `late_fire`, 1 of 1 in all three builds), so `start_timer`'s comment (`task_asio.hh:81-82`,
"on_fire is then destroyed unrun") is wrong for that case and is corrected in step C1. The timer
callback therefore holds a reference to the deadline's state, and the state word carries a
`disarmed` bit: `~deadline` sets it (release) before destroying the timer, and the callback
requests only when it reads the bit clear (acquire), so a deadline destroyed first never requests,
whether or not its expiry was queued. `~deadline` may run on any thread: the disarm bit is atomic,
the timer callback owns a state reference, and asio's timer queue cancels under its own lock
(20,000 deadlines destroyed from a non-loop thread against a running loop: TSan clean). It holds
no scheduler reference and asserts nothing, so a scheduler that cannot tell its thread
needs no hook (code review CR1). Xapiand's
`AsioHost::Armed` uses the same disarm shape (`host_asio.h:68`, `:87`). There is no `when_any`: a
race is "arm a deadline, pass its token to the operation", the shape `expire_read` already has
(`http_asio.h:546-555`).

Q1 (a), ruled: `async_mutex::lock(cancel_token t = {})` replaces `lock_until`/`lock_for`
(`task.hh:898-951`).

1. `lock()` with no argument is today's `lock_awaiter` (`:872-893`) exactly. `lock(token)` is a
   separate overload returning `cancellable_lock_awaiter` (80 bytes against 16), so frames that
   never pass a token do not grow; an empty token behaves as `lock()` (its registration is inert).
2. A free lock is taken whatever the token (`await_ready` tries it, as `timed_awaiter` does,
   `:912`). A requested token that finds the lock held returns an empty guard without queueing.
3. Otherwise the awaiter enqueues (`:955-973`), then registers a `cancel_callback` whose body is
   `lock_timeout`'s (`:788-790`): settle as `timed_out`, then `wake()`. A request that lands
   between the two runs the callback inside the constructor; `wake()` before `park()` makes
   `await_suspend` continue in place (`:769-774`), the handshake the timed lock already uses.
4. `await_resume` destroys the registration (D2 waits for a callback running elsewhere), then
   reads the outcome: a guard if granted, an empty guard otherwise.

Xapiand's `ShardEndpoint::take_turn` is the one caller of the timed form
(`turn.lock_until(*host, deadline)`, `src/database/pool.cc:350`); it migrates to
`lock(deadline.token())` with the deadline linked to the request's token in the same wave, which
gives the turn wait shutdown cancellation. Step C1 removes `lock_until`/`lock_for` only in a wave
that carries that migration (T7).

### D5. `await_asio` takes a token, registers before it starts, and parks

```cpp
template <class Sig, class Init>
[[nodiscard]] auto await_asio(Init&& init, cancel_token token = {});
```

1. **Empty token.** Today's path exactly, with D5.4's handshake: no slot, no node.
2. **Requested before start.** With a token, `Sig` must begin with `asio::error_code`
   (`static_assert`). `await_ready` returns `token.requested()`; a requested token completes the
   await at once with `asio::error::operation_aborted` and value-initialized remaining arguments,
   and `init` never runs. An argument without a default constructor (`asio::ip::tcp::socket`,
   which `async_accept` completes with) comes back as a `std::optional<R>`, empty when the
   operation never started and engaged otherwise (asio completes a cancelled accept with a
   socket that is not open); any other argument keeps its type (code review CR4).
3. **Register, then start.** `await_suspend` records the handle and `current()`, constructs the
   `cancel_callback`, and checks the token again: if the request landed in between, it destroys
   the registration and returns false with `operation_aborted`, initiating nothing. Otherwise it
   sets `initiating`, calls `init(bind_cancellation_slot(signal.slot(), handler))`, clears
   `initiating`, and emits `cancellation_type::all` itself if the callback ran meanwhile on this
   thread (an emit before the slot holds the operation's handler is lost: a request on the loop
   inside `init`, before or after the operation starts, ends it aborted). The callback:
   - on the waiting scheduler's thread (`runs_here()`), outside initiation: emits `all`
     (reactor's choice, `reactor.h:300`) as its last action;
   - on the waiting scheduler's thread, inside initiation: records the request for
     `await_suspend` to emit;
   - on another thread: adds one to the resume count (D5.4) and posts a plain function through
     `post_call` (D7) that emits on the loop and then drops its count. It is not a frame: a stopped
     loop's `io_context` destroys it unrun, and a closed scheduler drops it (D8), so nothing leaks
     (CN-R7.1). With no current scheduler, or one without `post_call`, the cancel is lost (D7);
     Debug asserts when that happens (code review N1).

   If the request landed between registration and the check from another thread, its callback
   has already added a count and posted the emit function (NF1): the early-out therefore gives
   up the handler's share and parks like the normal path, so it suspends while that count is
   pending and the emit function's drop resumes the frame with `operation_aborted`. If `init`
   throws, `await_suspend` destroys the registration, stores the exception, gives up the
   handler's share and parks the same way; `await_resume` rethrows. No handler runs.
4. **One resume count.** The awaiter holds a count, 2 at start: `await_suspend`'s park and the
   handler; a posted emit adds 1. Whoever drops it to zero resumes the frame. The handler stores
   the result, destroys the registration (D2 waits for a callback running on another thread; after
   it returns, no callback starts, so the count can no longer grow), then drops its count; the
   emit function emits, then drops its. `await_suspend` drops its count last: at zero, it returns
   false and the coroutine continues in place; otherwise it suspends. So a handler that runs inside
   `init` (an `asio::dispatch` on the loop, a direct call) only stores and drops, and the frame
   never runs to its end inside its own `await_suspend` (CN-R1, probe `dispatch_inline`). A handler
   or emit function that reaches zero after the park resumes through D6. This is `lock_waiter`'s
   park/wake (`task.hh:767-774`) with a third party: the handler, the emit function and
   `await_suspend` run on the loop, only the increment comes from another thread.
5. **A cancel that loses the race.** An operation that completed before the emit returns its real
   result (probe `race`: success and 4 bytes; a second emit after the handler is a no-op, all
   builds). A caller that must stop checks `token.requested()` after the await; http D4's "a
   timed-out read ends with `operation_aborted`" holds only when the cancel wins.
6. **Loop-local only (Q2 (a), ruled).** A token-bound operation's I/O object runs on the waiting
   scheduler's loop, so the handler, `await_suspend` and an on-loop callback share a thread; a
   Debug assertion in the handler checks `runs_here()`. Every reactor, http and Xapiand use is
   loop-local (`signal_loop`, `manager.cc:473`; `read_chunk`, `search_views.cc:2944`).

Asio takes the slot's handler memory from its per-thread recycling allocator
(`asio/impl/cancellation_signal.ipp:52-72` in 1.36.0), so steady state adds no `operator new`.

### D6. Inline resume inside the scheduler's scope

1. **When.** The handler or emit function that drops D5.4's count to zero after the park resumes
   inline when all three hold: `sched_.runs_here()`, no other scheduler is current
   (`!current() || current() == sched_`), and the thread's inline depth is below 4. It runs
   `{ running_on scope(sched_); h_.resume(); }`; otherwise it posts as today.
2. **A resume is a resume.** `running_on` stamps a fresh slice (`task.hh:164-168`), so the inline
   resume counts for the per-resume slice: a budget that spends its slice demotes at the next
   checkpoint (`:654`, `:661-662`), the property probe `nocurrent` shows a raw resume loses (0
   yielding checkpoints against 19-20). Awaits after it record `current() == sched_` (`task.hh:444`,
   `:724`, `:882`), so continuations come home to the loop.
3. **Depth.** Initiation no longer nests a resume (D5.4), so the bound (4, a constant) limits what
   remains: a handler invoked from inside another inline resume, as when a resumed coroutine
   dispatches or emits a cancel that completes synchronously. Past it the resume is posted.
4. **Reentrancy rule (XN2).** An operation awaited through `await_asio` invokes its completion
   handler with its object's state consistent and outside its own critical section, and the
   handler never resumes the awaiting frame inside its own initiation (D5.4 enforces the second
   half for every operation). Checked against every hand-written operation on the path: task's
   `start_timer` callback and `deadline` callback (run from the timer's handler, no lock held),
   `cancel_callback` (no lock held, D2), `async_mutex::unlock` (wakes after releasing its lock,
   `task.hh:850-870`), `event::set` (posts, `:712`); http's `ConnectionInput`
   (`arm`/`completed`, `try_send` on `done_`, `http_asio.h:549-561`) and `BodyStream`
   (`async_compose` over a channel, `http_body_stream.h:60-64`), both completed by posting (probe
   `channel`: a waiting receive never runs inside `async_send` or `try_send`, all builds). Xapiand's
   streamed back-to-back read test is http T4 (j) (TH2-R5), where `push` and `read` live.
5. **Where it lives (step C3).** `detail::asio_park::resume` (`task_asio.hh:203-214`), called
   from the `drop()` that reaches zero in the handler or the emit function; the depth is the
   thread-local `detail::inline_resume_depth` and the bound `detail::inline_resume_bound`
   (`:186-187`). The tests read the depth to tell an inline resume from a posted one. Under g++'s
   trampoline (`task.hh:448-461`), a child task's return inside an inline resume queues on the
   running trampoline instead of nesting, so the frame above an inline-resumed child continues
   after it returns, from the trampoline's loop; the rules are unchanged.

Expected effect, to be measured (T3, T4): io_context handlers per socket round trip fall from 6.00
to asio's 4.00.

### D7. `scheduler_ref` becomes two pointers with optional hooks, forwarded by wrappers

`scheduler_ref` (`task.hh:123-149`) holds `{self, const vtable*}` with `post`, `post_demoted`,
`post_call`, `runs_here` and `roots` entries. `of(S&)` fills the optional ones by detection, as it
does `post_demoted` today (`:132-136`): no `runs_here` means never inline, no `post_call` means no
cross-thread cancel of a token-bound operation (Q2), no `roots` means no root ownership. Its
members and `==` keep their meaning; `runs_here()` is added. Size falls from 24 to 16 bytes in
every promise (`:366`) and waiter (`:719`, `:754`).

```cpp
// optional members a scheduler, or a wrapper forwarding to one, may declare
bool runs_here() const noexcept;                       // D6, D5.3
void post_call(void (*fn)(void*), void* arg);          // D5.3's emit; dropped when closed
task::root_list* roots() noexcept;                     // D8; nullptr: no ownership
```

`asio_scheduler` resolves `runs_here` once at construction through
`executor_.target<asio::io_context::executor_type>()` and its `running_in_this_thread()` (the
check `reactor::Reactor::on_loop` uses, `reactor.h:196`); another executor type answers false and
keeps posting. **Wrappers forward (C9).** Detection sees the type `scheduler_ref::of(*this)` names.
Xapiand spawns through its `Host` (`host.h:90`, `:241`, `:246`) and its turn waiter records the
`Host`'s reference (`task.hh:914`, `:882` for `lock(token)`), so `Host` declares the three members
and `AsioHost` forwards them to the reactor's scheduler; its `post` drops while that scheduler is
closed (D8). A scheduler wrapper elsewhere does the same, or its tasks are not roots and its
waiters never resume inline.

### D8. Teardown: four phases across every scheduler

Frames, guards and I/O objects cross schedulers: a root on loop A can be suspended on loop B
(`schedule_on`, a lock or event wait there, a timer of B's), and a dying frame's RAII posts to
other schedulers (a guard's `unlock` through the waiter's scheduler, `task.hh:866-868`, `:770`; an
`event::set` through each waiter's, `:712`). Ordering one scheduler's teardown is not enough
(probes `crossroot` and `holder`: ASan heap-use-after-free in all ASan runs, `rel` SIGSEGV and
`bad_executor`, TSan silent). The contract is the owner's, across every scheduler and pool a root
may be suspended on:

1. **Roots.** `spawn` (`task.hh:499-503`) links its frame (`detail::detached`, `:471-480`) into the
   scheduler's `root_list` when `roots()` is non-null, and unlinks when it completes: one
   uncontended lock each, per spawn, since spawn may run on any thread. `asio_scheduler` has a
   list.
2. **Phase 1: stop.** Stop and join every loop and pool any root may be suspended on: the
   `io_context`s stop and their threads join (reactor's `release_loop`), WorkPool chores are
   cancelled and drained (Xapiand's `WorkGroup`s, `pool.cc` `ChoreAwaiter`). After it no thread
   runs a handler or a resume. `inline_scheduler` has no roots (Q3 (a), ruled): strata's
   `inline_executor` drains in its destructor (`executor.hh:63`).
3. **Phase 2: close every scheduler.** `close()` makes `post`, `post_demoted` and `post_call` drop
   what they are given (a dropped handle's frame belongs to a root destroyed in phase 3), and
   `spawn` onto a closed scheduler destroys the task at once, unrun, without calling `done`,
   instead of linking a new root while roots are destroyed.
4. **Phase 3: destroy every root, with every `io_context` alive.** `destroy_roots()` pops one root
   at a time under the list's lock and destroys it outside the lock; a root owns its awaited chain
   (each parent frame holds its child `task`, whose destructor destroys it, `task.hh:431-433`).
   Then it drops the demoted handles (they belonged to roots), which replaces the assertion at
   `task_asio.hh:54`. Each primitive stays consistent when a frame suspended on it dies:
   - `await_asio`: the registration is destroyed; the handler never runs; a posted emit function is
     dropped (closed) or destroyed unrun in phase 4; the I/O object's pending operation goes to its
     stopped `io_context` and is destroyed unrun there (probe `composed`: ASan clean destroying
     signals, then sockets, then the `io_context`).
   - `lock(token)` / `lock()` waiter: waiting, it settles as `timed_out` and `unlock` sheds it
     (`:855-863`); **granted but not resumed, its awaiter's destructor unlocks** (CN-R3, probe
     `grant_destroyed`: today the mutex stays locked forever, all builds). The test is the
     destructor's: `w_` still set (so `await_resume` has not run) and the waiter's state is
     `granted` (CN-R3: a waiter that timed out never held the lock), then `m_.unlock()`, which
     hands the lock on through a post the closed scheduler drops, until a waiter that is not
     being destroyed, or nobody, gets it. The token-bound awaiter destroys its registration
     first, waiting for a callback running on another thread (C1 review X3). Precondition (C1
     review CR6): no frame is destroyed between `unlock()`'s grant and its `wake()`; the phases
     guarantee it, since phase 3 runs after every loop joined and every unlock in it runs on
     the owner's thread.
   - `event` waiter: a node `set()` has not taken unlinks itself; `set()` marks each node it
     takes before resuming it, so a waiter destroyed after a dropped wake leaves the event
     alone. `~event` marks any waiter left as orphaned instead of asserting (`:696`) and counts
     it in Debug builds (`detail::orphaned_waiters()`, NF2), the signal the assertion gave;
     such a waiter's frame either dies in this phase or shows in `live_frames()`. Unlinking is a
     teardown rule: it is valid only while no thread can call `set()`, which phase 3 guarantees
     and a frame destroyed while suspended outside teardown does not.
   - `deadline`: disarmed, then its timer destroyed with its `io_context` alive (D4).
   - `cancel_callback`: deregistered; the state is counted, so its source may be gone (D2).
   - `when_all` children die with their parent (D9).

   Ownership rule: a primitive awaited by frames of more than one root (an `async_mutex`, an
   `event`) is owned outside every root, by an object destroyed after phase 3; Xapiand's turn
   mutex is an index's, which outlives the http service. For a mutex the Debug assertion in
   `~async_mutex` catches a violation first and deterministically (NF3, `own_a_first`: a mutex
   destroyed held or with a granted waiter); a mutex with only queued waiters is safe in either
   order. In Release (`NDEBUG`) a violation is a use after free that only ASan reports, and
   task cannot enforce the rule there.
5. **Phase 4: destroy schedulers, then `io_context`s.** `~asio_scheduler` runs `close()` and
   `destroy_roots()` itself if the owner did not (an owner with one scheduler and no cross-loop
   frames). `destroy_roots()` asserts in Debug that live roots imply a stopped `io_context`, and
   `close()` and `destroy_roots()` that they do not run on a loop thread. Destroying an
   `io_context` without `stop()` is unsupported, as the decision says. **The `io_context` is
   never run again after phase 3** (`io.restart(); io.poll()` as a flush is not supported):
   resumes and emits queued before `close()` stay in its queue and refer to destroyed frames.
   The scheduler's posted functions check `closed()` before they touch a frame, so one that does
   run drops its work, and a stopped `io_context` destroys the rest unrun (code review N2).
6. **Counting and unrooted frames.** `task::detail::live_frames()` counts in
   `pooled_frame::operator new`/`delete` (`task.hh:300-303`), so it covers both allocation paths:
   pooled classes and frames over 2 KiB that `frame_pool` hands to `operator new` (`:255`; http's
   connection frame is one, `char tmp[64 * 1024]`, `http_asio.h:765`), plus `lock_waiter`s (`:750`)
   and D1's cancel states. On when `TASK_COUNT_FRAMES` is set (opt-in, off in every build type, so
   Debug pays no global read-modify-write per allocation; the test targets set it); tests
   assert it returns to its baseline; Linux ASan runs LSan too. Frames that are not roots, whose
   dropped post therefore leaks visibly: the `sync_wait` driver (`task.hh:548`, a `detached` that
   `sync_wait` resumes without `spawn`), tasks posted by handle rather than spawned (the probes'
   shape; their owner destroys them), `run()` on an `inline_scheduler`, and Xapiand's debug
   `Run::Resume`. A frame suspended with no scheduler (`resume_on` with an empty reference resumes
   inline, `:682-684`) must not be pending at phase 3: a `sync_wait` blocked on a stopped loop
   never returns (Limit 3).
7. **The API reactor codes against (TH2-L1).**

   ```cpp
   // task_asio.hh, on asio_scheduler (and forwarded by wrappers, D7)
   void close() noexcept;           // phase 2: post, post_demoted, post_call drop; spawn destroys the task
   void destroy_roots() noexcept;   // phase 3: destroys every root, drops demoted handles; io_context alive
   bool closed() const noexcept;
   ~asio_scheduler();               // phase 4: close() and destroy_roots() if not yet done; then the io_context
   ```

   `close()` and `destroy_roots()` are idempotent and are the owner thread's, called after phase
   1's join (a `Reactor`'s `scheduler_` destructor repeats them); `closed()` is a plain read
   after the join.

   reactor's `TcpServer::stop()` runs, after its join (`reactor.h:596-600` at reactor:ade618d):
   `close()` on every reactor's scheduler, then `destroy_roots()` on every one, then destroys the
   reactors; `UdpServer::stop()` the same for its one loop. For a process with several servers or
   pools, `TcpServer` exposes the phases as calls (stop loops, close, destroy roots), and the
   process runs each phase across all of them before the next, as http's "What Xapiand changes"
   item 7 states.

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
| `scheduler_ref`, `scheduler` concept, `spawn`, `running_on` | layout only (D7); concept unchanged; three optional members | strata `executor.hh:98`; prospect `sliced_test.cc`; Xapiand `host.h`, `host_asio.h`, `pool.cc:69`, tests | rebuild; Xapiand `Host` declares and `AsioHost` forwards `roots`, `runs_here`, `post_call` (D7) |
| `work_pool_executor` (strata), WorkPool chores (Xapiand `pool.cc` `ChoreAwaiter`) | none: they use tokens and record `current()` | strata, prospect, Xapiand | rebuild; Xapiand drains WorkGroups in phase 1 (D8.2) |
| `await_asio` | token parameter; park handshake; inline resume | Xapiand `manager.cc:473`, `search_views.cc:2944` | the code after the await may run inside the completion handler, same thread and scope; the Xapiand lead checked both (XN2: `signal_loop` relies on no queue trip; `read_chunk` is http T4 (j)) |
| `asio_scheduler` | `close`, `destroy_roots`, `closed`; destructor destroys roots instead of asserting | reactor (new owner, D8.7), Xapiand `AsioHost` (moves to the reactor's) | per the http wave |
| `event` | `~event` orphans waiters instead of asserting | none outside task tests | `test/sync_test.cc` death check changes |
| `as_awaitable`; `lock_until`/`lock_for` | removed (D10; Q1 (a)) | Xapiand `pool.cc:350` (`take_turn`) | Xapiand migrates to `lock(deadline.token())` in the same wave |
| residency | pins task, uses no symbol above | none | none |

The Xapiand lead reviews this diff once for strata, prospect, residency and Xapiand (decision
item, "Detent owns the task and reactor changes").

## Rulings adopted

- **Q1 (a):** `lock(cancel_token)` with `deadline` replaces `lock_until`/`lock_for` (D4). Premise
  corrected (XN3): Xapiand's `take_turn` calls `lock_until` (`pool.cc:350`) and migrates in the
  same wave.
- **Q2 (a):** token-bound `await_asio` only on the waiting scheduler's loop, with the Debug
  assertion (D5.6).
- **Q3 (a):** only `asio_scheduler` owns roots (D8.2).

No question is open for the owner in this revision.

## Gate rows

Builds in the step's worktree, each a separate directory: `build-rel` (Apple clang `-O2`),
`build-asan` (`/usr/local/opt/llvm/bin/clang++ -fsanitize=address,undefined
-fno-sanitize-recover=undefined`, `-stdlib++-isystem /usr/local/opt/llvm/include/c++/v1`),
`build-tsan` (same compiler, `-fsanitize=thread`), and g++ Debug ASan (g++ takes the trampoline
path, `task.hh:314-320`). Every build: `quiet light task-build 20 "task <dir>" -- nice -n 19
cmake --build <dir> -j4`, then `ctest --test-dir <dir> --output-on-failure`. Delete build
directories when done. Teardown tests run in the ASan builds: TSan does not see the use-after-free
shapes (`crossroot` and `holder` run clean under TSan at task:8cad661).

| Row | Target | Procedure |
| --- | --- | --- |
| T1 Unit tests | each passes, and fails with its fix reverted (each revert shown once in the ledger) | `test/test.cc`, `sync_test.cc`, `asio_test.cc` gain: **(D1)** reason first-wins across 8 racing threads; `requested()` and `reason()` agree; source construction in a loop of 10,000 adds 0 `operator new` (counting `operator new`, as `bench/bench.cc:37-46`). **(D2)** callback before and after request; deregistration during a callback on another thread waits (TSan); self-deregistration inside the callback; a throwing callback is dropped and the next still runs. **(D3)** parent request reaches a child; a child of a requested parent is born requested; child state freed after parent dies. **(D4)** a deadline requests `deadline` at its time; destroying it first never requests; destroying it after its expiry is queued (probe `late_fire`'s two-timer shape) leaves the token unrequested; a linked deadline takes the parent's reason; `lock(token)`: free lock taken with a requested token, held lock with a requested token returns empty without queueing, request while queued (same thread, other thread under TSan), request between enqueue and registration continues in place. **(D5)** a requested token never calls `init`; an `Init` that completes through `asio::dispatch` on the loop and one that calls the handler directly, each with and without a token, each ending its root in the continuation, ASan clean and the frame ends after `await_suspend` returns; a socket read with a token requested on the loop ends with `operation_aborted` within one `run_one`; requested from another thread, same result and the frame outlives the posted emit (TSan, 10,000 iterations); a lost race returns the real result; empty token binds no slot. **(D6)** a budget loop after an inline-resumed socket read demotes (C1 of the revision-1 review); a continuation after an inline resume runs on the loop's thread (TSan); a chain of 100 completions driven by dispatches from resumed coroutines stays within depth 4. **(D7)** a task spawned through a forwarding wrapper (a `Host`-shaped type declaring `roots`, `runs_here`, `post_call`) is a root destroyed at teardown and resumes inline. **(D8)** under ASan, `live_frames()` back to baseline in each: one loop with roots suspended in `await_asio`, an `event`, a lock wait, a demoted checkpoint and a `when_all`; `crossroot` (root on A suspended on B's timer); `holder` with holder on A and waiter on B, destroyed in both orders (A's roots first, B's first); `grant_destroyed` (grant, destroy the frame unrun, `try_lock()` succeeds); an `event` set by a dying frame with a waiter on another loop; a destructor that spawns during phase 3 (the task is destroyed, not linked); a frame destroyed with a cross-thread emit pending; a connection-sized frame over 2 KiB counted |
| T2 Sanitizers | 0 reports | T1 and the existing suites in `build-asan`, `build-tsan` and g++ Debug ASan; the Linux CI ASan job (LSan) clean |
| T3 Architect's probes | revision 1: `cancel` read ends within 1 ms of the request; `teardown` task 1 of 1 after the phases; `posts` task 4.00 io handlers per round trip (asio 4.00), `operator new` ≤ asio's 9 per 10,000; `nocurrent` an inline-resumed loop yields at as many checkpoints as one resumed through the scheduler (19-20 at task:8cad661). Revision 2, in all three builds: `late_fire` 0 requests after disarm; `grant_destroyed` mutex free afterwards; `crossroot`, `holder` exit 0 and ASan clean; `dispatch_inline` ASan clean; `channel`, `race`, `composed` unchanged | copy `http-wt/task-handlers/.scratch/arch-review/` and `.scratch/arch-review-rev2/` to this worktree's `.scratch/` (the originals read only); replace each `src/task.hh` and `src/task_asio.hh` with the branch's. Port revision 1 as before (`cancel` to `await_asio(init, source.token())` with the request posted to the loop; `teardown` to the four phases). Port revision 2: `late_fire` to a `deadline` destroyed by the first timer's handler; `crossroot` and `holder` to `spawn` on A, then `stop`, `close()` on both, `destroy_roots()` on both, then destroy (and `holder` again in the other order); `dispatch_inline` to the branch's `await_asio` with an `Init` calling `asio::dispatch(io, handler)`; `grant_destroyed` unchanged. `quiet light probes 10 "arch probes" -- .scratch/arch-review-rev2/build.sh rel` (and `asan`, `tsan`; same for `arch-review`), then `run.sh`; results in each `results.txt` |
| T4 task bench | reported by the lead, never gating alone: no-op task and yield-to-resume within the A/A band of task:8cad661; new rows for an `await_asio` socket round trip, posted against inline, with io handlers and `operator new` per round trip, and a linked deadline per read (D3's lock pairs) | `bench/bench.cc` gains the round-trip rows (a socket pair on one io_context, 200,000 round trips, best of 5); the lead runs A (task:8cad661), A' (a copy) and B in one `quiet exclusive` hold, 7 rounds, order rotated, `cpp-libraries/tools/ab_compare.py` |
| T5 Idle waiter cost | bytes stated by a test, 0 `operator new` when the token is empty or never requested, 0 per await in steady state when used | a test prints and `static_assert`s caps: `sizeof(cancel_callback<…>)`, the `await_asio` awaiter with and without a token, the state block with and without a parent, `scheduler_ref` (16); estimated, to be replaced by the printed values: node 40 bytes, awaiter growth about 64 bytes (the count and `initiating` add to revision 1's 56), state 48/≤128 bytes; plus asio's slot handler block while an operation is pending (recycled). 10,000 idle reads with a token, then 10,000 completed ones: `operator new` count 0 after warm-up |
| T6 Consumers | build and pass at the branch | `FETCHCONTENT_SOURCE_DIR_TASK=<worktree>` for strata, prospect, residency (`ctest` under `quiet light`, 4 jobs); Xapiand by the Xapiand lead on the C3 pin (ctest, `async_check --strict`, the docs E2E, `stall_load.py`, the Lua flood harness, and its shutdown test with requests suspended on a turn, a chore and a body read) |
| T7 API | no bridge, no duplicate timeout | `git grep -n 'as_awaitable' -- '*.hh' test bench` empty; `git grep -n 'lock_until\|lock_for'` empty in task, and in Xapiand at its wave commit |

## Steps

Each step is one developer on one branch (`phase/w2a-task-*` from task `origin/main`), with its
own ledger, under the order in http `docs/design/task-handlers.md`, "Steps". No open question
blocks any of them.

1. **C1 Tokens (D1-D4).** State block, reason, callbacks, linked sources, `deadline` with the
   disarm bit, `lock(token)`; `lock_until`/`lock_for` removed in the same wave as Xapiand's
   `take_turn` migration; `start_timer`'s comment corrected; `async-mutex.md` rewritten. T1
   (D1-D4), T2, T3 (`late_fire`), T6.
2. **C2 Operations and teardown (D5, D7-D10).** `await_asio` with a token, register-first and the
   resume count; `scheduler_ref` vtable with `post_call`, `runs_here`, `roots`; `root_list`,
   `close()`, `destroy_roots()`, the phase-3 primitive rules (the lock awaiter's destructor
   unlock, `event` orphaning), `live_frames()`; `when_all` array; `as_awaitable` removed; docs.
   T1 (D5, D7, D8), T2, T3 (`cancel`, `teardown`, `dispatch_inline` with the handler resuming only
   through the count, `crossroot`, `holder`, `grant_destroyed`), T5. Its last commit is http's B0
   pin; reactor's step R codes against its interface (D8.7) now.
3. **C3 Inline resume (D6).** T1 (D6), T2, T3 (`posts`, `nocurrent`), T4 rows. Its last commit is
   B1's pin. Then the Xapiand lead's review of the task diff, and the lead lands C1-C3.

## Limits

1. A token-bound operation must belong to its scheduler's loop (Q2); a violation is caught only in
   Debug.
2. Teardown needs the owner's four phases across every scheduler and pool (D8). A root suspended
   on a scheduler whose loop still runs during phase 3, or a primitive owned by one root and
   awaited from another, is a use after free that nothing in task detects outside ASan.
3. `sync_wait` frames are not roots: a `sync_wait` blocked on a stopped loop never returns.
   `spawn` onto a closed scheduler destroys the task and its `done` unrun and never calls
   `done`: a blocking run whose `done` sets a flag it waits on hangs; one whose `done` owns a
   `std::promise` returns at once with `broken_promise` (T1 (D8) tests it).
4. Callbacks run on the requesting thread; a slow callback delays every later one on that
   request, silently.
5. A cancel that loses the race returns the real result (D5.5); the token, not the error code, says
   whether the caller should stop.
6. Nothing here is built or measured. Sizes in D1 and T5 are estimates from reading the code;
   the probe numbers are the architect's at task:8cad661.

## What to review

The scoped re-check the review names: D5 (register-first, the resume count, the emit function),
D6 (the depth bound and the reentrancy rule), D8 (the four phases, the primitive rules, the
ownership rule and D8.7), against the probes in `.scratch/arch-review-rev2/`.
