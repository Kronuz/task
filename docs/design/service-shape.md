> Status: DRAFT (2026-11-12), revised for review. Reads task:47d50f3 (`task.hh`, `task_asio.hh`),
> Asio 1.36.0, Xapiand:000224f5a, Detent:833c118c, prism:e0aea0e, journal:9c87929 and
> consensus:3d988d1. Line numbers are at those commits. Xapiand D132 (2026-11-19) renamed the
> driver: `serve` is `task::lifecycle`, the `service` concept is `component`, and the driver takes
> a stop source. The API, the sleeps, `group`, `periodic`, `debounce` and the stop sources are
> built (`task.hh`, `task_timers.hh`, `lifecycle.hh`); the migrations are not.

# One lifecycle and housekeeping shape

How Prism, Detent and Xapiand start, run periodic work, and stop, in one shape each migrates to
in its own quest (Detent D76), with one shutdown behavior (Detent D80, Xapiand D71).

## Summary

Each process's main thread runs one control loop. `task::stop_signals` turns the first signal
into a stop token, the second into a hurry token, and the third into flush-then-die; it is one
stop source of several. A component has `start()`, `stop()`, `drain(bound)` and `flush()`;
`task::lifecycle` starts components in order,
then stops them all and drains them in reverse. Requests get one 35 s bound; durable work has
none. Every wait is a wheel entry armed through the scheduler (D79); no shutdown polls.

## Shutdown, the same in all three (D80)

1. The first signal closes listeners and refuses new keep-alive requests; requests in flight
   finish inside one 35 s bound.
2. Requests still in flight 5 s before the bound, or at the second signal, are cancelled and get
   5 s to end. Durable work still completes, with no bound.
3. The third signal kills only after every `flush()` returns, so acknowledged writes survive.

Durable work:

- Xapiand: the index commits (Stop 3), then the WAL writers and fsync threads (Stops 4 and 5,
  `manager.cc:1124-1185`), which are `flush()`: every WAL write is queued (`wal.cc:1111-1115`).
- Detent: a write is acknowledged only once its journal append is durable (journal README:20-21;
  consensus README:73), and what the authority did not finish is answered `outcome_unknown`
  (`service.cc:2720-2800`). Checkpoint generations and the agent's table publish atomically
  (synced, renamed, directory synced: `agent_store.cc:494-506`). So `flush()` is empty; drains
  let the in-flight append or publication finish, and a third signal cutting one loses nothing
  acknowledged.
- Prism: nothing (no `fsync` in `prism/src`).

## Today's shapes, mapped

| Today | Where | Becomes |
| :---- | :---- | :---- |
| Detent authority: a 1 ms `asio::steady_timer`; `begin_shutdown` ends the runtime, which answers held polls 503 and pending writes `outcome_unknown`, then retires on 1 ms | `detent_authority/service.cc:640-655,2669,2691-2801` | the runtime sleeps `sleep_until(at, stop)`; `stop()` refuses new commands and answers held polls 503, while admitted writes still apply; `drain(b)` awaits them on `b.cancel`, then ends the runtime and joins it with no token; retirement paces on `sleep_for(1ms)`, no token |
| detentd: a flag polled every 5 ms and 200 ms; `ready()` after the lone-quorum wait | `detentd/main.cc:183-223` | `lifecycle(loop, signals, {}, ready, authority, http)`; the authority's `start()` holds the lone-quorum wait and throws `failure()`; a later failure calls `signals.fail()`; a stop before `ready` when detached stays a failure exit |
| Detent agent: an `asio::signal_set`, a 20 ms drain poll, and `monitor` on 20 ms, which announces ready and makes a fence or preparation timeout the exit status | `detent_agent/managed_host.cc:28-124,186-227` | `start()` returns at `may_announce_ready`, throwing at the preparation timeout; `stop()` is `coordinator->close()`; `monitor` keeps its 20 ms status read for logs, no token, and calls `signals.fail(cause)` if the coordinator drains unasked; `drain(b)` joins, then runs the terminal checks |
| Detent relay: an `asio::signal_set`; a 1 ms `tick` until every coordinator drains | `detent_relay/host.cc:276-290,355-375` | the tick paces on `sleep_for(1ms)`, no token; its failure (`:371-372`) calls `signals.fail()` |
| Prism: `g_stop` polled every 100 ms, then a blocking `service.stop()` | `prism/src/main.cc:238-239,338-349` | a control loop and `lifecycle(loop, signals, {}, ready, http)` |
| Xapiand: `XapiandManager` | `manager.cc` | the "manager.cc" table below |

## API

`task.hh` gains the sleeps. Times are the scheduler's `now()` nanoseconds (D14):

```cpp
// On a scheduler with timers: true at `at`, false once `token` is requested first.
// at = task::never waits for the token alone.
[[nodiscard]] detail::sleep_awaiter sleep_until(std::uint64_t at, cancel_token token = {});
[[nodiscard]] detail::sleep_awaiter sleep_for(std::chrono::nanoseconds d, cancel_token token = {});
```

`task_timers.hh` holds Xapiand's `control_plane.h`, generalized, and needs only `task.hh`, so
housekeeping is usable without a lifecycle:

```cpp
class group {  // control::Tasks: tasks on one scheduler, counted until their frames end
public:
	explicit group(scheduler_ref s) noexcept;
	void spawn(task<void> t);  // any thread; an escaping exception goes to `failed`
	std::size_t running() const noexcept;
	[[nodiscard]] join_awaiter join(cancel_token token = {});  // true at running() == 0
	std::function<void(std::exception_ptr)> failed;
};

// tick() every `period` after the previous tick ends, until `stop`; a throw goes to
// on_throw, and the next tick runs. tick may return task<void>.
template <std::invocable F, std::invocable<std::exception_ptr> E>
task<void> periodic(std::chrono::nanoseconds period, cancel_token stop, F tick, E on_throw);

template <typename Key, typename Hash = std::hash<Key>>
class debounce {  // control::Debounce: fire(key) once quiet, never later than a random force
public:
	struct timing { std::chrono::nanoseconds quiet, busy, min_force, max_force; };
	debounce(group& g, timing t, cancel_token stop, std::function<void(const Key&)> fire);
	bool touch(const Key& key);  // any thread; false once stop is requested: run the call now
};
```

`lifecycle.hh` holds the lifecycle and the stop sources (POSIX; it includes only `task.hh`):

```cpp
struct stop_bound {
	cancel_token cancel;   // at bound - grace, or the hurry: cancel requests in flight
	cancel_token abandon;  // grace after cancel: stop waiting for cancelled work
};
struct stop_timing { std::chrono::nanoseconds bound = 35s, grace = 5s; };

template <typename C>
concept component = requires(C& c, cancel_token stop, const stop_bound& b) {
	{ c.start(stop) } -> std::same_as<task<void>>;  // on the loop; returns once serving, or at stop
	c.stop();   // any thread, at once, idempotent: refuse new outside work; a throw is reported
	{ c.drain(b) } -> std::same_as<task<std::vector<std::string>>>;  // on the loop; what it left
	c.flush();  // any thread, blocking, idempotent: acknowledged writes are durable on return
};  // an optional name() labels a failing start

class components {  // a set of components in start order, itself a component: lifecycles nest
public:
	template <component... C> explicit components(C&... cs);
	const std::string& failed() const noexcept;  // the failing start: name() or position, "1/0" nested
	task<drained> drain_all(const stop_bound& b);  // lines and the first failure, both kept
	// start, stop, drain, flush as the concept
};

class stop_source {  // tests, admin QUIT, a parent component, a tool's end
public:
	stop_source();
	explicit stop_source(const cancel_token& parent, const cancel_token& parent_hurry = {});
	cancel_token stop() const noexcept;   // request_stop(), fail() or the parent
	cancel_token hurry() const noexcept;  // request_hurry() or the parent's hurry
	void request_stop() noexcept;              // any thread
	void request_hurry() noexcept;             // any thread; stops too
	void fail(std::exception_ptr e) noexcept;  // any thread: keeps the first, then stops
	flush_registration add_flush(std::function<void()> f);  // removed when the registration dies
	void run_flushes() noexcept;               // every added flush, last first
};

class stop_signals : public stop_source {  // one per process: the signals as a stop source
public:
	explicit stop_signals(std::initializer_list<int> stops = {SIGINT, SIGTERM},
	                      std::initializer_list<int> dumps = {}, std::function<void(int)> dump = {});
	~stop_signals();  // restores the previous handlers, joins the signal thread
	int first_signal() const noexcept;  // 0 when the stop was asked for
};

struct stopped {
	std::exception_ptr failure;           // the first start, ready, stop, drain or flush error, else fail()'s
	std::string failed;                   // the component whose start threw
	std::vector<std::string> unfinished;  // what drains left at `abandon`, in drain order
	bool ready = false;                   // every start returned and ready() ran
};
template <scheduler L, component... C>
task<stopped> lifecycle(L& loop, stop_source& stop, stop_timing t,
                        std::function<void()> ready, C&... components);
```

Every `main` reads the same:

```cpp
task::stop_signals signals;
asio::io_context io(1);
task::asio_scheduler loop(io.get_executor());
Registry registry(...); Autocommit autocommit(registry); Http http(registry, ...);
task::stopped result;
task::spawn(task::scheduler_ref::of(loop),
            task::lifecycle(loop, signals, {}, ready, registry, autocommit, http),
            [&](std::exception_ptr e, std::optional<task::stopped> r) {
	            result = r ? std::move(*r) : task::stopped{e};
	            io.stop();
            });
io.run();                                 // phase 1; the lifecycle holds the loop's work guard
if (!result.unfinished.empty()) {         // every flush() has returned
	for (auto& line : result.unfinished) { L_WARNING("{}", line); }
	Logging::finish(); Logging::join();
	std::_Exit(EX_TEMPFAIL);
}
loop.close(); loop.destroy_roots();       // phases 2 and 3; destructors are phase 4
return result.failure ? exit_status(result.failure) : 0;
```

A nested lifecycle runs inside a component: it holds its children as one `task::components
children(x, y)`, its `start(stop)` spawns `lifecycle(loop, inner, ..., children)` over a
`stop_source inner(stop)`, its drain awaits that lifecycle's end, and its `flush()` forwards to
`children.flush()`, so the third signal reaches them (Xapiand D140). The inner lifecycle sees the
stop through a hop, so the parent's earlier siblings stop before its children. A plain set of
components nests without one: `task::components inner(b, c)` is passed as one component.

## Stop and drain order

```text
signal 1                -> every stop(), in reverse; then each drain, in reverse
bound - grace, signal 2 -> b.cancel: requests in flight are cancelled
b.cancel + grace        -> b.abandon: a drain stops waiting and reports what is left
signal 3                -> the signal thread runs every flush(), then SIG_DFL and raise
```

1. `lifecycle` adds each component's `flush` to the stop source and awaits its `start(stop.stop())`, in
   order, so a first signal ends a start's wait (detentd's lone-quorum wait, the agent's
   preparation) as it does today. A start that throws, or one a stop ended, becomes `failure`
   or a stop, and the started ones stop and drain. Then it calls `ready`. A start or `ready`
   failure requests the stop token the components were given, so work a started component runs
   on it ends before its drain.
2. At `stop.stop()` it makes `b.cancel`, requested at `now + bound - grace` or at
   `stop.hurry()`, and `b.abandon`, `grace` after `b.cancel`, both on the loop's wheel.
3. It calls every `stop()` in reverse (a throw becomes `failure`, and the rest still stop), then
   each drain in reverse. In a drain no wait takes the
   stop token: requests take `b.cancel`, cancelled work `b.abandon`, and pacing sleeps and
   durable work none.
4. A drain's throw becomes `failure`. A drain that returns lines leaves threads that may use the
   later components, so those only `flush()`. `main` then exits `EX_TEMPFAIL`
   without teardown, as Xapiand does (`manager.cc:1188-1195`).
5. Every path ends with each started component's `flush()`, in reverse, on a thread of its own
   (D11) that `lifecycle` awaits before it returns, or inline on the loop when that thread cannot
   start, so a `flush()` must never wait on the loop: a
   normal stop, a start that throws, a drain that throws (later drains still run), and an
   unfinished drain. Xapiand's WAL join on a normal stop is that `flush()`.

Every `stop()` before any drain is what detentd needs: the authority's `stop()` answers held
polls 503 before HTTP's drain waits, as `begin_shutdown` precedes `service.stop()` today
(`detentd/main.cc:215-216`). HTTP's `stop()` only closes listeners; today's cancelling, joining
`service.stop()` is in HTTP's drain. Whatever touches admitted work, such as
`database_pool->finish()`, belongs in a drain.

## Timers

Each wait arms a `wheel::timer_entry` in its frame through `scheduler_ref::arm`, allocating
nothing. The Xapiand timers msg-30 found on `AsioHost::arm` move:

| Timer | Today: `AsioHost::arm`, one `asio::steady_timer` per arm (`host_asio.h:68-75`) | Moves to |
| :---- | :---- | :---- |
| `control::Periodic` | `control_plane.h:143` | `periodic`, `sleep_for(period, stop)` |
| `control::Debounce` | `control_plane.h:294` | `debounce`, `sleep_until(target, stop)` |
| `engine::SleepFor` | `engine/host.h:165-188` | `task::sleep_for` |
| Registry timers | `database/pool.cc:449-455,477-488` | an entry per callback, armed with `arm`; the registry's drain cancels them on the live control loop, and the `Timer` dies before the scheduler |
| `deadline<engine::Host>` | `Host::start_timer` (`host.h:109`) | `engine::Host` forwards `arm`, `cancel`, `now` as virtuals, like `post_call` (`host.h:88-90`); `BlockingScheduler` owns a wheel |

A closed `asio_scheduler`'s wheel never fires (`task_asio.hh:111-113,222`). Today Xapiand closes
the control scheduler (`manager.cc:1119`), then reruns the loop so the registry's timers fire
(`:1131`). Here the registry drains on the live loop, which closes only after `lifecycle` returns. A
sleep still armed at teardown is cancelled when phase 3 destroys its frame (D10).

## manager.cc

| Duty | Today | New owner |
| :---- | :---- | :---- |
| Ordered stop (Xapiand phase7c D16) | `join()` `:1033-1212`; one deadline `:1045`; `insisted()` `:1042` | `lifecycle(loop, signals, {}, ready, registry, autocommit, http)`. Stop 1, HTTP's drain: requests awaited on `b.cancel`, then a D11 thread runs `HttpAsioService::stop()`, `IndexChores::cancel_all`, `close()`, `destroy_roots()` (`:1090-1102`) and sets the event, awaited on `b.abandon`. Stop 2, autocommit's drain. Stop 3, the registry's, no token: `finish()`, its control tasks joined as its `group` (`pool.cc:670-700`), timers cancelled, indexes committed. Stops 4-5: its `flush()` |
| Autocommit debounce | `:301-308`; `schedule_autocommit` `:536` | `task::debounce<Endpoint>` on its own source, requested by its drain, so writes during HTTP's drain still debounce |
| Housekeeping | `control::Periodic` `:826`; `housekeeping_tick` `:506` | `periodic` owned by the schema cache, whose `on_throw` logs, as today (`control_plane.h:100-102`) |
| Node identity | `:340-458`; `init` `:762-804`; name checks `:812-821` | free functions of a node module that `main` calls before `lifecycle` |
| Server wiring | `make_servers` `:895-985`; `set_ready_impl` `:989` | the HTTP component's `start()`; `fsyncher()` (`:984`) moves to the registry's `start()` |
| Index settings resolution | `:1257-1275` | callers use the index resolver (`IndexResolverLRU`) directly |
| Metrics | `server_metrics_impl` `:1284`; `reactor_lag_metrics` `:1348`; labels `:883` | a function over the components' counters; labels set in `main` |
| Signals | self-pipe `:186-215`; "you insisted" `:646-662`; `sig_handler` `main.cc:232-262,296` | `stop_signals` (`SA_RESTART`; its thread logs each signal's name); the second signal replaces "you insisted"; a `cancel_callback` on `signals.stop()` points stdin at `/dev/null`; the third signal replaces `exit(EX_SOFTWARE)` once deinited. SIGUSR1 and SIGINFO keep a dump `signal_set` |
| Shutdown retries | `PeriodicTimer` `:184,708`; `shutdown_checks` `:739` | gone (D80); `PeriodicTimer` leaves Kronuz/reactor |
| Admin stop | `/_quit`, `/_flush?clients` (`search_views.cc:1534,1551`) | `QUIT` stays behind `--admin-commands` and calls `signals.request_stop()`, the SIGTERM path; `FLUSH?clients` closes idle connections through http's idle close (Detent D81); `FLUSH?databases` unchanged (owner, Xapiand D76) |
| A control task that fails | `control_task_failed` `:731` | `signals.fail()`; `main` keeps the exit code |

## Decisions

| # | Decision | Lost, and why |
| :---- | :---- | :---- |
| D1 | One timer: every wait arms a wheel entry through `scheduler_ref::arm`/`cancel` | `AsioHost::arm`, three allocations per arm (`host_asio.h:68-75`) |
| D2 | Cancellable waits return `bool`, never throw | Throwing `cancelled`, unlike `m.lock(token)` |
| D3 | A fire resumes through `resume_on`; a cross-thread cancel goes through `post_call` (`task_asio.hh:284-310`) | Resuming inside `expire()`, which delays later fires |
| D4 | `periodic` and `debounce` stop on a token, with no `stop()` | An event pointer under a mutex (`control_plane.h:110-117`) |
| D5 | `group::join` wakes at zero | `drain_control`'s 10 ms loop (`manager.cc:1007-1015`), the agent's 20 ms poll |
| D6 | A `sigaction` handler (`SA_RESTART`) counts and writes a byte to a pipe; a signal thread requests stop, then hurry, and at the third runs every flush, restores `SIG_DFL` and raises. Job 261112-101111-197ad6: three SIGTERMs 200 ms apart kill with the loop idle, wedged, or past `io.run()`, after a 1 s flush | `asio::signal_set`: `SIG_DFL` returns only when the loop runs the completion, so a wedged loop or a finished `io.run()` swallows signals 2 and 3 (job 261112-094453-310c13). `SIG_DFL` set in the handler kills before the flush. `sigwait` needs masks before every thread |
| D7 | One 35 s bound for requests, the 5 s grace inside it | Per-step bounds, `insisted()` (`manager.cc:1042`), and HTTP's grace leaving a reserve (`:165-169`) that Stop 3 needed only while it shared the deadline |
| D8 | Every `stop()` at the first signal, then drains in reverse | Each `stop()` just before its drain: detentd's held polls would hold HTTP's drain to the bound |
| D9 | Durable work has no bound; `flush()` runs on every path | Cutting the WAL at the bound or hurry, losing queued acknowledged writes (`tests/test_node.cc:934-963`) |
| D10 | Once its loops joined, a scheduler's `cancel`, `close()` and `destroy_roots()` run on any thread but its loop's (`task_asio.hh:56-62,148` change). Job 261112-094456-971fc0: ASan-clean with the frame's cancel, use-after-poison without | A refcounted state per sleep; phases 2 and 3 on the owner's thread, which keeps HTTP's pins through Stop 3 |
| D11 | A blocking step (`HttpAsioService::stop`, a flush) runs on its own thread, which sets an `event` | `shutdown_wait`'s 50 ms poll |
| D12 | A tick that throws goes to `on_throw`; the next tick runs | Ending on a throw: one `schemas->cleanup()` throw would end cleanup until restart |
| D13 | No wait inside a drain takes the stop token | Pacing loops would spin; the agent's terminal checks would never run |
| D14 | One time shape, `now()` nanoseconds; step 1 moves `deadline` to it (5 call sites outside task) | `time_point` for `deadline` alone; `scheduler_ref` has no clock type |
| D15 | The driver is `task::lifecycle` over `component`s; it takes a `stop_source`, of which `stop_signals` is one, and a set of components (`components`) is a component, so lifecycles nest (Xapiand D132) | `serve(loop, stop_signals&, ...)`: only the process's signals could stop a set, and a set could not nest |
| D16 | Flushes run off the loop: at the end of every path on a thread of their own (D11) that `lifecycle` awaits through `post_call`, and on the signal thread at the third signal; `flush()` is idempotent and thread-safe (Xapiand D140). When that thread cannot start (`std::system_error`), the flushes run inline on the loop, so a `flush()` must never wait on the loop | Flushes on the loop: the loop may be what is stuck, abandoned work could not move during the flush, a `flush()` that needs the loop would deadlock, and a nested lifecycle's flush would block its parent's loop |

## Limits

- A wheel fire is up to 2.2 ms late (`docs/mechanisms/deadlines.md`), so a 1 ms sleep takes 1 to
  2 ms. Detent's 1 ms loops measure throughput before and after (D1's measured escape).
- A drain that blocks the loop stalls the bound; the third signal still ends the process.
- A `flush()` that hangs on a dead disk stops the third signal; SIGKILL then loses the WAL queue.
- POSIX merges a standard signal that is still pending: three sent back to back killed in 2 of 5
  probe runs.
- A later `sigaction` for the signal replaces the handler (Xapiand's `main.cc:299-300`); each
  migration removes its own.
- A non-root frame still sleeping when its scheduler dies gets a `false` fire and never resumes.
- The third signal exits 130 or 143, without a log flush.

## Checks and first steps

- A1, A2, A4: `rg -n '^## (API|Stop and drain order|Timers|manager.cc)' docs/design/service-shape.md`
  finds four headings, and `campaign verify service-shape` runs the docs check.
- A3: one `design` review, plus the Xapiand lead's agreement, recorded in the journal.

Each step is its own quest with tests that fail without it (among them: `flush()` runs once on
each path of step 5, and a signal during `start()` ends it):

1. task: sleeps, D10, D14; under ASan, phase 3 destroying a sleeping root, one a
   root of another scheduler.
2. task: `group`, `periodic` (a throwing tick ticks again), `debounce`, and a drain loop on
   `sleep_for(1ms)` that keeps pacing after the stop, on `inline_scheduler`.
3. task: `stop_signals` and `lifecycle`: three SIGTERMs, with the loop blocked in a drain or in
   teardown, kill after the flushes.
4. Migrations: Prism, then Detent (an open poll stops well under the bound), then Xapiand after
   its Phase 7c gate (a second signal with queued WAL writes, and a restart reads them all back;
   a request suspended on a turn at stop, and the stop exits 0 in order).
