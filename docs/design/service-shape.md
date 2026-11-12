> Status: DRAFT (2026-11-12) ready for review. Reads task:47d50f3 (`task.hh`, `task_asio.hh`),
> wheel at the pin task links, Asio 1.36.0, Xapiand:000224f5a, Detent:d2325649, prism:e0aea0e.
> Line numbers are at those commits. Nothing here is built.

# One service and housekeeping shape

How Prism, Detent and Xapiand start, run periodic work, and stop, in one shape that each migrates
to in its own quest (Detent D76).

## Summary

Each process's main thread runs one control loop, an `asio_scheduler`.
`task::stop_signals` turns the first SIGINT or SIGTERM into a stop token and the second into a
hurry token. A service has three calls, `start()`, `stop()` and `drain(bound)`. `task::serve`
starts the services in order, waits for the stop, then stops and drains them in reverse under one
bound. That bound is a `task::deadline` linked to the hurry token, so a second signal makes it
arrive at once. Periodic work, debounces and every wait are tasks that `co_await task::sleep_*`.
A sleep is one wheel entry armed through the scheduler's owner-thread `arm`/`cancel` (D79).
Nothing polls: the 100 ms, 200 ms, 20 ms, 10 ms and 50 ms loops listed below are gone, and so are
`reactor::PeriodicTimer` and the Asio timer per use behind `AsioHost::arm`.

## Today's shapes, mapped

| Today | Where | Becomes |
| :---- | :---- | :---- |
| Detent authority: the runtime loop awaits a 1 ms `asio::steady_timer`; `begin_shutdown` requests `stop_` | `detent_authority/service.cc:1288,1742,1758,2669,2784`; `:640-646` | `sleep_until(at, stop_.token())` on the owner loop's wheel; `begin_shutdown` becomes `stop()`; the blocking `drain()` (a future wait, `:647-655`) becomes `drain(bound)`, which joins the runtime's `group` |
| detentd: a signal flag polled every 5 ms and 200 ms; teardown by hand | `detentd/main.cc:32-35,198-223` | `stop_signals`, `serve(signals, bound, authority, publisher)`, `teardown(...)`; the lone-quorum leader wait moves into the authority's `start()`, awaiting an `event` that the runtime sets |
| Detent agent: an `asio::signal_set`, a 20 ms drain poll, and a `monitor` loop on a 20 ms timer | `detent_agent/managed_host.cc:186-213`, `:28-124` | `stop_signals`; `coordinator->close()` becomes `stop()`; the poll becomes `group::join(bound)`; `monitor` becomes `periodic(20ms, stop, report)` |
| Detent relay: an `asio::signal_set` and a 1 ms `tick` | `detent_relay/host.cc:285-290,364-375` | `stop_signals`; the tick becomes a task looping on `sleep_for(1ms, stop)` |
| Prism: `g_stop` polled every 100 ms, then a blocking `service.stop()` | `prism/src/main.cc:238-239,338-349` | a control loop, `stop_signals`, and `serve(signals, bound, http)` |
| Xapiand: `XapiandManager` | `manager.cc` | the "manager.cc" table below |

## API

`task.hh` gains the primitives:

```cpp
// On a scheduler with timers: true at `at` (now() nanoseconds), false once `token` is requested
// first. Off a scheduler, or on one whose arm() returns false, throws std::logic_error.
[[nodiscard]] detail::sleep_awaiter sleep_until(std::uint64_t at, cancel_token token = {});
[[nodiscard]] detail::sleep_awaiter sleep_for(std::chrono::nanoseconds d, cancel_token token = {});
// event: true once set(), false when `token` is requested first.
[[nodiscard]] detail::event_wait_awaiter event::wait(cancel_token token);
```

`task_service.hh` (new, so `task.hh` does not include `<functional>`, `<random>` or
`<unordered_map>`) holds Xapiand's `control_plane.h`, generalized:

```cpp
class group {  // control::Tasks: tasks spawned on one scheduler, counted until their frames end
public:
	explicit group(scheduler_ref s) noexcept;
	scheduler_ref scheduler() const noexcept;
	void spawn(task<void> t);  // any thread; an escaping exception goes to `failed`
	std::size_t running() const noexcept;
	// true once running() reaches 0; false when `bound` is requested first
	[[nodiscard]] detail::join_awaiter join(cancel_token bound);
	std::function<void(std::exception_ptr)> failed;
};

// tick() every `period` after the previous tick ends, until `stop`; a throw ends it.
template <std::invocable F>
task<void> periodic(std::chrono::nanoseconds period, cancel_token stop, F tick);

template <typename Key, typename Hash = std::hash<Key>>
class debounce {  // control::Debounce: fire(key) once quiet, never later than a random force
public:
	struct timing { std::chrono::nanoseconds quiet, busy, min_force, max_force; };
	debounce(group& g, timing t, cancel_token stop, std::function<void(const Key&)> fire);
	bool touch(const Key& key);  // any thread; false once stop is requested: run the call now
	std::size_t pending() const;
	std::uint64_t fired() const noexcept;
};

struct drained {
	std::vector<std::string> unfinished;  // what each drain left at the bound, in drain order
};

template <typename S>
concept service = requires(S& s, cancel_token bound) {
	{ s.start() } -> std::same_as<task<void>>;           // on the control loop; returns once serving
	s.stop();                                            // any thread, at once, idempotent
	{ s.drain(bound) } -> std::same_as<task<drained>>;   // on the control loop, after stop()
};

// Teardown phases 2 and 3 (task_asio.hh:56-62): close() on every argument, then destroy_roots().
template <typename... S> void teardown(S&... s);
```

`task_asio.hh` gains the process half:

```cpp
class stop_signals {
public:
	explicit stop_signals(asio_scheduler& loop, std::initializer_list<int> signals = {SIGINT, SIGTERM});
	~stop_signals();  // cancels the wait and restores SIG_DFL
	asio_scheduler& loop() const noexcept;
	cancel_token stop() const noexcept;   // the first signal, or request_stop()
	cancel_token hurry() const noexcept;  // the second signal, or request_hurry()
	void request_stop(cancel_reason why = cancel_reason::shutdown) noexcept;  // any thread
	void request_hurry() noexcept;                                            // any thread
	int first_signal() const noexcept;   // 0 when the stop came from request_stop()
};

template <service... S>
task<drained> serve(stop_signals& signals, std::chrono::nanoseconds bound, S&... services);
```

Every `main` reads the same:

```cpp
asio::io_context io(1);
task::asio_scheduler loop(io.get_executor());
task::stop_signals signals(loop);
Registry registry(...); Http http(registry, ...);
task::drained result;
task::spawn(task::scheduler_ref::of(loop), task::serve(signals, 35s, registry, http),
            [&](std::exception_ptr e, std::optional<task::drained> d) { /* keep */ io.stop(); });
io.run();                                        // phase 1 for the control loop
if (!result.unfinished.empty()) { /* log each line */ std::_Exit(EX_TEMPFAIL); }
task::teardown(http, registry, loop);            // phases 2 and 3; destructors are phase 4
```

## Stop and drain order

```text
signal 1 -> stop()   serve: stop+drain(services[n-1]) ... stop+drain(services[0])
signal 2 -> hurry()  -> the bound is requested now; each remaining drain waits for nothing
signal 3 -> SIG_DFL  -> the process ends, even with the loop wedged
```

1. `serve` awaits each `start()` in argument order (dependencies first). A stop during a start
   calls that service's `stop()`. A start that throws drains the started services, then
   `serve` rethrows.
2. On `signals.stop()`, `serve` makes the one bound,
   `deadline<asio_scheduler>(loop, now + bound, signals.hurry())`.
3. For each started service in reverse, `serve` calls `stop()`, then
   `co_await drain(bound.token())`. A later service keeps serving the earlier drains. Once the
   bound is requested, each remaining drain starts its non-blocking close and waits for nothing.
4. `serve` returns the `unfinished` lines. When any remain, threads may still use the
   services, so `main` exits without teardown, which is Xapiand's rule today
   (`manager.cc:1189-1196`).

## Timers

Each wait arms a `wheel::timer_entry` inside the awaiter, in the frame, through `scheduler_ref::arm`
on the loop it runs on. No timer allocates. The Xapiand timers that msg-30 found on
`AsioHost::arm` move as follows:

| Timer | Today: `AsioHost::arm`, one `asio::steady_timer` per arm (`host_asio.h:68-75`) | Moves to |
| :---- | :---- | :---- |
| `control::Periodic` | `control_plane.h:143` | `periodic`, `sleep_for(period, stop)` |
| `control::Debounce` | `control_plane.h:294` | `debounce`, `sleep_until(target, stop)` |
| `engine::SleepFor` | `engine/host.h:165-188` | `task::sleep_for` |
| Registry timers | `database/pool.cc:449-455` | one entry per callback, armed with `arm` from the same post |
| `deadline<engine::Host>` | `Host::start_timer` (`host.h:109`) | `engine::Host` forwards `arm`, `cancel` and `now` as virtuals, the way it forwards `post_call` (`host.h:88-90`); `AsioHost` forwards to its `asio_scheduler`, and `BlockingScheduler` owns a wheel as `inline_scheduler` does |

A closed `asio_scheduler`'s wheel never fires (`task_asio.hh:111-113,222`). Today Xapiand closes
the control scheduler (`manager.cc:1119`), then runs the loop again so the registry's timers fire
while the indexes close (`:1131`). In this shape the registry's drain is a task on the live control
loop, and `close()` comes only after `serve` returns. No drain depends on a closed loop's timer. A
timer still armed at teardown is cancelled when phase 3 destroys its frame (D10).

## manager.cc

| Duty | Today | New owner |
| :---- | :---- | :---- |
| Ordered stop (D16) | `join()` `:1033-1212`; one deadline `:1045`; `insisted()` `:1042` | `serve(signals, shutdown_write_bound, registry, autocommit, http)` drains HTTP (Stop 1), then autocommit (Stop 2), then the registry with the WAL writers and fsync threads (Stops 3-5); a second signal is the hurry |
| Autocommit debounce | `:301-308`; `schedule_autocommit` `:536` | `task::debounce<Endpoint>` in an autocommit service: its stop token ends the waits, and Stop 2 is `group::join` |
| Housekeeping | `control::Periodic` `:826`; `housekeeping_tick` `:506` | `periodic` owned by the schema cache (`schemas->cleanup()`) |
| Node identity | `load/save/set_node_name` `:340-391`; `host_address` `:395-458`; `init` `:762-804`; name checks `:812-821` | free functions of a node module that `main` calls before `serve`; not a service, since it runs no work |
| Server wiring | `make_servers` `:895-985`; `setup_node_task` `:873`; `set_ready_impl` `:989` | the HTTP service's `start()`; `fsyncher()` (`:984`) moves to the registry's `start()` |
| Index settings resolution | `:1257-1275` | callers use the index resolver (`IndexResolverLRU`) directly |
| Metrics | `server_metrics_impl` `:1284`; `reactor_lag_metrics` `:1348`; labels `:883` | a metrics function over the services' counters; labels set in `main` after node identity |
| Signals | self-pipe `:186-215`; `signal_sig` `:462`; `signal_loop` `:483`; `sigaction` in `main.cc:299-300` | `stop_signals`; SIGUSR1 and SIGINFO keep a dump `signal_set` of their own |
| Shutdown retries | `PeriodicTimer` `:184,708`; `shutdown_checks` `:739`; `shutdown_async` `:186` | gone: HTTP's drain awaits the requests in flight under the bound (Q2); `PeriodicTimer` leaves Kronuz/reactor |
| A control task that fails | `control_task_failed` `:731` | `signals.request_stop()`; `main` keeps the exit code |

## Decisions

| # | Decision | Lost, and why |
| :---- | :---- | :---- |
| D1 | One timer: every wait arms a wheel entry through `scheduler_ref::arm`/`cancel` | `AsioHost::arm` allocates a `shared_ptr` state and a node per arm (`host_asio.h:69`) and is a second mechanism. No measurement argues against the wheel (but see Limits) |
| D2 | Cancellable waits return `bool` and never throw | Throwing `cancelled`: unlike `m.lock(token)` and `await_asio(…, token)`, which return a value |
| D3 | A sleep's fire resumes through `resume_on`; a cross-thread cancel goes through `post_call`, with `asio_park`'s resume count (`task_asio.hh:284-310`) | Resuming inside `expire()` runs task code in the wheel's loop and delays later fires |
| D4 | Shutdown is a token: `periodic` and `debounce` take `stop`, with no `stop()` of their own | Xapiand's `stop()` sets an event pointer published under a mutex (`control_plane.h:110-117`) |
| D5 | `group::join` wakes when the count reaches zero | `drain_control`'s 10 ms `run_for` loop (`manager.cc:1007-1015`) and the agent's 20 ms poll |
| D6 | Signals arrive through `asio::signal_set`. After the second, `stop_signals` clears it, and Asio restores `SIG_DFL` (`signal_set_service.ipp:456-465,515-522`) | A self-pipe re-implements `signal_set`'s own. A `sigwait` thread needs the signals masked before every thread starts, library threads included. Flag polling is what Prism and detentd do today |
| D7 | One bound: a `deadline` created at the stop, with `hurry` as its parent | Per-step bounds, and `insisted()` polled in each wait (`manager.cc:1042,1078-1081`) |
| D8 | Start in order; stop and drain in reverse, each `stop()` just before its `drain`; teardown after `serve` | Stopping everything at once: the registry would refuse HTTP's in-flight writes |
| D9 | When work is unfinished at the bound, `main` calls `_Exit(EX_TEMPFAIL)` without teardown | Tearing down under live threads |
| D10 | `asio_scheduler::cancel` is allowed on the teardown thread once closed (the wheel's `release()` permits it) | A refcounted state per sleep, which `deadline` needs because its entry outlives the frame; a sleep's entry does not |
| D11 | A blocking stop, such as `HttpAsioService::stop` joining its reactors, runs on a thread of its own that sets an `event`; `drain` awaits `event::wait(bound)` | `shutdown_wait`'s 50 ms poll (`shutdown_wait.h`) |
| D12 | A `periodic` tick that throws ends that `periodic`; `group::failed` reports it | Continuing silently after each throw; a tick catches only what it tolerates |

## Limits

- The wheel fires up to one 1 ms tick plus the loop's wake late (0.5 to 2.2 ms measured,
  `docs/mechanisms/deadlines.md`). It wakes at the end of the entry's slot, so a 1 ms sleep takes
  1 to 2 ms. Detent's authority and relay loops tick every 1 ms, so their quests measure loop
  throughput before and after the switch. A regression there is the measured reason D1 allows.
- Silent: a `drain` that blocks the loop thread stalls the bound and the hurry. Only the third
  signal ends it.
- Silent: a frame that is not a root, still sleeping when its scheduler dies, gets a `false` fire
  and never resumes. Only roots are destroyed in phase 3.
- Silent: a later `sigaction` for the same signal, such as Xapiand's `main.cc:299-300`, replaces
  `signal_set`'s handler. Each migration removes its own.
- A third signal exits with the signal's status (130 or 143) and without a log flush.

## Open questions

- **Q1 (owner): the bound for Prism and Detent.** Today their stops are unbounded
  (`detentd/main.cc:217` waits on a future; Prism's `service.stop()` blocks). Options: 30 s, or
  per-program flags. Recommendation: 30 s, with a flag.
- **Q2 (Xapiand lead): one bound from the first signal.** Today `shutdown_checks` retries every
  5 s and forces the stop on its eighth try (`manager.cc:692-694,708`), while the acceptors
  still accept; then `join()` spends its own 35 s (`shutdown_write_bound`, `:1045`). In this shape HTTP's `stop()` closes the acceptors at once, and 35 s covers
  everything. Recommendation: accept.

## Checks and first steps

- A1, A2, A4: `rg -n '^## (API|Stop and drain order|Timers|manager.cc)' docs/design/service-shape.md`
  finds four headings, and `campaign verify service-shape` runs the docs check.
- A3: one `design` review, plus the Xapiand lead's agreement, recorded in the journal.

Each step is its own quest with tests that fail without it:

1. task: `sleep_until`/`sleep_for`, `event::wait`, D10. Tests on `inline_scheduler` with
   `advance()`; an asio test where phase 3 destroys a sleeping root under ASan.
2. task: `task_service.hh`, ported from `control_plane.h`, with deterministic `inline_scheduler`
   tests of `periodic`, `debounce` and `group::join`.
3. task: `stop_signals`, `serve`, `teardown`; a test that sends three SIGTERMs.
4. Migrations: Prism first (the smallest), then the Detent agent, relay and detentd, then
   Xapiand's manager.cc quest after its Phase 7c gate.
