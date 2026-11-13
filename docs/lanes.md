> Status: CURRENT (2026-11-12) how the heavy lane classes a scope and orders its resumptions, read from `task.hh` and `task_asio.hh` on quest/xapiand/task-lanes. The design and its evidence are Xapiand's `docs/design/loop-scheduling.md` (D1-D5).

# The heavy lane

A loop that runs both short requests and long ones should run the short ones first, without a
knob and without starving the long ones. A budget opted into lanes classes its scope by the slices
it has spent (least attained service, two levels): light while it has spent fewer than
`light_slices`, heavy from then on. Light resumptions run with the loop's ordinary posts; every
resumption of a heavy scope waits in the heavy lane, which runs one turn per pass of the loop.

## The pieces

- `budget(slice, token = {}, light_slices = 0)`. `checkpoint()` counts each spent slice
  (`spent()`). With `light_slices` N > 0, a spent slice before the N-th requeues the task with
  `post()`, at the tail of the ordinary posts. The N-th, and any slice spent in a heavy scope,
  posts it heavy.
- The lane is a bit in `scheduler_ref`, packed into the low bit of the vtable pointer (vtables
  are aligned), so `scheduler_ref` stays 16 bytes in every promise, awaiter and waiter.
  `heavy()` reads it and `with_heavy(bool)` copies the reference with it set or clear. A heavy
  reference's `post()` and `post_demoted()` call the scheduler's `post_heavy()`.
- The scope is `task::current()`, a thread-local reference set by `running_on` for each resume.
  Whatever captures it at suspension (a task's continuation, an event or lock waiter, an
  `await_asio`, a chore's reference) carries the lane, so a heavy task's completions, events,
  timers and I/O all come back heavy, with no change to any awaiter.
- `asio_scheduler` keeps one demoted queue, drained one handle per pass. Each entry records
  whether it came in heavy; the drainer resumes it in a heavy scope (`with_heavy(true)`) or a
  light one. `post_demoted()` and `post_heavy()` are callable from any thread: off the loop
  they post a hop that queues the entry from the loop, so `demoted()` counts it once it arrives.
  Over an executor that is not an `io_context`'s the scheduler cannot tell its thread, so an
  off-thread call is unsupported, and a Debug assertion catches one made from another
  scheduler's scope.
- `await_asio` resumes inline in a top-level handler only for a light reference. A heavy one
  posts, or its first turn would run outside the lane.
- `task::enter_light()` marks the current scope light. `budget::rearm_light(allowance)` marks it
  light and sets the count so that the scope runs `allowance` more slices light (all
  `light_slices` by default; 0 makes the next spent slice heavy).
- A scheduler without `post_heavy` gets `post_demoted` in its place, and one without that gets
  `post`. `inline_scheduler` keeps its order: demoted and heavy handles run only when nothing
  else is ready, in a light scope.

## Rules

- **The default is today's policy.** `light_slices` 0 demotes every spent slice with
  `post_demoted()` and never makes a scope heavy; the drainer resumes those entries light. A
  budget that passes no `light_slices` behaves as before the lane existed.
- **Heavy is sticky.** Once a scope is heavy, every checkpoint posts heavy, whatever the count.
  Only `enter_light()` and `rearm_light()` clear it.
- **Equality ignores the lane.** `operator==` compares the scheduler only, so a child that
  turned heavy completes into its parent by symmetric transfer, and the parent continues in the
  child's heavy scope.
- **A join continues in the last child's lane.** `when_all` resumes its parent from the child
  that finishes last, so a heavy last child leaves the parent heavy, as any completion does.
- **A rearm covers what starts after it.** `rearm_light()` changes the count and the current
  scope; a sibling that already captured a heavy reference stays heavy, and so does the join
  it finishes. A streaming request that overlaps reading the next chunk with applying the
  previous one rearms in the parent, before it starts the chunk's children, not inside the
  read: a rearm inside the read leaves the next chunk heavy (`when_all_join_lane`).
- **Leaks are bounded.** Heavy falls back to light when a continuation was captured on a
  reference of another identity (a wrapper scheduler), and separate budgets count apart. The
  count restores heavy at the next spent slice.

## Bounds

The drainer holds one place in the loop's queue, so every pass with a non-empty heavy lane runs
exactly one heavy turn. A light resumption waits for at most one heavy turn per pass, plus the
light handles queued ahead of it. A timer is seen at the pass's poll and runs in the next pass:
at most two heavy turns after its expiry (`timer_within_two_heavy_turns`). A heavy handle at
position p runs within p passes. Fairness is per handle: a heavy request with several child
tasks takes one turn per child per pass.

## Tests

`test/lanes_test.cc`, one test per rule; `test/lanes_mutant.py NAME` builds it against a mutant
of the headers and passes when the rule's test fails:

| Test | Mutant |
|---|---|
| `light_overtakes_heavy` | `light-posts-demoted`: a light spent slice posts demoted |
| `timer_within_two_heavy_turns` | `drain-runs-all`: the drainer runs the whole queue in one turn |
| `heavy_progresses_under_flood` | `strict-priority`: the drainer runs only once the loop is idle |
| `heavy_sticky_across_chore` | `post-ignores-lane`: `post()` ignores the lane |
| `cross_thread_hop` | `no-hop`: an off-thread call pushes from its own thread |
| `default_unchanged` | `default-heavy`: `light_slices` 0 enters the heavy lane |
| `equality_ignores_lane` | `eq-compares-lane`: `operator==` compares the lane |
| `rearm_light_resumes_light` | `rearm-stays-heavy`: `rearm_light()` leaves the scope heavy |
| `when_all_join_lane` | `rearm-keeps-count`: `rearm_light()` keeps the count |
| `turns_heavy_at_light_slices` | `heavy-late`: heavy one slice after `light_slices` |
| `await_asio_heavy_posts` | `inline-heavy`: a heavy reference resumes inline |

`test/resume_ab.py BASE` times the resume path with `light_slices` 0 (`test/resume_bench.cc`)
against task at BASE, in ABBA blocks.
