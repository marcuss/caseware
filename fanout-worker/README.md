# Template-publish fan-out worker

The verifier from the design document (`docs/design.md`, sections 1 to 3), in Java 21 with no framework.
Build and test: `mvn -q -B test`.

## Which flow this serves, and why it may spend a minute per engagement

The publish path never loads an engagement: the materializer settles rows from the projection alone, in
seconds. What it cannot settle, a row whose base version is `UNVERIFIED`, it leaves to this worker. The
worker also runs the one-off backfill and the nightly re-verification, as synthetic publishes with their own
ids. Every load it makes costs a minute of the engagement team's capacity, so the worker's job is less to
be fast than to be polite, exact, and impossible to confuse by a redelivery or a restart.

## The contracts

| Type | Promise |
|---|---|
| `PublishJob` | A publish as delivered: a `PublishId` and a template. It names no engagements; the worker asks the projection. |
| `VerifyTask` | One unit of work: one engagement for one publish, with the row's `seq` at enqueue time. |
| `EngagementSystem` | The other team's system. `loadEffectiveVersion` costs a minute and throws a classified `DownstreamFailure`. |
| `ProjectionStore` | Per-region rows, read a page at a time. Every write is conditional on the `seq` the worker saw and on the row still being `UNVERIFIED`. |
| `DeadLetterQueue` | Where given-up work goes, with the attempt count and cause an operator needs. |
| `DownstreamCapacity` | The one place the cap lives. Shared by every publish in flight; halves once per generation of dispatches on a refusal or a timeout; the operator resizes it. |
| `RetryPolicy` | Exponential backoff with a ceiling on attempts charged to one row. |
| `WorkerPolicy` | The rest of the knobs: when to stop dispatching, how long to back off, the downstream's load budget, the page size. |
| `Timer` | Where retries and backoff wait. Tests drive it by hand. |
| `WorkerListener` | The observability hooks. A hook that throws is logged and ignored. |
| `PublishHandle` | The worker's promise per submission: an outcome once every task settles, cancellation until then. |

## Decisions and the alternatives they beat

**Idempotency lives in the projection row; the claim lives on the engagement.** The state that survives a
restart is the row's `verification` field, written under a condition on `seq`, so a resubmission re-reads
the projection and finds only what is left. Within the process the unit of work is claimed by
`EngagementId`, not by `PublishId`: the backfill and the nightly sweep are publishes of their own over the
same rows, and two publishes that meet on a row must share one load rather than spend the other team's
minute twice. The second publish settles that row as dropped at once, without waiting for the first.
The honest gap: a crash after the load and before the write repeats that one load. The test `crashMidRun`
shows the cost is exactly the loads in flight, never a second write.

**A row is dead-lettered only for something that is about that row.** `TERMINAL` is the engagement system
saying trying again cannot help, and it goes to the dead-letter queue on the first attempt: five retries
across 20,000 rows would burn 1,667 hours of another team's time on failures that cannot succeed. Every
other failure is charged differently:

| What happened | Slot | Charged to the row | Ends in |
|---|---|---|---|
| `TERMINAL` | released | – | dead letter, row marked `DEAD_LETTERED` |
| `TRANSIENT`, downstream otherwise healthy | released | yes | retry with backoff, dead letter at the ceiling |
| `TRANSIENT` during an outage, `OVER_CAPACITY` | released | no | requeued after the backoff; dispatch stops meanwhile |
| `TIMED_OUT` | held for the rest of the load budget | no | requeued after the backoff; cap halved |
| The worker's own store threw | released | no (its own small budget) | requeued; if the store stays down, the publish fails and is not acknowledged |
| The write was refused because the row moved | released | yes | loaded again; if the row keeps moving, reported abandoned, never dead-lettered |

The reason is that a per-row attempt ceiling only bounds anything if the attempts were about the row. A
downstream that refuses instantly costs no downstream minute, so charging refusals would let a ten-minute
busy period dead-letter a whole publish in seconds, and `DEAD_LETTERED` is exactly the state a redelivery
will not repair. So refusals stop dispatch instead: `DownstreamGate` counts consecutive downstream
failures, closes after `failuresBeforePause`, and reopens after the backoff, one failure at a time, until a
load succeeds. That is the circuit breaker an earlier version of this README said was not needed; the cap
alone does not bound the damage, because the damage is dead letters, not throughput.

**A timeout does not free downstream capacity, so it does not free a slot.** The adapter cannot cancel a
load it has stopped waiting for, and that load is still running in the engagement system. Releasing the
slot then, as a plain `TRANSIENT` would, doubles the worker's real concurrency against a system that is
already slow. `TIMED_OUT` keeps the slot for the rest of `loadBudget` and halves the cap, like a refusal.

**The cap is one object, and refusals shrink it once per generation.** Threads are not the bound: loads run
on virtual threads and the bound is the negotiated slot count in `DownstreamCapacity`. A slot carries the
generation it was taken in; a shrink or an operator resize ends that generation, so a burst of refusals
from one window halves once, and a straggler refusal never re-halves the number an operator has just set.
In a multi-instance deployment each instance is given its share of the negotiated cap; a distributed lease
counter would be the next step and is not needed to make the point.

**Rows are read a page at a time.** `unverifiedRows` takes a cursor and a limit and returns one page: a
publish covers 20,000 rows and the backfill 800,000, and no store can hand that over in one call, nor can a
message handler wait for it. `submit` reads one page and returns; the next page is read as tasks settle, so
the queue is bounded by roughly a page per publish in flight rather than by the size of the projection.

**Every task settles, on every path.** `deadLetters.send`, the projection writes, the timer and every
listener hook are calls into code this module does not own, and any of them can throw. If one does, the
task still settles: as `DROPPED` where the work was not needed, and otherwise as an unrecorded settlement,
which makes the publish's outcome fail rather than complete. That distinction is the whole point of the
outcome contract: a failed outcome tells the adapter not to acknowledge the delivery, and because the
handle is also removed, the next delivery re-drives the publish instead of joining a hang. The dispatch
loop is guarded the same way, so a failing metrics sink cannot take down the only thread that hands out
work.

**Stale results are discarded, but the row is not.** Before the load, the row is re-read; if it is archived
or already settled, the task is dropped, and the sequence that read saw is what guards the write, so a row
that merely moved on while it waited its turn is still verified. If a user acts during the minute the load
takes, the conditional write is refused and the task is loaded again rather than left to the next weekly
publish. A row that keeps moving until the ceiling is reported through `taskAbandoned`, separately from
work that was never needed, because "still unverified" and "nothing to do" are different numbers to an
on-call engineer and only one of them belongs in the design's 1% objective.

**Fairness is round-robin across firms.** `FairQueue` gives each firm with waiting work the next slot in
turn, so the firm with 40 files is dispatched within 80 positions of the front whatever the 40,000-file
firm is doing, and a firm alone in the queue gets every slot. This meets the design's "no firm holds more
than 5% of the slots while another waits" whenever twenty or more firms wait, and wastes nothing when
fewer do. A weighted or priority scheme was considered and rejected: nobody has asked for one, and
round-robin is the rule an on-call engineer can predict.

**Shutdown finishes in-flight loads and abandons the rest. Cancel is for operators, not for withdrawals.**
A load already running has spent its minute, so it finishes and is recorded. Queued work is not persisted
by the worker; it is re-derived from the projection when the publish is delivered again, which is why the
handle's outcome fails at shutdown rather than completing. `cancel(PublishId)` is on the worker, so a stop
message that arrives on a different delivery can use it without the adapter keeping its own copy of the
worker's state. A withdrawal is deliberately not a reason to cancel: the base version this worker
establishes does not depend on which head was withdrawn, and the retarget the design does on a withdrawal
needs that base known.

## What the adapter around this must do

The SQS adapter calls `submit` on delivery, acknowledges on a normal outcome, and extends the message's
visibility on a heartbeat, since a 40,000-row publish at 50 slots runs for thirteen hours and the maximum
visibility timeout is twelve. The `EngagementSystem` adapter enforces a per-call timeout and reports it as
`TIMED_OUT`, not `TRANSIENT`, so the worker keeps accounting for the load the engagement system is still
running; `loadBudget` must be the longest a load can really take there. An exception the adapter does not
classify is a contract violation, and the worker treats it as retryable rather than blaming the engagement.
Neither adapter is in this module.

## Left out on purpose

Per-row claims shared between processes (the claim here is in-process; the design has one consumer per
region, and the conditional writes are what make a second one safe rather than fast); retrying the
projection write on its own after a successful load; growing the cap back automatically, which is the
other team's number to give; value types for `templateId` and `baseVersion`, which the design's branching
lineage will want and this module does not use.
