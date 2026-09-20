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
| `ProjectionStore` | Per-region rows. Every write is conditional on the `seq` the worker saw and on the row still being `UNVERIFIED`. |
| `DeadLetterQueue` | Where given-up work goes, with the attempt count and cause an operator needs. |
| `DownstreamCapacity` | The one place the cap lives. Shared by every publish in flight; halves on a rejection; the operator resizes it. |
| `RetryPolicy` | Exponential backoff with a ceiling on attempts. |
| `Timer` | Where retries wait. Tests drive it by hand. |
| `WorkerListener` | The observability hooks. |
| `PublishHandle` | The worker's promise per submission: an outcome once every task settles, cancellation until then. |

## Decisions and the alternatives they beat

**Idempotency lives in the projection row, not in the worker.** The key is `(publishId, engagementId)`; the
state is the row's `verification` field, written under a condition on `seq`. A duplicate delivery joins the
run in progress (`submit` returns the same handle). A resubmission after a restart re-reads the projection
and finds only what is left. The alternative, a separate dedup table or an SQS deduplication id, adds a
second store that can disagree with the first and a five-minute window that a thirteen-hour publish
outlives. The honest gap: a crash after the load and before the write repeats that one load. The test
`crashMidRun` shows the cost is exactly the loads in flight, never a second write.

**The cap is one object, and rejections shrink it.** Threads are not the bound: loads run on virtual threads
and the bound is the negotiated slot count in `DownstreamCapacity`. An `OVER_CAPACITY` rejection halves the
cap once per window (`releaseRefused` returns the slot and halves in one step, and ignores a second rejection from the same window), and nothing grows it
back but the operator, because the other team owns that number. In a multi-instance deployment each
instance is given its share of the negotiated cap; a distributed lease counter would be the next step and is
not needed to make the point.

**Retry only what a retry can fix.** `TERMINAL` failures and unexpected exceptions go to the dead-letter
queue on the first attempt: five retries across 20,000 rows would burn 1,667 hours of another team's time
on failures that cannot succeed. `TRANSIENT` and `OVER_CAPACITY` retry with doubling backoff up to the
ceiling. A dead-lettered row is marked in the projection so the next delivery does not try it again; the
next event on that engagement, or an operator replay, resets it. No jitter: the cap serialises retries, so
a synchronised burst is already bounded by it.

**Fairness is round-robin across firms.** `FairQueue` gives each firm with waiting work the next slot in
turn, so the firm with 40 files is dispatched within 80 positions of the front whatever the 40,000-file
firm is doing, and a firm alone in the queue gets every slot. This meets the design's "no firm holds more
than 5% of the slots while another waits" whenever twenty or more firms wait, and wastes nothing when
fewer do. A weighted or priority scheme was considered and rejected: nobody has asked for one, and
round-robin is the rule an on-call engineer can predict.

**Stale results are discarded twice.** Before the load, the row is re-read and dropped if it moved on or
was settled elsewhere. After the load, the conditional write refuses a row that moved during the minute.
Both guards are the design's; the second is what makes the first safe to skip.

**Shutdown finishes in-flight loads and abandons the rest.** A load already running has spent its minute,
so it finishes and is recorded. Queued work is not persisted by the worker; it is re-derived from the
projection when the publish is delivered again, which is why the handle's outcome fails at shutdown
rather than completing: the adapter must not acknowledge the message. Cancellation is the same idea per
publish, driven by a withdrawal.

## What the adapter around this must do

The SQS adapter calls `submit` on delivery, acknowledges on a normal outcome, and extends the message's
visibility on a heartbeat, since a 40,000-row publish at 50 slots runs for thirteen hours and the maximum
visibility timeout is twelve. The `EngagementSystem` adapter enforces a per-call timeout and reports it as
`TRANSIENT`, so a hung call cannot hold a slot forever. Neither is in this module.

## Left out on purpose

A circuit breaker for a downstream outage (the cap bounds the damage, backoff bounds the rate); per-row
claims for concurrent consumers of the same delivery (one consumer per region in the design); retrying the
projection write on its own after a successful load; paging of `unverifiedRows`.
