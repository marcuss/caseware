# Template-publish fan-out worker

The verifier from `docs/design.md` (§1–§2). The publish path never loads an engagement: the materializer
settles rows from the projection alone and leaves behind the rows whose base version is unconfirmed:
`UNKNOWN` in the design's state machine, `Verification.UNVERIFIED` here, which the store folds into the
user-visible `state`. This worker settles those, and runs the backfill and the nightly re-verification as
publishes of their own over the same rows. Every load costs about a minute of another team's capacity, so
the constraint is politeness and exactness rather than throughput.

## Running it

`mvn -q -B test` in this directory. Java 21, JUnit 5, no other dependencies; 40 tests, which drive time
through `ManualTimer` rather than sleeping. There is no `main`: `FanoutWorker` takes its collaborators in
the constructor, `start()` starts the single dispatcher thread, and `submit(PublishJob)` returns a
`PublishHandle`. The SQS consumer, the engagement client, the store and the metrics sink are adapters that
implement the interfaces below and are not in this module.

| Type | What it promises |
|---|---|
| `EngagementSystem` | `loadEffectiveVersion` returns the version a file is really on, costs a minute, and throws a `DownstreamFailure` classified `TRANSIENT`, `OVER_CAPACITY`, `TIMED_OUT` or `TERMINAL`. |
| `ProjectionStore` | Pages of this template's `UNVERIFIED` rows, and two writes that land only if the row is still `UNVERIFIED` at the `seq` the caller saw. It owns the user-visible fields; the worker supplies the base version and nothing else. |
| `DeadLetterQueue` | Takes given-up work with the attempt count, the cause and both sequence numbers. |
| `DownstreamCapacity` | The only place the cap lives. `acquire` blocks; `shrink` halves the limit once per generation of dispatches; `setLimit` is the operator's. |
| `RetryPolicy` / `WorkerPolicy` | The attempt ceiling and backoff; the outage threshold, the backoff, the downstream load budget and the page size. |
| `Timer` | Runs an action after a delay. Every wait in the module is a delay, never a wall-clock deadline. |
| `WorkerListener` | Fires after the fact it reports. A hook that throws is logged and ignored. |
| `PublishHandle` | One outcome per submission: `PublishOutcome` when every task settled and each settlement was recorded, an exception when one was not or the worker stopped first. The second means do not acknowledge the delivery. |

## Tradeoffs

**The cap is taken in the dispatch loop, not inside the load.** `dispatchOne` takes the task off the queue,
acquires a slot, and only then hands it to a virtual thread. That is the one point every publish passes
through, so a single `DownstreamCapacity` bounds concurrent loads however many publishes are in flight, and
back-pressure appears as a queue rather than as thousands of started loads. Threads are not the bound. The
costs: the dispatcher is a serialization point, a task leaves the fair queue before it waits for a slot,
and the cap bounds this process only, so a multi-instance deployment divides the negotiated number
statically.

**The idempotency key is the engagement, and the durable form of it is the row.** Work is claimed by
`EngagementId`, not by `PublishId`, so the nightly sweep meeting the weekly publish on a row settles as
dropped rather than spending a second minute on it. What survives a restart is the row's `verification`
field, written under a condition on `seq`. It does not protect against a repeated *load*: nothing is
persisted before the load, so a crash or an overlapping process repeats the loads in flight and only the
write is deduplicated. It does not stop duplicate dead letters, since the letter is sent before the row is
marked. It is not a claim other processes can see. And it loses to a user by design: an event during the
load moves `seq`, the write is refused, and the answer is discarded.

**Restart.** `shutdown` stops the feeds, fails every active publish, and lets loads in flight finish and be
recorded. Queued work and work parked on the retry timer are not persisted, so failing the handle is what
keeps the delivery unacknowledged; the next delivery re-derives what is left by scanning the projection for
`UNVERIFIED` rows. The cost is the loads in flight at the moment of the crash, and nothing else.

**A poison engagement is isolated by deciding who a failure is about.** `TERMINAL` dead-letters on the
first attempt, because retrying rows that cannot succeed would burn hundreds of hours of the other team's
time, and the `DEAD_LETTERED` mark keeps later publishes from finding the row. A `TRANSIENT` or `TIMED_OUT`
failure while the downstream is otherwise healthy is charged to the row and reaches the ceiling.
`DownstreamGate` counts distinct failing engagements rather than failures, so one file that fails every
time cannot declare an outage and win free retries for ever; when the downstream really is out, dispatch
stops and no row is charged.

**A timeout keeps its slot, and the cap only shrinks.** The adapter cannot cancel a load it has stopped waiting
for, so a `TIMED_OUT` slot is held for the rest of `loadBudget` before it is released, or the worker would
double its real concurrency against a system that is already slow. An `OVER_CAPACITY` refusal halves the cap,
once per generation of dispatches, and nothing grows it back but `setLimit`: over a thirteen-hour publish, one
refusal an hour walks 50 down to 1 in six hours, so `capacityShrunk` is a page, not a log line.

**Fairness is round-robin across firms, in `FairQueue`.** Each firm with work waiting gets the next slot,
so the firm with 40 files is not stuck behind the firm with 40,000. This is not the design's 5% cap:
round-robin meets it once twenty or more firms wait, and idles nothing when fewer do. It costs three things.
It balances only what is currently queued, which is about one page per publish, so within a single publish
fairness is whatever order the store's index returns rows in; the 40,000-against-40 test runs two publishes.
It is per-process. And it stretches the large firm's publish, since a firm that joins late takes a turn
ahead of older work.

**Left out on purpose**, because this is one component and not an application. The adapters: the SQS
consumer needs a visibility heartbeat, since a 40,000-row publish at 50 slots runs about thirteen hours
against a twelve-hour maximum. A cross-process lease, which costs a table and a renewal and buys only the
loads in flight at a crash. Publish cancellation, which no caller has, since a withdrawal does not change
the base version this worker establishes. Automatic growth of the cap, which is the other team's number to
give. Retrying only the projection write after a successful load: today a store failure afterwards discards
that minute and reloads later, and fixing it means carrying the loaded version on the requeued task. A
ceiling on how long a publish may wait out an outage, which would turn a recoverable stall into rows left
`UNVERIFIED` with nothing to replay.
