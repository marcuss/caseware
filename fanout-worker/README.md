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
| `DeadLetterQueue` | Where given-up work goes, with the attempt count, the cause and both sequence numbers an operator needs. |
| `DownstreamCapacity` | The one place the cap lives. Shared by every publish in flight; each generation of dispatches can halve it once; the operator resizes it. |
| `RetryPolicy` | Exponential backoff with a ceiling on attempts charged to one row. |
| `WorkerPolicy` | The rest of the knobs: when to stop dispatching, how long to back off, the downstream's load budget, the page size. |
| `Timer` | Where retries and backoff wait. Tests drive it by hand. |
| `WorkerListener` | The observability hooks. A hook that throws is logged and ignored. |
| `PublishHandle` | The worker's promise per submission: an outcome once every task settles, or a failure if it could not finish the job. |

## Decisions and the alternatives they beat

**Idempotency lives in the projection row; the claim lives on the engagement.** The state that survives a
restart is the row's `verification` field, written under a condition on `seq`, so a resubmission re-reads
the projection and finds only what is left. Within the process the unit of work is claimed by
`EngagementId`, not by `PublishId`: the backfill and the nightly sweep are publishes of their own over the
same rows, and two publishes that meet on a row must share one load rather than spend the other team's
minute twice. The second publish settles that row as dropped at once, without waiting for the first.
The honest gap: a crash after the load and before the write repeats that one load. The test `crashMidRun`
shows the cost is exactly the loads in flight, never a second write.

**A row is charged an attempt only for something that is about that row.** `TERMINAL` is the engagement
system saying trying again cannot help, and it goes to the dead-letter queue on the first attempt: five
retries across 20,000 rows would burn 1,667 hours of another team's time on failures that cannot succeed.
Every other failure is charged by asking one question — is this the downstream's condition or this file's?

| What happened | Slot | Charged to the row | Ends in |
|---|---|---|---|
| `TERMINAL` | released | – | dead letter, row marked `DEAD_LETTERED` |
| `TRANSIENT` or `TIMED_OUT`, downstream otherwise healthy | released; a timeout's slot is held for the rest of the load budget | yes | retry with backoff, dead letter at the ceiling |
| `TRANSIENT` or `TIMED_OUT` while the downstream is out | as above | no | requeued after the backoff; dispatch stops meanwhile; the cap is halved for a timeout |
| `OVER_CAPACITY` | released | no | requeued after the backoff; cap halved |
| The worker's own store threw | released | no (its own small budget) | requeued; if the store stays down, the publish fails and is not acknowledged |
| The write was refused because the row moved | released | yes | loaded again; if the row keeps moving, reported abandoned, never dead-lettered |

The reason is that a per-row attempt ceiling only bounds anything if the attempts were about the row. A
downstream that refuses instantly costs no downstream minute, so charging refusals would let a ten-minute
busy period dead-letter a whole publish in seconds, and `DEAD_LETTERED` is exactly the state a redelivery
will not repair. So refusals stop dispatch instead: `DownstreamGate` closes after `failuresBeforePause`
and reopens after the backoff, one failure at a time, until a load succeeds. That is the circuit breaker an
earlier version of this README said was not needed; the cap alone does not bound the damage, because the
damage is dead letters, not throughput.

**The gate counts engagements, not failures.** An outage fails many engagements; a bad row fails only
itself. Counting failures made the breaker impossible to tell apart from one corrupt file that the
engagement system answers with a 5xx every time: at the tail of a publish that file is the only thing being
loaded, so its own failures alone declared the downstream out, and a row the downstream is never charged
for is a row that can never reach the ceiling. It was retried for ever, a minute of the other team's
capacity each time, and the publish never finished. So the gate holds the set of engagements that have
failed since the last success, and closes when the set reaches `failuresBeforePause`.

**A timeout is only the downstream's problem when the downstream is in trouble.** The adapter cannot cancel
a load it has stopped waiting for, and that load is still running in the engagement system, so a timeout
keeps its slot for the rest of `loadBudget` whatever caused it: releasing it would let the worker double its
real concurrency against a system that is already slow. But *who* the timeout is about is a separate
question, and the gate answers it. One very large file that alone takes longer than the budget, while its
neighbours load fine, is a fact about that file: it is charged, and it reaches the dead-letter queue at the
ceiling instead of being retried until someone notices. It is also not a reason to halve the region's cap,
which nothing but an operator raises again. When the gate says the downstream is out, the same timeout is
charged to nobody and does halve the cap.

**The cap is one object, and a generation of dispatches can halve it once.** Threads are not the bound:
loads run on virtual threads and the bound is the negotiated slot count in `DownstreamCapacity`. A slot
carries the generation it was taken in; a shrink or an operator resize ends that generation, so a window of
refusals answered by one halving does not halve again for every straggler in the same window, and a
straggler never re-halves the number an operator has just set. A downstream that is still refusing the loads
dispatched at the new limit does halve again, down to one slot, which is the point: that is the engagement
system saying the cap is still too high. In a multi-instance deployment each instance is given its share of
the negotiated cap; a distributed lease counter would be the next step and is not needed to make the point.

**Rows are read a page at a time.** `unverifiedRows` takes a cursor and a limit and returns one page: a
publish covers 20,000 rows and the backfill 800,000, and no store can hand that over in one call, nor can a
message handler wait for it. `submit` reads one page and returns; the next page is read as tasks settle, so
the queue is bounded by roughly a page per publish in flight rather than by the size of the projection.

**Every task settles, and a publish ends exactly one of three ways.** `deadLetters.send`, the projection
writes, the timer and every listener hook are calls into code this module does not own, and any of them can
throw. If one does, the task still settles: as `DROPPED` or `ABANDONED` where the work is over, and
otherwise as an unrecorded settlement. The publish then *completes* when everything settled and nothing was
unrecorded; *completes exceptionally* when something was; or *fails* because the scan threw or the worker
shut down while it still owed work. All three run the same finishing path, so the worker always forgets the
handle, and the last two tell the adapter the same thing: do not acknowledge this delivery, because the next
delivery re-derives what is left from the projection. The dispatch loop is guarded the same way, so a
failing metrics sink cannot take down the only thread that hands out work.

**Dropped and abandoned are different numbers, and the outcome says both.** A dropped row needed nothing:
it was archived, or another publish had already claimed it. An abandoned row is still `UNVERIFIED` and this
worker stopped trying, so nothing will look at it before the next publish of that template — that is the
number the design's "unverified under 1%" objective is measured against, and an adapter that reports one
number should report that one.

**Stale results are discarded, but the row is not.** Before the load, the row is re-read; if it is archived
or already settled, the task is dropped. The sequence that read saw guards *both* of the worker's
conditional writes, the verification and the dead-letter mark: a row that merely moved on while it waited
its turn is still settled, and only a user acting during the minute the load takes refuses the write. Using
the enqueue-time sequence for the mark and the re-read one for the verification, as an earlier round did,
meant that in a thirteen-hour publish almost any terminal row had a letter in the queue and no mark on the
row, so every later publish loaded it again and dead-lettered it again. A refusal that does happen is
reported through `taskAbandoned` naming the letter already sent, because an operator holding a letter for a
row the store does not call dead needs to know that before replaying it. A row refused because it keeps
moving is loaded again rather than left to the next weekly publish, and reported abandoned at the ceiling.

**Fairness is round-robin across firms.** `FairQueue` gives each firm with waiting work the next slot in
turn, so the firm with 40 files is dispatched within 80 positions of the front whatever the 40,000-file
firm is doing, and a firm alone in the queue gets every slot. This meets the design's "no firm holds more
than 5% of the slots while another waits" whenever twenty or more firms wait, and wastes nothing when
fewer do. A weighted or priority scheme was considered and rejected: nobody has asked for one, and
round-robin is the rule an on-call engineer can predict.

**Shutdown finishes in-flight loads and fails the rest.** A load already running has spent its minute, so
it finishes and is recorded. Everything else the publish still owed — work in the queue and work parked on
the retry or backoff timer — is not persisted by the worker; it is re-derived from the projection when the
publish is delivered again. So `shutdown` fails every active handle there and then, before a timer action
can settle a publish's last task and complete it normally: a retry the worker never ran is work it owes and
did not do, and settling it as "not needed" would acknowledge a delivery whose rows are still `UNVERIFIED`
with nothing left to re-drive them.

## What the adapter around this must do

The SQS adapter calls `submit` on delivery, acknowledges on a normal outcome, and extends the message's
visibility on a heartbeat, since a 40,000-row publish at 50 slots runs for thirteen hours and the maximum
visibility timeout is twelve. The `EngagementSystem` adapter enforces a per-call timeout and reports it as
`TIMED_OUT`, not `TRANSIENT`, so the worker keeps accounting for the load the engagement system is still
running; `loadBudget` must be the longest a load can really take there. An exception the adapter does not
classify is a contract violation, and the worker treats it as retryable rather than blaming the engagement.
The `Timer` must be monotonic — a `ScheduledExecutorService` is — because every wait here is a delay, not a
wall-clock deadline, and nothing in the module reads the wall clock. Neither adapter is in this module.

## Left out on purpose

Cancelling a publish in flight. An earlier version had it, and no caller has one: a withdrawal is
deliberately not a reason to cancel, because the base version this worker establishes does not depend on
which head was withdrawn, and the retarget the design does on a withdrawal needs that base known. An
operator kill-switch is the only honest caller, nobody has asked for it, and it cost a flag, a callback and
five branches through the dispatch and settle paths that every future change would have had to reason about.

A ceiling on how long a publish may wait for the engagement system. If the downstream is down for a day the
gate keeps dispatch stopped, `downstreamPaused` fires each time it re-closes, and the publish's outcome
never resolves, so the adapter heartbeats the message. Giving up after N waits would convert a recoverable
stall into 20,000 rows silently left `UNVERIFIED`, with nothing for an operator to replay; the design's
nightly re-verification sweep already owns rows in that state. What is missing is a signal that names the
publish that has now been waiting six hours, and that belongs with the adapter's heartbeat, not here.

Also out: per-row claims shared between processes (the claim here is in-process; the design has one consumer
per region, and the conditional writes are what make a second one safe rather than fast); retrying the
projection write on its own after a successful load; growing the cap back automatically, which is the other
team's number to give; value types for `templateId` and `baseVersion`, which the design's branching lineage
will want and this module does not use.
