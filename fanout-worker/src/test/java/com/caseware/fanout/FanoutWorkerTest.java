package com.caseware.fanout;

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.caseware.fanout.DownstreamFailure.Kind;
import com.caseware.fanout.ProjectionRow.Verification;

/**
 * Each test proves one promise of the worker. Time never passes on its own: the downstream holds, refuses or fails
 * calls exactly when the test says so, retries and backoff wait on a timer the test advances, and every wait is
 * for a condition, not a duration.
 */
class FanoutWorkerTest {

    private static final String TEMPLATE = "audit-2026";
    private static final Duration BACKOFF = Duration.ofSeconds(10);
    private static final Duration LOAD_BUDGET = Duration.ofMinutes(1);

    private final FakeEngagementSystem engagements = new FakeEngagementSystem();
    private final InMemoryProjectionStore projection = new InMemoryProjectionStore();
    private final RecordingDeadLetters deadLetters = new RecordingDeadLetters();
    private final ManualTimer timer = new ManualTimer();
    private final RecordingListener listener = new RecordingListener();
    private final RetryPolicy threeAttempts = new RetryPolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(4));
    private final WorkerPolicy policy = new WorkerPolicy(threeAttempts, 5, BACKOFF, LOAD_BUDGET, 1_000);
    private final List<FanoutWorker> workers = new ArrayList<>();

    @AfterEach
    void stopWorkers() throws InterruptedException {
        engagements.recover();
        engagements.releaseAll();
        for (FanoutWorker worker : workers) {
            worker.shutdown();
            worker.awaitTermination(Duration.ofSeconds(10));
        }
    }

    @Test
    void theCapIsSharedAcrossPublishes_soLoadsInFlightNeverExceedIt() throws Exception {
        projection.seed("template-a", firm("a"), 5);
        projection.seed("template-b", firm("b"), 5);
        engagements.holdCalls();
        FanoutWorker worker = startWorker(new DownstreamCapacity(3));

        PublishHandle first = worker.submit(new PublishJob(new PublishId("p-a"), "template-a"));
        PublishHandle second = worker.submit(new PublishJob(new PublishId("p-b"), "template-b"));
        engagements.awaitArrivals(3);
        assertEquals(3, engagements.inFlight(), "two publishes together fill the cap, not twice the cap");

        engagements.release(1);
        engagements.awaitArrivals(4);
        assertEquals(3, engagements.inFlight(), "a freed slot admits exactly one more load");

        engagements.releaseAll();
        assertEquals(new PublishOutcome(5, 0, 0, 0), outcomeOf(first));
        assertEquals(new PublishOutcome(5, 0, 0, 0), outcomeOf(second));
        assertEquals(3, engagements.maxInFlight());
        assertEquals(10, engagements.arrivals());
    }

    @Test
    void thePublishDeliveredTwice_runsOnceAndWritesEachEngagementsOwnVersion() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 6);
        engagements.holdCalls();
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        PublishHandle first = worker.submit(publish("p1"));
        engagements.awaitArrivals(2);
        PublishHandle duplicate = worker.submit(publish("p1"));
        assertSame(first, duplicate, "a duplicate delivery joins the run already in progress");

        engagements.releaseAll();
        assertEquals(new PublishOutcome(6, 0, 0, 0), outcomeOf(first));
        assertEquals(6, engagements.arrivals());
        assertEquals(ids.stream().collect(toMap(id -> id, id -> "v-" + id)), projection.baseVersions(),
                "every row holds the version loaded for that engagement, not another's");

        PublishHandle afterwards = worker.submit(publish("p1"));
        assertNotSame(first, afterwards);
        assertEquals(new PublishOutcome(0, 0, 0, 0), outcomeOf(afterwards), "nothing is left for a late redelivery");
        assertEquals(6, engagements.arrivals());
        assertEquals(listener.accepted(), listener.finished(), "no publish finishes that was never reported accepted");
    }

    @Test
    void twoPublishesOverOneTemplate_shareOneLoadPerEngagement() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 6);
        engagements.holdCalls();
        FanoutWorker worker = startWorker(new DownstreamCapacity(12));

        PublishHandle real = worker.submit(publish("p-v6"));
        engagements.awaitArrivals(6);

        PublishHandle nightly = worker.submit(new PublishJob(new PublishId("p-nightly"), TEMPLATE));
        assertEquals(new PublishOutcome(0, 6, 0, 0), outcomeOf(nightly),
                "the nightly sweep finds every row claimed and settles without waiting for a load");
        assertEquals(6, engagements.arrivals(), "a row already being loaded is not loaded a second time for another publish");

        engagements.releaseAll();
        assertEquals(new PublishOutcome(6, 0, 0, 0), outcomeOf(real));
        assertEquals(6, engagements.arrivals(), "the scarce thing is a load per engagement, not per publish");
        assertEquals(ids.stream().collect(toMap(id -> id, id -> "v-" + id)), projection.baseVersions());
    }

    @Test
    void aRowThatMovedOnWhileItWaitedItsTurn_isStillDeadLetteredWhenItsLoadIsTerminal() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 1);
        EngagementId poison = ids.get(0);
        engagements.poison(poison, Kind.TERMINAL);
        FanoutWorker worker = newWorker(new DownstreamCapacity(1));

        PublishHandle handle = worker.submit(publish("p1"));
        projection.bumpSeq(poison);
        worker.start();

        assertEquals(new PublishOutcome(0, 0, 0, 1), outcomeOf(handle));
        assertEquals(Verification.DEAD_LETTERED, projection.read(poison).orElseThrow().verification(),
                "the mark is guarded by the seq the attempt read, so a user event before the load does not orphan the letter");
        assertEquals(1, deadLetters.only().task().lastSeqAtEnqueue(), "the operator still sees where the row was at enqueue");
        assertEquals(2, deadLetters.only().seqAtLoad(), "and the seq the mark was made against");
        assertTrue(listener.abandonedReasons().isEmpty());
        assertTrue(listener.droppedReasons().isEmpty());
    }

    @Test
    void aRowThatMovedOnDuringTheLoad_isReportedAbandoned_withTheDeadLetterItAlreadySent() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 3);
        EngagementId poison = ids.get(1);
        engagements.poison(poison, Kind.TERMINAL);
        engagements.duringEachLoad(id -> {
            if (id.equals(poison)) projection.bumpSeq(id);
        });
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        assertEquals(new PublishOutcome(2, 0, 1, 0), outcomeOf(worker.submit(publish("p1"))),
                "a row the store will not call dead is not counted as dead-lettered");
        assertEquals(Map.of(poison, "row moved on during the load; the dead letter was already sent"),
                listener.abandonedReasons(), "the orphan letter is named, not settled in silence as work nobody needed");
        assertEquals(Verification.UNVERIFIED, projection.read(poison).orElseThrow().verification(),
                "the conditional mark was refused, so the row is not dead");
    }

    @Test
    void gracefulShutdown_finishesLoadsInFlight_andTheNextProcessResumesWithoutRepeatingThem() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 10);
        engagements.holdCalls();
        FanoutWorker first = startWorker(new DownstreamCapacity(3));
        PublishHandle handle = first.submit(publish("p1"));
        engagements.awaitArrivals(3);

        first.shutdown();
        engagements.releaseAll();
        assertTrue(first.awaitTermination(Duration.ofSeconds(10)));
        assertEquals(3, projection.count(Verification.VERIFIED), "loads in flight at shutdown are finished and recorded");
        ExecutionException unfinished = assertThrows(ExecutionException.class, () -> outcomeOf(handle));
        assertInstanceOf(IllegalStateException.class, unfinished.getCause(), "an unfinished publish must not be acknowledged");
        assertThrows(IllegalStateException.class, () -> first.submit(publish("p2")));

        FanoutWorker next = startWorker(new DownstreamCapacity(3));
        assertEquals(new PublishOutcome(7, 0, 0, 0), outcomeOf(next.submit(publish("p1"))));
        assertEquals(10, engagements.arrivals(), "no engagement was loaded twice");
        assertEquals(10, projection.count(Verification.VERIFIED));
    }

    @Test
    void shutdownWithARetryOnTheTimer_failsTheOutcome_soTheRowIsDeliveredAgain() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 2);
        EngagementId waiting = ids.get(0);
        engagements.poison(waiting, Kind.TRANSIENT);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> listener.verified() == 1 && listener.retryDelays().size() == 1,
                "one row verified and the other parked on the retry timer");
        worker.shutdown();
        timer.advance(Duration.ofSeconds(1));

        ExecutionException failed = assertThrows(ExecutionException.class, () -> outcomeOf(handle));
        assertInstanceOf(IllegalStateException.class, failed.getCause(),
                "a publish with work still owed is not acknowledged, whether that work was queued or on the timer");
        assertEquals(Verification.UNVERIFIED, projection.read(waiting).orElseThrow().verification());
        assertEquals(0, deadLetters.count());

        engagements.recover();
        FanoutWorker next = startWorker(new DownstreamCapacity(2));
        assertEquals(new PublishOutcome(1, 0, 0, 0), outcomeOf(next.submit(publish("p1"))),
                "the redelivery finds the row the stopped worker still owed");
    }

    @Test
    void crashMidRun_repeatsOnlyTheLoadsThatWereInFlight_andRecordsEachRowOnce() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 6);
        engagements.holdCalls();
        FanoutWorker crashed = startWorker(new DownstreamCapacity(2));
        crashed.submit(publish("p1"));
        engagements.awaitArrivals(2);
        crashed.shutdown();

        FanoutWorker replacement = startWorker(new DownstreamCapacity(2));
        PublishHandle handle = replacement.submit(publish("p1"));
        engagements.awaitArrivals(4);
        engagements.release(2);
        assertTrue(crashed.awaitTermination(Duration.ofSeconds(10)));
        assertEquals(2, projection.count(Verification.VERIFIED), "the crashed process's late results still land");

        engagements.releaseAll();
        assertEquals(new PublishOutcome(4, 2, 0, 0), outcomeAfterBackoff(handle),
                "the replacement's repeats are discarded, not double-written");
        assertEquals(6, projection.count(Verification.VERIFIED));
        assertEquals(8, engagements.arrivals(), "only the two loads in flight at the crash were repeated");
        assertEquals(ids.stream().collect(toMap(id -> id, id -> "v-" + id)), projection.baseVersions(),
                "each row holds its own engagement's version, whichever process wrote it");
    }

    @Test
    void aRefusedLoad_shrinksTheCap_andTheRowIsTriedAgainWithoutBeingChargedForIt() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 6);
        engagements.refuseNext(1);
        DownstreamCapacity capacity = new DownstreamCapacity(4);
        FanoutWorker worker = startWorker(capacity);

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> listener.requeueReasons().size() == 1, "the refused load to be put back");
        assertEquals(2, capacity.limit(), "a refusal halves the cap");
        assertArrayEquals(new int[] {4, 2}, listener.capacityChanges().get(0), "and the operator is told the cap moved");
        assertEquals(1, listener.capacityChanges().size(), "once for that window, not once per load in it");
        assertTrue(listener.retryDelays().isEmpty(), "a refusal is the downstream's condition, not the row's");

        assertEquals(new PublishOutcome(6, 0, 0, 0), outcomeAfterBackoff(handle));
        assertEquals(1, engagements.refusals(), "no further refusals once the cap fits");
        assertEquals(7, engagements.arrivals(), "the refused load was tried again, nothing else");
    }

    @ParameterizedTest
    @EnumSource(names = {"OVER_CAPACITY", "TRANSIENT"})
    void aDownstreamOutage_stopsDispatchAndDeadLettersNothing(Kind kind) throws Exception {
        projection.seed(TEMPLATE, firm("a"), 8);
        engagements.failEveryCall(kind);
        FanoutWorker worker = startWorker(new DownstreamCapacity(4));

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> listener.pauses() == 1, "dispatch to stop while the engagement system is out");
        engagements.recover();

        assertEquals(new PublishOutcome(8, 0, 0, 0), outcomeAfterBackoff(handle),
                "an outage costs no row its retry budget, so every row is still verified");
        assertEquals(0, deadLetters.count(), "nothing an operator has to replay");
        assertEquals(0, projection.count(Verification.DEAD_LETTERED));
        assertEquals(8, projection.count(Verification.VERIFIED));
    }

    @Test
    void oneEngagementFailingEveryLoad_isNotAnOutage_soItStillReachesTheCeiling() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 1);
        EngagementId poison = ids.get(0);
        engagements.poison(poison, Kind.TRANSIENT);
        WorkerPolicy twoEngagementsAreAnOutage = new WorkerPolicy(threeAttempts, 2, BACKOFF, LOAD_BUDGET, 1_000);
        FanoutWorker worker = startWorker(new DownstreamCapacity(4), twoEngagementsAreAnOutage, listener);

        assertEquals(new PublishOutcome(0, 0, 0, 1), outcomeAfterBackoff(worker.submit(publish("p1"))),
                "one corrupt file failing over and over is the file's problem, however many times it fails");
        assertEquals(3, engagements.loadsOf(poison), "its own failures are charged to it, so the ceiling is reached");
        assertEquals(1, deadLetters.count());
        assertEquals(0, listener.pauses(), "and the rest of the region is never held up for it");
    }

    @Test
    void manyEngagementsFailing_isAnOutage_atTheSameThreshold() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 8);
        engagements.failEveryCall(Kind.TRANSIENT);
        WorkerPolicy twoEngagementsAreAnOutage = new WorkerPolicy(threeAttempts, 2, BACKOFF, LOAD_BUDGET, 1_000);
        FanoutWorker worker = startWorker(new DownstreamCapacity(4), twoEngagementsAreAnOutage, listener);

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> listener.pauses() == 1, "dispatch to stop once a second engagement fails too");
        engagements.recover();

        assertEquals(new PublishOutcome(8, 0, 0, 0), outcomeAfterBackoff(handle));
        assertEquals(0, deadLetters.count(), "the same threshold that ignores one bad row still catches a real outage");
    }

    @Test
    void aRowThatAlwaysTimesOut_whileTheDownstreamIsHealthy_isChargedAndTheCapIsLeftAlone() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 4);
        EngagementId slow = ids.get(0);
        engagements.poison(slow, Kind.TIMED_OUT);
        DownstreamCapacity capacity = new DownstreamCapacity(8);
        FanoutWorker worker = startWorker(capacity);

        PublishHandle handle = worker.submit(publish("p1"));
        assertEquals(new PublishOutcome(3, 0, 0, 1), outcomeAfterBackoff(handle),
                "a file that alone takes longer than the budget is a fact about that file");
        assertEquals(3, engagements.loadsOf(slow), "so it is charged, and the ceiling ends it");
        assertEquals(3, deadLetters.only().attempts());
        assertEquals(8, capacity.limit(), "a healthy downstream with one slow file is not a cap that is too high");

        timer.advance(LOAD_BUDGET);
        assertEquals(0, capacity.inFlight(), "each timed-out load's slot came back when its budget ran out");
    }

    @Test
    void timeoutsEverywhere_areTheDownstreamsCondition_soTheyHoldSlotsAndHalveTheCap() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 8);
        engagements.failEveryCall(Kind.TIMED_OUT);
        DownstreamCapacity capacity = new DownstreamCapacity(4);
        WorkerPolicy threeEngagementsAreAnOutage = new WorkerPolicy(threeAttempts, 3, BACKOFF, LOAD_BUDGET, 1_000);
        FanoutWorker worker = startWorker(capacity, threeEngagementsAreAnOutage, listener);

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> !listener.capacityChanges().isEmpty(), "the cap to be halved once the downstream is out");
        assertEquals(2, capacity.limit(), "the whole window of timeouts costs one halving");
        assertEquals(1, listener.capacityChanges().size());
        assertTrue(capacity.inFlight() >= 1, "the engagement system is still running the loads we stopped waiting for");

        engagements.recover();
        assertEquals(new PublishOutcome(8, 0, 0, 0), outcomeAfterBackoff(handle));
        assertEquals(0, deadLetters.count(), "a downstream too slow for everyone dead-letters nobody");
    }

    @Test
    void aTerminalFailure_isDeadLetteredAtOnce_andNotRetriedByALaterDelivery() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 5);
        EngagementId poison = ids.get(2);
        engagements.poison(poison, Kind.TERMINAL);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        assertEquals(new PublishOutcome(4, 0, 0, 1), outcomeOf(worker.submit(publish("p1"))));
        assertEquals(1, engagements.loadsOf(poison), "a terminal failure spends no retries");
        assertTrue(listener.retryDelays().isEmpty());
        DeadLetter deadLetter = deadLetters.only();
        assertEquals(poison, deadLetter.task().engagementId());
        assertEquals(1, deadLetter.attempts());
        assertEquals(Verification.DEAD_LETTERED, projection.read(poison).orElseThrow().verification());

        assertEquals(new PublishOutcome(0, 0, 0, 0), outcomeOf(worker.submit(publish("p1"))));
        assertEquals(1, engagements.loadsOf(poison), "the dead-lettered row waits for an operator, not a redelivery");
    }

    @Test
    void aTransientFailureOnOneRow_retriesWithBackoff_thenDeadLettersAtTheCeiling_withoutStallingThePublish() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 5);
        EngagementId poison = ids.get(0);
        engagements.poison(poison, Kind.TRANSIENT);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        PublishHandle handle = worker.submit(publish("p1"));
        listener.awaitUntil(() -> listener.verified() == 4 && listener.retryDelays().size() == 1,
                "every healthy engagement to finish while the poison one waits for its first retry");
        assertFalse(handle.outcome().toCompletableFuture().isDone());
        assertEquals(List.of(Duration.ofSeconds(1)), listener.retryDelays());

        timer.advance(Duration.ofSeconds(1));
        listener.awaitUntil(() -> listener.retryDelays().size() == 2, "the second retry to be scheduled");
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2)), listener.retryDelays(), "backoff doubles");

        timer.advance(Duration.ofSeconds(2));
        assertEquals(new PublishOutcome(4, 0, 0, 1), outcomeOf(handle));
        assertEquals(3, engagements.loadsOf(poison), "the ceiling is three attempts");
        assertEquals(3, deadLetters.only().attempts());
        assertEquals(0, timer.pending());
    }

    @Test
    void aProjectionStoreThatThrows_isRetried_andNeverBlamedOnTheEngagement() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 3);
        projection.failReads(2);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        assertEquals(new PublishOutcome(3, 0, 0, 0), outcomeAfterBackoff(worker.submit(publish("p1"))));
        assertEquals(3, engagements.arrivals(), "a store failure spends none of the other team's capacity");
        assertEquals(0, deadLetters.count(), "our own store is never the engagement's fault");
        assertEquals(0, projection.count(Verification.DEAD_LETTERED));
    }

    @Test
    void aProjectionStoreThatStaysDown_leavesTheRowsUnverified_andFailsThePublish() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 3);
        projection.failReads(100);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        ExecutionException failed = assertThrows(ExecutionException.class,
                () -> settled(worker.submit(publish("p1"))).get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failed.getCause(), "the delivery must not be acknowledged");
        assertEquals(3, projection.count(Verification.UNVERIFIED), "the next delivery picks the rows up");
        assertEquals(0, deadLetters.count());
        assertEquals(3, listener.unsettled().size());
    }

    @Test
    void aProjectionScanThatThrows_failsThePublish_andTheNextDeliveryRunsIt() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 3);
        projection.failScans(1);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        ExecutionException failed = assertThrows(ExecutionException.class, () -> outcomeOf(worker.submit(publish("p1"))));
        assertInstanceOf(IllegalStateException.class, failed.getCause(), "a publish whose rows we could not read is not acknowledged");
        assertEquals(0, engagements.arrivals());

        assertEquals(new PublishOutcome(3, 0, 0, 0), outcomeOf(worker.submit(publish("p1"))),
                "the redelivery reads the rows and runs the publish");
    }

    @Test
    void aTimerThatRefusesToSchedule_failsThePublishRatherThanLosingTheRow() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 2);
        engagements.poison(ids.get(0), Kind.TRANSIENT);
        timer.refuseEverything();
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        ExecutionException failed = assertThrows(ExecutionException.class, () -> outcomeOf(worker.submit(publish("p1"))));
        assertInstanceOf(IllegalStateException.class, failed.getCause());
        assertEquals(1, listener.unsettled().size(), "a retry that cannot be scheduled settles the task, it does not lose it");
        assertEquals(Verification.UNVERIFIED, projection.read(ids.get(0)).orElseThrow().verification());
        assertEquals(0, deadLetters.count());
    }

    @Test
    void aDeadLetterQueueThatThrows_failsThePublishInsteadOfHangingIt() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 3);
        EngagementId poison = ids.get(1);
        engagements.poison(poison, Kind.TERMINAL);
        deadLetters.failEverySend(new IllegalStateException("dead-letter queue unavailable"));
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        PublishHandle handle = worker.submit(publish("p1"));
        ExecutionException failed = assertThrows(ExecutionException.class, () -> outcomeOf(handle));
        assertInstanceOf(IllegalStateException.class, failed.getCause());
        assertEquals(1, listener.unsettled().size(), "the task settled even though the queue threw");
        assertEquals(Verification.UNVERIFIED, projection.read(poison).orElseThrow().verification(),
                "a row we could not record is left for the next delivery, not marked dead");
        assertEquals(2, projection.count(Verification.VERIFIED), "the healthy rows still finished");

        PublishHandle redelivered = worker.submit(publish("p1"));
        assertNotSame(handle, redelivered, "the publish is not wedged in the worker; a redelivery re-drives it");
        assertThrows(ExecutionException.class, () -> outcomeOf(redelivered));
    }

    @Test
    void aListenerThatThrows_cannotStopThePublishFromFinishing() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 4);
        WorkerListener breaks = new WorkerListener() {
            @Override
            public void taskDispatched(VerifyTask task, int attempt) {
                listener.taskDispatched(task, attempt);
                throw new IllegalStateException("metrics sink is down");
            }

            @Override
            public void taskVerified(VerifyTask task, Duration loadTime) {
                listener.taskVerified(task, loadTime);
                throw new IllegalStateException("metrics sink is down");
            }
        };
        FanoutWorker worker = startWorker(new DownstreamCapacity(2), policy, breaks);

        assertEquals(new PublishOutcome(4, 0, 0, 0), outcomeOf(worker.submit(publish("p1"))));
        assertEquals(4, projection.count(Verification.VERIFIED));
        assertTrue(listener.dispatcherFailures().isEmpty(), "observability failures never reach the dispatch loop");
    }

    @Test
    void aRowThatMovesOn_isStillVerified_whetherItMovedBeforeOrDuringTheLoad() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 3);
        engagements.holdCalls();
        FanoutWorker worker = newWorker(new DownstreamCapacity(1));
        PublishHandle handle = worker.submit(publish("p1"));

        projection.bumpSeq(ids.get(0));
        projection.archive(ids.get(2));
        worker.start();

        engagements.awaitArrivals(1);
        engagements.release(1);
        listener.awaitUntil(() -> listener.verified() == 1, "the row that moved on while it waited to be loaded anyway");

        engagements.awaitArrivals(2);
        projection.bumpSeq(ids.get(1));
        engagements.releaseAll();

        assertEquals(new PublishOutcome(2, 1, 0, 0), outcomeAfterBackoff(handle));
        assertEquals(3, engagements.arrivals(), "the row that moved on during the load was loaded again, not abandoned");
        assertEquals(Map.of(ids.get(2), "engagement archived"), listener.droppedReasons());
        assertEquals(Map.of(ids.get(0), "v-" + ids.get(0), ids.get(1), "v-" + ids.get(1)), projection.baseVersions());
    }

    @Test
    void aRowThatKeepsMovingUnderEveryLoad_isReportedAbandoned_notDeadLettered() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 1);
        EngagementId busy = ids.get(0);
        engagements.duringEachLoad(projection::bumpSeq);
        FanoutWorker worker = startWorker(new DownstreamCapacity(1));

        assertEquals(new PublishOutcome(0, 0, 1, 0), outcomeAfterBackoff(worker.submit(publish("p1"))));
        assertEquals(3, engagements.loadsOf(busy), "the row is loaded again rather than left to the next publish");
        assertEquals(0, deadLetters.count(), "a user editing their file is nothing for an operator to replay");
        assertEquals(Map.of(busy, "row moved on during the load"), listener.abandonedReasons(),
                "an abandoned row is counted apart from work that was never needed");
        assertTrue(listener.droppedReasons().isEmpty());
    }

    @Test
    void rowsAreReadAPageAtATime_soAPublishIsNotHeldInMemoryAllAtOnce() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 10);
        engagements.holdCalls();
        WorkerPolicy twoRowsPerPage = new WorkerPolicy(threeAttempts, 5, BACKOFF, LOAD_BUDGET, 2);
        FanoutWorker worker = startWorker(new DownstreamCapacity(1), twoRowsPerPage, listener);

        PublishHandle handle = worker.submit(publish("p1"));
        engagements.awaitArrivals(1);
        assertTrue(worker.queued() <= 3, "the worker holds about a page, not the whole publish; it held " + worker.queued());

        engagements.releaseAll();
        assertEquals(new PublishOutcome(10, 0, 0, 0), outcomeOf(handle), "every page is still read and verified");
        assertEquals(10, engagements.arrivals());
    }

    @Test
    void aFirmWith40Files_finishesWhileTheFirmWith40000IsStillQueued() throws Exception {
        FirmId big = firm("big");
        FirmId small = firm("small");
        projection.seed("template-big", big, 40_000);
        projection.seed("template-small", small, 40);
        FanoutWorker worker = newWorker(new DownstreamCapacity(8));
        PublishHandle bigPublish = worker.submit(new PublishJob(new PublishId("p-big"), "template-big"));
        PublishHandle smallPublish = worker.submit(new PublishJob(new PublishId("p-small"), "template-small"));
        worker.start();

        assertEquals(40, outcomeOf(smallPublish).verified());
        assertEquals(40_000, outcomeOf(bigPublish, Duration.ofSeconds(60)).verified());
        List<VerifyTask> order = listener.dispatched();
        int lastSmall = order.stream().map(VerifyTask::firmId).toList().lastIndexOf(small);
        assertTrue(lastSmall <= 80, "the small firm's last file was dispatched at position " + lastSmall + " of " + order.size());
    }

    private FanoutWorker startWorker(DownstreamCapacity capacity) {
        return startWorker(capacity, policy, listener);
    }

    private FanoutWorker startWorker(DownstreamCapacity capacity, WorkerPolicy policy, WorkerListener listener) {
        FanoutWorker worker = new FanoutWorker(projection, engagements, deadLetters, capacity, policy, timer, listener);
        workers.add(worker);
        worker.start();
        return worker;
    }

    private FanoutWorker newWorker(DownstreamCapacity capacity) {
        FanoutWorker worker = new FanoutWorker(projection, engagements, deadLetters, capacity, policy, timer, listener);
        workers.add(worker);
        return worker;
    }

    private static PublishJob publish(String id) {
        return new PublishJob(new PublishId(id), TEMPLATE);
    }

    private static FirmId firm(String name) {
        return new FirmId(name);
    }

    private PublishOutcome outcomeOf(PublishHandle handle) throws Exception {
        return outcomeOf(handle, Duration.ofSeconds(10));
    }

    private static PublishOutcome outcomeOf(PublishHandle handle, Duration timeout) throws Exception {
        return handle.outcome().toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private PublishOutcome outcomeAfterBackoff(PublishHandle handle) throws Exception {
        return settled(handle).get(10, TimeUnit.SECONDS);
    }

    /** Moves the clock on whenever the worker has parked work on it, until the publish is done. */
    private CompletableFuture<PublishOutcome> settled(PublishHandle handle) {
        CompletableFuture<PublishOutcome> outcome = handle.outcome().toCompletableFuture();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!outcome.isDone()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the publish never finished; the timer holds " + timer.pending() + " actions");
            }
            if (timer.pending() > 0) timer.advance(BACKOFF);
            else Thread.onSpinWait();
        }
        return outcome;
    }
}
