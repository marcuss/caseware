package com.caseware.fanout;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.caseware.fanout.DownstreamFailure.Kind;
import com.caseware.fanout.ProjectionRow.Verification;

/**
 * Each test proves one promise of the worker. Time never passes on its own: the downstream holds calls until the
 * test releases them, retries wait on a timer the test advances, and every wait is for a condition, not a duration.
 */
class FanoutWorkerTest {

    private static final String TEMPLATE = "audit-2026";

    private final FakeEngagementSystem engagements = new FakeEngagementSystem();
    private final InMemoryProjectionStore projection = new InMemoryProjectionStore();
    private final RecordingDeadLetters deadLetters = new RecordingDeadLetters();
    private final ManualTimer timer = new ManualTimer();
    private final RecordingListener listener = new RecordingListener();
    private final RetryPolicy threeAttempts = new RetryPolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(4));
    private final List<FanoutWorker> workers = new ArrayList<>();

    @AfterEach
    void stopWorkers() throws InterruptedException {
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
        assertEquals(new PublishOutcome(5, 0, 0, false), outcomeOf(first));
        assertEquals(new PublishOutcome(5, 0, 0, false), outcomeOf(second));
        assertEquals(3, engagements.maxInFlight());
        assertEquals(10, engagements.arrivals());
    }

    @Test
    void thePublishDeliveredTwice_runsOnceAndLoadsEachEngagementOnce() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 6);
        engagements.holdCalls();
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        PublishHandle first = worker.submit(publish("p1"));
        engagements.awaitArrivals(2);
        PublishHandle duplicate = worker.submit(publish("p1"));
        assertSame(first, duplicate, "a duplicate delivery joins the run already in progress");

        engagements.releaseAll();
        assertEquals(new PublishOutcome(6, 0, 0, false), outcomeOf(first));
        assertEquals(6, engagements.arrivals());

        PublishHandle afterwards = worker.submit(publish("p1"));
        assertNotSame(first, afterwards);
        assertEquals(new PublishOutcome(0, 0, 0, false), outcomeOf(afterwards), "nothing is left for a late redelivery");
        assertEquals(6, engagements.arrivals());
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
        assertEquals(new PublishOutcome(7, 0, 0, false), outcomeOf(next.submit(publish("p1"))));
        assertEquals(10, engagements.arrivals(), "no engagement was loaded twice");
        assertEquals(10, projection.count(Verification.VERIFIED));
    }

    @Test
    void crashMidRun_repeatsOnlyTheLoadsThatWereInFlight_andRecordsEachRowOnce() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 6);
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
        assertEquals(new PublishOutcome(4, 2, 0, false), outcomeOf(handle), "the replacement's repeats are discarded, not double-written");
        assertEquals(6, projection.count(Verification.VERIFIED));
        assertEquals(8, engagements.arrivals(), "only the two loads in flight at the crash were repeated");
    }

    @Test
    void aDownstreamRefusingLoadsOverItsCap_shrinksTheCapOnce_andEveryEngagementIsStillVerified() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 6);
        engagements.ownLimit(2);
        engagements.holdCalls();
        DownstreamCapacity capacity = new DownstreamCapacity(4);
        FanoutWorker worker = startWorker(capacity);

        PublishHandle handle = worker.submit(publish("p1"));
        engagements.awaitArrivals(4);
        listener.awaitUntil(() -> listener.retryDelays().size() >= 2, "the two refused loads to be scheduled for retry");
        assertEquals(2, engagements.rejections());
        assertEquals(2, capacity.limit(), "two rejections from one window halve the cap once");
        assertEquals(1, listener.capacityChanges().size());

        engagements.releaseAll();
        timer.advance(Duration.ofSeconds(1));
        assertEquals(new PublishOutcome(6, 0, 0, false), outcomeOf(handle));
        assertEquals(2, engagements.rejections(), "no further rejections once the cap fits");
        assertEquals(8, engagements.arrivals());
    }

    @Test
    void aTerminalFailure_isDeadLetteredAtOnce_andNotRetriedByALaterDelivery() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 5);
        EngagementId poison = ids.get(2);
        engagements.poison(poison, Kind.TERMINAL);
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));

        assertEquals(new PublishOutcome(4, 0, 1, false), outcomeOf(worker.submit(publish("p1"))));
        assertEquals(1, engagements.loadsOf(poison), "a terminal failure spends no retries");
        assertTrue(listener.retryDelays().isEmpty());
        DeadLetter deadLetter = deadLetters.only();
        assertEquals(poison, deadLetter.task().engagementId());
        assertEquals(1, deadLetter.attempts());
        assertEquals(Verification.DEAD_LETTERED, projection.read(poison).orElseThrow().verification());

        assertEquals(new PublishOutcome(0, 0, 0, false), outcomeOf(worker.submit(publish("p1"))));
        assertEquals(1, engagements.loadsOf(poison), "the dead-lettered row waits for an operator, not a redelivery");
    }

    @Test
    void aTransientFailure_retriesWithBackoff_thenDeadLettersAtTheCeiling_withoutStallingThePublish() throws Exception {
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
        assertEquals(new PublishOutcome(4, 0, 1, false), outcomeOf(handle));
        assertEquals(3, engagements.loadsOf(poison), "the ceiling is three attempts");
        assertEquals(3, deadLetters.only().attempts());
        assertEquals(0, timer.pending());
    }

    @Test
    void aRowThatMovedOn_isLeftAlone_whetherBeforeOrDuringTheLoad() throws Exception {
        List<EngagementId> ids = projection.seed(TEMPLATE, firm("a"), 3);
        engagements.holdCalls();
        FanoutWorker worker = newWorker(new DownstreamCapacity(1));
        PublishHandle handle = worker.submit(publish("p1"));

        projection.bumpSeq(ids.get(0));
        projection.archive(ids.get(2));
        worker.start();
        engagements.awaitArrivals(1);
        projection.bumpSeq(ids.get(1));
        engagements.releaseAll();

        assertEquals(new PublishOutcome(0, 3, 0, false), outcomeOf(handle));
        assertEquals(1, engagements.arrivals(), "only the row that still needed a load was loaded");
        assertEquals(0, projection.count(Verification.VERIFIED), "a stale result never overwrites a newer fact");
        assertEquals(Map.of(ids.get(0), "row moved on before the load",
                            ids.get(1), "row moved on during the load",
                            ids.get(2), "engagement archived"),
                listener.droppedReasons());
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

    @Test
    void cancel_dropsQueuedWork_andLetsLoadsInFlightFinish() throws Exception {
        projection.seed(TEMPLATE, firm("a"), 10);
        engagements.holdCalls();
        FanoutWorker worker = startWorker(new DownstreamCapacity(2));
        PublishHandle handle = worker.submit(publish("p1"));
        engagements.awaitArrivals(2);

        handle.cancel();
        engagements.releaseAll();

        assertEquals(new PublishOutcome(2, 8, 0, true), outcomeOf(handle));
        assertEquals(2, engagements.arrivals(), "no load starts after the cancel");
        assertEquals(2, projection.count(Verification.VERIFIED), "the minute already spent is not thrown away");
        assertEquals(0, worker.queued());
    }

    private FanoutWorker startWorker(DownstreamCapacity capacity) {
        FanoutWorker worker = newWorker(capacity);
        worker.start();
        return worker;
    }

    private FanoutWorker newWorker(DownstreamCapacity capacity) {
        FanoutWorker worker = new FanoutWorker(projection, engagements, deadLetters, capacity, threeAttempts, timer, listener);
        workers.add(worker);
        return worker;
    }

    private static PublishJob publish(String id) {
        return new PublishJob(new PublishId(id), TEMPLATE);
    }

    private static FirmId firm(String name) {
        return new FirmId(name);
    }

    private static PublishOutcome outcomeOf(PublishHandle handle) throws Exception {
        return outcomeOf(handle, Duration.ofSeconds(10));
    }

    private static PublishOutcome outcomeOf(PublishHandle handle, Duration timeout) throws Exception {
        return handle.outcome().toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
