package com.caseware.fanout;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.caseware.fanout.DownstreamCapacity.Slot;
import com.caseware.fanout.DownstreamFailure.Kind;
import com.caseware.fanout.EngagementVerifier.Dropped;
import com.caseware.fanout.EngagementVerifier.Failed;
import com.caseware.fanout.EngagementVerifier.StoreFailed;
import com.caseware.fanout.EngagementVerifier.Superseded;
import com.caseware.fanout.EngagementVerifier.Verified;
import com.caseware.fanout.PublishHandle.Settlement;

/**
 * The template-publish fan-out worker: the verifier of the design document. A publish becomes one task per
 * unverified row, read a page at a time; tasks are dispatched round-robin across firms, one downstream load per
 * slot of the shared {@link DownstreamCapacity}, and each settles exactly once.
 *
 * <p>Promises. The same publish submitted twice runs once, and two publishes over the same template load each
 * engagement once, because a row is claimed by engagement and not by publish. A load is never made for a row that
 * no longer needs it. A row is charged an attempt, and so can eventually be dead-lettered, only for a failure that
 * is about that row: while the engagement system is out, or is refusing calls outright, nothing is charged to any
 * row and dispatch stops instead. Every task settles on every path, including one where the dead-letter queue, the
 * store or a listener throws, and a publish that could not record a row fails rather than completes, so the
 * delivery is not acknowledged. Shutdown lets in-flight loads finish and fails the outcome of every publish that
 * still owed work, so the next delivery re-derives it from the projection.
 */
public final class FanoutWorker {

    private static final System.Logger LOG = System.getLogger(FanoutWorker.class.getName());

    private final ProjectionStore projection;
    private final DeadLetterQueue deadLetters;
    private final DownstreamCapacity capacity;
    private final WorkerPolicy policy;
    private final Timer timer;
    private final WorkerListener listener;
    private final EngagementVerifier verifier;
    private final DownstreamGate gate;
    private final FairQueue queue = new FairQueue();
    private final ConcurrentMap<PublishId, PublishHandle> active = new ConcurrentHashMap<>();
    private final ConcurrentMap<PublishId, PublishFeed> feeds = new ConcurrentHashMap<>();
    private final ConcurrentMap<EngagementId, PublishId> claims = new ConcurrentHashMap<>();
    private final ExecutorService loads;
    private final Thread dispatcher = Thread.ofPlatform().name("fanout-dispatcher").daemon(true).unstarted(this::dispatch);
    private volatile boolean stopping;

    public FanoutWorker(ProjectionStore projection, EngagementSystem engagements, DeadLetterQueue deadLetters,
                        DownstreamCapacity capacity, WorkerPolicy policy, Timer timer, WorkerListener listener) {
        this.projection = Objects.requireNonNull(projection, "projection");
        this.deadLetters = Objects.requireNonNull(deadLetters, "deadLetters");
        this.capacity = Objects.requireNonNull(capacity, "capacity");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.timer = Objects.requireNonNull(timer, "timer");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.verifier = new EngagementVerifier(projection, Objects.requireNonNull(engagements, "engagements"));
        this.gate = new DownstreamGate(policy.failuresBeforePause(), policy.downstreamBackoff(), timer, new DownstreamGate.Events() {
            @Override
            public void paused(Duration pause, Throwable cause) {
                safely(() -> listener.downstreamPaused(pause, cause));
            }

            @Override
            public void resumed() {
                safely(listener::downstreamResumed);
            }
        });
        ThreadFactory threads = Thread.ofVirtual().name("fanout-load-", 0)
                .uncaughtExceptionHandler((thread, error) -> LOG.log(System.Logger.Level.ERROR, "load thread died", error))
                .factory();
        this.loads = Executors.newThreadPerTaskExecutor(threads);
    }

    /**
     * Accepts a publish. If the same publish is already running, returns its handle and enqueues nothing.
     * Otherwise the template's unverified rows become tasks, one page now and the rest as the queue drains; rows
     * another process already settled are not among them, which is what makes a resubmission after a restart
     * resume rather than repeat.
     */
    public PublishHandle submit(PublishJob job) {
        if (stopping) throw new IllegalStateException("worker is shutting down");
        PublishHandle running = active.get(job.publishId());
        if (running != null) return running;

        PublishHandle handle = new PublishHandle(job.publishId(), this::finished);
        PublishHandle winner = active.putIfAbsent(job.publishId(), handle);
        if (winner != null) return winner;

        PublishFeed feed = new PublishFeed(job.templateId(), handle, projection, policy.pageSize(),
                task -> enqueue(handle, task), handle::fail);
        feeds.put(job.publishId(), feed);
        safely(() -> listener.publishAccepted(job.publishId()));
        feed.readAhead();
        return handle;
    }

    public void start() {
        dispatcher.start();
    }

    /**
     * Stops dispatching. Loads in flight finish and are recorded, and every publish that still owed work fails at
     * once: queued work and work parked on the retry timer are both unrecorded, so the delivery must not be
     * acknowledged and the next process re-derives what is left from the projection. Failing here rather than in
     * {@link #awaitTermination} is what stops a retry the timer fires a moment later from settling the last task
     * of a publish and completing it normally with rows still unverified.
     */
    public void shutdown() {
        stopping = true;
        feeds.values().forEach(PublishFeed::stop);
        failEveryActivePublish();
        boolean interrupted = false;
        dispatcher.interrupt();
        while (dispatcher.isAlive()) {
            try {
                dispatcher.join();
            } catch (InterruptedException waitAgain) {
                interrupted = true;
                dispatcher.interrupt();
            }
        }
        loads.shutdown();
        if (interrupted) Thread.currentThread().interrupt();
    }

    /** Waits for the loads that were in flight at shutdown to finish and be recorded. */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        boolean drained = loads.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        // A submission that raced the shutdown flag could have landed after shutdown's sweep; this catches it.
        failEveryActivePublish();
        return drained;
    }

    public int queued() {
        return queue.size();
    }

    private void failEveryActivePublish() {
        active.values().forEach(handle -> handle.fail(new IllegalStateException(
                "worker shut down before publish " + handle.publishId() + " finished")));
    }

    private void dispatch() {
        while (!stopping) {
            try {
                dispatchOne();
            } catch (InterruptedException stop) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException unexpected) {
                // The dispatcher is the only thread that hands work out. It reports and carries on rather than
                // dying and leaving every publish to hang.
                LOG.log(System.Logger.Level.ERROR, "dispatch failed", unexpected);
                safely(() -> listener.dispatcherFailed(unexpected));
            }
        }
    }

    private void dispatchOne() throws InterruptedException {
        gate.awaitOpen();
        QueuedTask next = queue.take();
        Slot slot = capacity.acquire();
        safely(() -> listener.taskDispatched(next.task(), next.attempt()));
        try {
            loads.execute(() -> runOne(next, slot));
        } catch (RuntimeException notRunning) {
            capacity.release();
            unsettled(next, notRunning);
        }
    }

    /**
     * One attempt. Whether the failure was the downstream's or this row's is decided once, here, and used by both
     * the slot accounting and the settlement: they must not be allowed to answer it differently, or a row the cap
     * is blamed for is a row nobody ever charges.
     */
    private void runOne(QueuedTask queued, Slot slot) {
        EngagementVerifier.Result result = null;
        boolean downstreamIsOut = false;
        try {
            try {
                result = verifier.verify(queued.task());
                downstreamIsOut = result instanceof Failed failed && failed.kind() != Kind.TERMINAL
                        && gate.recordFailure(queued.task().engagementId(), failed.cause());
            } finally {
                releaseSlot(slot, result, downstreamIsOut);
            }
            settle(queued, result, downstreamIsOut);
        } catch (Throwable unexpected) {
            // Everything between the load and the settlement is a call into code this module does not own. If one
            // of them throws, the task still settles, or the publish would never complete and no redelivery could
            // repair it.
            LOG.log(System.Logger.Level.ERROR, "task " + queued.task().engagementId() + " could not be settled", unexpected);
            unsettled(queued, unexpected);
        }
    }

    /**
     * Returns the slot. A refusal is the engagement system saying outright that the cap is too high, and a timeout
     * says the same only when the downstream is out; one slow file while its neighbours load fine is not a cap
     * that is too high, and halving the region's cap for it would leave the worker at one slot with nothing but an
     * operator to raise it again. A timeout does keep its slot either way, for the rest of the load budget: the
     * load we stopped waiting for is still running down there, and releasing the slot now would let the worker
     * double its real concurrency against the team it is being polite to.
     */
    private void releaseSlot(Slot slot, EngagementVerifier.Result result, boolean downstreamIsOut) {
        if (!(result instanceof Failed failed)) {
            capacity.release();
            return;
        }
        if (failed.kind() == Kind.OVER_CAPACITY || (failed.kind() == Kind.TIMED_OUT && downstreamIsOut)) {
            capacity.shrink(slot).ifPresent(to -> safely(() -> listener.capacityShrunk(slot.limit(), to)));
        }
        Duration stillRunningDownstream = failed.kind() == Kind.TIMED_OUT
                ? policy.loadBudget().minus(failed.elapsed())
                : Duration.ZERO;
        if (stillRunningDownstream.isPositive() && later(stillRunningDownstream, capacity::release)) return;
        capacity.release();
    }

    private void settle(QueuedTask queued, EngagementVerifier.Result result, boolean downstreamIsOut) {
        switch (result) {
            case Verified verified -> {
                gate.recordSuccess();
                safely(() -> listener.taskVerified(queued.task(), verified.loadTime()));
                finish(queued, Settlement.VERIFIED);
            }
            case Dropped dropped -> drop(queued, dropped.reason());
            case Superseded superseded -> {
                gate.recordSuccess();
                loadAgain(queued, superseded.reason());
            }
            case Failed failed -> afterFailure(queued, failed, downstreamIsOut);
            case StoreFailed storeFailed -> afterStoreFailure(queued, storeFailed.cause());
        }
    }

    private void afterFailure(QueuedTask queued, Failed failed, boolean downstreamIsOut) {
        if (failed.kind() == Kind.TERMINAL) {
            deadLetter(queued, failed);
            return;
        }
        if (downstreamIsOut || failed.kind() == Kind.OVER_CAPACITY) {
            waitForTheDownstream(queued, failed.cause());
            return;
        }
        // TRANSIENT or TIMED_OUT while the engagement system is otherwise healthy: the file is what is wrong, so
        // the row is charged and reaches the dead-letter queue at the ceiling instead of being retried for ever.
        retryThisRow(queued, failed);
    }

    /**
     * A refusal or an outage says nothing about this engagement, so it costs the row no attempt and can never
     * dead-letter it. Dispatch is already stopped by the gate if the downstream is out; the row waits with
     * everything else and is tried again when it comes back.
     */
    private void waitForTheDownstream(QueuedTask queued, Throwable cause) {
        Duration delay = policy.downstreamBackoff();
        if (later(delay, () -> requeue(queued))) {
            safely(() -> listener.taskRequeued(queued.task(), delay, "waiting for the engagement system"));
        } else {
            unsettled(queued, cause);
        }
    }

    /** This row's own load failed while the downstream was otherwise healthy, so it is charged to the row. */
    private void retryThisRow(QueuedTask queued, Failed failed) {
        Optional<Duration> delay = policy.retries().delayBefore(queued.attempt() + 1);
        if (delay.isEmpty()) {
            deadLetter(queued, failed);
            return;
        }
        if (later(delay.get(), () -> requeue(queued.nextAttempt()))) {
            safely(() -> listener.retryScheduled(queued.task(), queued.attempt() + 1, delay.get(), failed.cause()));
        } else {
            unsettled(queued, failed.cause());
        }
    }

    /**
     * The row is healthy but a user event landed during the load, so the answer is stale. Load it again rather
     * than leave a row nobody is scheduled to look at until the next publish, and give up without a dead letter
     * if the row keeps moving: there is nothing for an operator to replay.
     */
    private void loadAgain(QueuedTask queued, String reason) {
        Optional<Duration> delay = policy.retries().delayBefore(queued.attempt() + 1);
        if (delay.isEmpty()) {
            abandon(queued, reason);
            return;
        }
        if (later(delay.get(), () -> requeue(queued.nextAttempt()))) {
            safely(() -> listener.taskRequeued(queued.task(), delay.get(), reason));
        } else {
            abandon(queued, reason);
        }
    }

    /**
     * The worker's own store failed. That is never the engagement's fault, so it never writes a dead letter: the
     * row is left unverified, and if the store stays down the publish fails so the delivery is not acknowledged.
     */
    private void afterStoreFailure(QueuedTask queued, Throwable cause) {
        Optional<Duration> delay = policy.retries().delayBefore(queued.storeAttempt() + 1);
        if (delay.isEmpty()) {
            unsettled(queued, cause);
            return;
        }
        if (later(delay.get(), () -> requeue(queued.nextStoreAttempt()))) {
            safely(() -> listener.taskRequeued(queued.task(), delay.get(), "projection store unavailable"));
        } else {
            unsettled(queued, cause);
        }
    }

    private void requeue(QueuedTask retry) {
        if (stopping) {
            // A retry the worker never got to run is work it owes and did not do, not work that was not needed.
            // Settling it as dropped would let the publish complete normally and be acknowledged with the row
            // still unverified and no delivery left to re-drive it.
            unsettled(retry, new IllegalStateException(
                    "worker stopped before " + retry.task().engagementId() + " was verified"));
            return;
        }
        queue.offer(retry);
    }

    private void drop(QueuedTask queued, String reason) {
        safely(() -> listener.taskDropped(queued.task(), reason));
        finish(queued, Settlement.DROPPED);
    }

    private void abandon(QueuedTask queued, String reason) {
        safely(() -> listener.taskAbandoned(queued.task(), reason));
        finish(queued, Settlement.ABANDONED);
    }

    /**
     * Sends the dead letter first and marks the row second, so a crash between them duplicates a letter rather
     * than losing one. The mark is guarded by the sequence this attempt read, the same guard the verification
     * write would have used: a row that merely moved on while it waited its turn is still marked, and only a user
     * acting during the load refuses it. That refusal leaves an operator holding a letter for a row the store does
     * not call dead, which is a row still unverified and nobody's scheduled work, so it is reported abandoned.
     */
    private void deadLetter(QueuedTask queued, Failed failed) {
        VerifyTask task = queued.task();
        boolean marked;
        try {
            deadLetters.send(new DeadLetter(task, queued.attempt(), failed.seqAtLoad(), failed.cause()));
            marked = projection.recordDeadLettered(task.engagementId(), failed.seqAtLoad());
        } catch (RuntimeException notRecorded) {
            unsettled(queued, notRecorded);
            return;
        }
        if (!marked) {
            abandon(queued, "row moved on during the load; the dead letter was already sent");
            return;
        }
        safely(() -> listener.taskDeadLettered(task, queued.attempt(), failed.cause()));
        finish(queued, Settlement.DEAD_LETTERED);
    }

    /** The worker could not record what happened to this row. The row stays unverified and the publish fails. */
    private void unsettled(QueuedTask queued, Throwable cause) {
        safely(() -> listener.taskUnsettled(queued.task(), cause));
        finish(queued, Settlement.UNRECORDED);
    }

    private void finish(QueuedTask queued, Settlement how) {
        // The claim is also the settlement token: it is taken once when the task is enqueued, survives every
        // requeue, and is given up here, so a task cannot be settled twice however it got here.
        if (!claims.remove(queued.task().engagementId(), queued.task().publishId())) return;
        queued.publish().settle(how);
        PublishFeed feed = feeds.get(queued.task().publishId());
        if (feed != null && !stopping) feed.readAhead();
    }

    /**
     * Claims the engagement, not the publish. The scarce thing is a minute of the other team's time for one file,
     * and the design runs the backfill and the nightly sweep as their own publishes over the same rows, so two
     * publishes that meet on a row must share one load rather than make two.
     */
    private void enqueue(PublishHandle handle, VerifyTask task) {
        PublishId claimant = claims.putIfAbsent(task.engagementId(), task.publishId());
        if (claimant != null) {
            safely(() -> listener.taskDropped(task, "engagement already claimed by publish " + claimant));
            handle.settle(Settlement.DROPPED);
            return;
        }
        queue.offer(QueuedTask.first(handle, task));
    }

    private boolean later(Duration delay, Runnable action) {
        try {
            timer.after(delay, action);
            return true;
        } catch (RuntimeException cannotSchedule) {
            LOG.log(System.Logger.Level.ERROR, "timer refused an action", cannotSchedule);
            return false;
        }
    }

    private void safely(Runnable hook) {
        try {
            hook.run();
        } catch (RuntimeException broken) {
            LOG.log(System.Logger.Level.WARNING, "a listener threw", broken);
        }
    }

    private void finished(PublishHandle handle, PublishOutcome outcome) {
        active.remove(handle.publishId(), handle);
        feeds.remove(handle.publishId());
        safely(() -> listener.publishFinished(handle.publishId(), outcome));
    }
}
