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
 * no longer needs it. A row is dead-lettered only for a failure that is about that row: a refusal, a timeout or a
 * downstream outage costs the row no attempt and stops dispatch instead. Every task settles on every path,
 * including one where the dead-letter queue, the store or a listener throws, and a publish that could not record
 * a row fails rather than completes, so the delivery is not acknowledged. Shutdown lets in-flight loads finish
 * and leaves queued work to the next delivery of the same publish.
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
        handle.onCancel(feed::stop);
        safely(() -> listener.publishAccepted(job.publishId()));
        feed.readAhead();
        return handle;
    }

    /** Operator stop for one publish, by id, so a message on another delivery can stop it. */
    public boolean cancel(PublishId publishId) {
        PublishHandle handle = active.get(publishId);
        if (handle == null) return false;
        handle.cancel();
        return true;
    }

    public void start() {
        dispatcher.start();
    }

    /** Stops dispatching. Loads in flight finish and are recorded; queued work waits for the publish to be delivered again. */
    public void shutdown() {
        stopping = true;
        feeds.values().forEach(PublishFeed::stop);
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

    /** Waits for in-flight loads, then fails the outcome of every publish that still had work queued. */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        boolean drained = loads.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        active.values().forEach(handle -> handle.abandon("worker shut down before publish " + handle.publishId() + " finished"));
        return drained;
    }

    public int queued() {
        return queue.size();
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
        if (next.publish().isCancelled()) {
            drop(next, "publish cancelled");
            return;
        }
        Slot slot = capacity.acquire();
        safely(() -> listener.taskDispatched(next.task(), next.attempt()));
        try {
            loads.execute(() -> runOne(next, slot));
        } catch (RuntimeException notRunning) {
            capacity.release();
            unsettled(next, notRunning);
        }
    }

    private void runOne(QueuedTask queued, Slot slot) {
        EngagementVerifier.Result result = null;
        try {
            try {
                // A slot can be waited for over a minute, so the publish may have been cancelled since. The load
                // is what is expensive, not the slot, and nothing has been spent yet.
                result = queued.publish().isCancelled()
                        ? new Dropped("publish cancelled")
                        : verifier.verify(queued.task());
            } finally {
                releaseSlot(slot, result);
            }
            settle(queued, result);
        } catch (Throwable unexpected) {
            // Everything between the load and the settlement is a call into code this module does not own. If one
            // of them throws, the task still settles, or the publish would never complete and no redelivery could
            // repair it.
            LOG.log(System.Logger.Level.ERROR, "task " + queued.task().engagementId() + " could not be settled", unexpected);
            unsettled(queued, unexpected);
        }
    }

    /**
     * Returns the slot. A refusal or a timeout is the engagement system saying the cap is too high, so it halves
     * the cap once for the generation of dispatches this slot came from. A timeout also keeps the slot: the load
     * we stopped waiting for is still running down there, and releasing the slot now would let the worker double
     * its real concurrency against the team it is being polite to.
     */
    private void releaseSlot(Slot slot, EngagementVerifier.Result result) {
        if (!(result instanceof Failed failed) || !saysTheCapIsTooHigh(failed.kind())) {
            capacity.release();
            return;
        }
        capacity.shrink(slot).ifPresent(to -> safely(() -> listener.capacityShrunk(slot.limit(), to)));
        Duration stillRunningDownstream = failed.kind() == Kind.TIMED_OUT
                ? policy.loadBudget().minus(failed.elapsed())
                : Duration.ZERO;
        if (stillRunningDownstream.isPositive() && later(stillRunningDownstream, capacity::release)) return;
        capacity.release();
    }

    private static boolean saysTheCapIsTooHigh(DownstreamFailure.Kind kind) {
        return kind == Kind.OVER_CAPACITY || kind == Kind.TIMED_OUT;
    }

    private void settle(QueuedTask queued, EngagementVerifier.Result result) {
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
            case Failed failed -> afterFailure(queued, failed);
            case StoreFailed storeFailed -> afterStoreFailure(queued, storeFailed.cause());
        }
    }

    private void afterFailure(QueuedTask queued, Failed failed) {
        if (queued.publish().isCancelled()) {
            drop(queued, "publish cancelled");
            return;
        }
        if (failed.kind() == Kind.TERMINAL) {
            deadLetter(queued, failed.cause());
            return;
        }
        boolean downstreamIsOut = gate.recordFailure(failed.cause());
        if (downstreamIsOut || failed.kind() != Kind.TRANSIENT) {
            waitForTheDownstream(queued, failed.cause());
            return;
        }
        retryThisRow(queued, failed.cause());
    }

    /**
     * A refusal, a timeout or an outage says nothing about this engagement, so it costs the row no attempt and can
     * never dead-letter it. Dispatch is already stopped by the gate if the downstream is out; the row waits with
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
    private void retryThisRow(QueuedTask queued, Throwable cause) {
        Optional<Duration> delay = policy.retries().delayBefore(queued.attempt() + 1);
        if (delay.isEmpty()) {
            deadLetter(queued, cause);
            return;
        }
        if (later(delay.get(), () -> requeue(queued.nextAttempt()))) {
            safely(() -> listener.retryScheduled(queued.task(), queued.attempt() + 1, delay.get(), cause));
        } else {
            unsettled(queued, cause);
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
        if (stopping || retry.publish().isCancelled()) {
            drop(retry, stopping ? "worker stopping" : "publish cancelled");
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
        finish(queued, Settlement.DROPPED);
    }

    private void deadLetter(QueuedTask queued, Throwable cause) {
        VerifyTask task = queued.task();
        boolean marked;
        try {
            deadLetters.send(new DeadLetter(task, queued.attempt(), cause));
            marked = projection.recordDeadLettered(task.engagementId(), task.lastSeqAtEnqueue());
        } catch (RuntimeException notRecorded) {
            unsettled(queued, notRecorded);
            return;
        }
        if (!marked) {
            // The row moved on while we were failing: it is not dead, and the operator holding the dead letter
            // needs to know that before replaying it.
            drop(queued, "row moved on before the dead letter");
            return;
        }
        safely(() -> listener.taskDeadLettered(task, queued.attempt(), cause));
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
