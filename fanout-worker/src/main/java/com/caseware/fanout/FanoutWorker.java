package com.caseware.fanout;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.caseware.fanout.DownstreamFailure.Kind;
import com.caseware.fanout.EngagementVerifier.Dropped;
import com.caseware.fanout.EngagementVerifier.Failed;
import com.caseware.fanout.EngagementVerifier.Verified;
import com.caseware.fanout.PublishHandle.Settlement;

/**
 * The template-publish fan-out worker: the verifier of the design document. A publish becomes one task per
 * unverified row; tasks are dispatched round-robin across firms, one downstream load per slot of the shared
 * {@link DownstreamCapacity}, and each settles exactly once as verified, dropped or dead-lettered.
 *
 * <p>Promises: the same publish submitted twice runs once; a load is never made for a row that no longer needs it;
 * a failure that retrying cannot fix goes to the dead-letter queue at once, and a retryable one is retried with
 * backoff up to the policy's ceiling without holding up any other engagement; shutdown lets in-flight loads finish
 * and leaves queued work to the next delivery of the same publish.
 */
public final class FanoutWorker {

    private final ProjectionStore projection;
    private final DeadLetterQueue deadLetters;
    private final DownstreamCapacity capacity;
    private final RetryPolicy retries;
    private final Timer timer;
    private final WorkerListener listener;
    private final EngagementVerifier verifier;
    private final FairQueue queue = new FairQueue();
    private final ConcurrentMap<PublishId, PublishHandle> active = new ConcurrentHashMap<>();
    private final ExecutorService loads = Executors.newVirtualThreadPerTaskExecutor();
    private final Thread dispatcher = Thread.ofPlatform().name("fanout-dispatcher").daemon(true).unstarted(this::dispatch);
    private volatile boolean stopping;

    public FanoutWorker(ProjectionStore projection, EngagementSystem engagements, DeadLetterQueue deadLetters,
                        DownstreamCapacity capacity, RetryPolicy retries, Timer timer, WorkerListener listener) {
        this.projection = Objects.requireNonNull(projection, "projection");
        this.deadLetters = Objects.requireNonNull(deadLetters, "deadLetters");
        this.capacity = Objects.requireNonNull(capacity, "capacity");
        this.retries = Objects.requireNonNull(retries, "retries");
        this.timer = Objects.requireNonNull(timer, "timer");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.verifier = new EngagementVerifier(projection, Objects.requireNonNull(engagements, "engagements"));
    }

    /**
     * Accepts a publish. If the same publish is already running, returns its handle and enqueues nothing. Otherwise
     * the template's unverified rows become tasks; rows another process already settled are not among them, which
     * is what makes a resubmission after a restart resume rather than repeat.
     */
    public PublishHandle submit(PublishJob job) {
        if (stopping) throw new IllegalStateException("worker is shutting down");
        PublishHandle running = active.get(job.publishId());
        if (running != null) return running;

        List<VerifyTask> tasks = projection.unverifiedRows(job.templateId()).stream()
                .map(row -> new VerifyTask(job.publishId(), row.firmId(), row.engagementId(), row.seq()))
                .toList();
        PublishHandle handle = new PublishHandle(job.publishId(), tasks.size(), this::finished);
        if (tasks.isEmpty()) return handle;
        PublishHandle winner = active.putIfAbsent(job.publishId(), handle);
        if (winner != null) return winner;

        listener.publishAccepted(job.publishId(), tasks.size());
        tasks.forEach(task -> queue.offer(new QueuedTask(handle, task, 1)));
        return handle;
    }

    public void start() {
        dispatcher.start();
    }

    /** Stops dispatching. Loads in flight finish and are recorded; queued work waits for the publish to be delivered again. */
    public void shutdown() {
        stopping = true;
        dispatcher.interrupt();
        try {
            dispatcher.join();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        loads.shutdown();
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
        try {
            while (!stopping) {
                QueuedTask next = queue.take();
                int limitAtDispatch = capacity.acquire();
                if (next.publish().isCancelled()) {
                    capacity.release();
                    drop(next, "publish cancelled");
                    continue;
                }
                listener.taskDispatched(next.task(), next.attempt());
                loads.execute(() -> runOne(next, limitAtDispatch));
            }
        } catch (InterruptedException stop) {
            Thread.currentThread().interrupt();
        }
    }

    private void runOne(QueuedTask queued, int limitAtDispatch) {
        EngagementVerifier.Result result = verifier.verify(queued.task());
        if (result instanceof Failed failed && failed.kind() == Kind.OVER_CAPACITY) {
            capacity.releaseRefused(limitAtDispatch).ifPresent(to -> listener.capacityShrunk(limitAtDispatch, to));
        } else {
            capacity.release();
        }
        switch (result) {
            case Verified verified -> {
                listener.taskVerified(queued.task(), verified.loadTime());
                queued.publish().settle(Settlement.VERIFIED);
            }
            case Dropped dropped -> drop(queued, dropped.reason());
            case Failed failed -> settleFailure(queued, failed);
        }
    }

    private void settleFailure(QueuedTask queued, Failed failed) {
        if (queued.publish().isCancelled()) {
            drop(queued, "publish cancelled");
            return;
        }
        Optional<Duration> delay = failed.kind() == Kind.TERMINAL
                ? Optional.empty()
                : retries.delayBefore(queued.attempt() + 1);
        if (delay.isEmpty()) {
            deadLetter(queued, failed.cause());
            return;
        }
        timer.after(delay.get(), () -> requeue(queued.nextAttempt()));
        listener.retryScheduled(queued.task(), queued.attempt() + 1, delay.get(), failed.cause());
    }

    private void requeue(QueuedTask retry) {
        if (!stopping) queue.offer(retry);
    }

    private void drop(QueuedTask queued, String reason) {
        listener.taskDropped(queued.task(), reason);
        queued.publish().settle(Settlement.DROPPED);
    }

    private void deadLetter(QueuedTask queued, Throwable cause) {
        deadLetters.send(new DeadLetter(queued.task(), queued.attempt(), cause));
        projection.recordDeadLettered(queued.task().engagementId(), queued.task().lastSeqAtEnqueue());
        listener.taskDeadLettered(queued.task(), queued.attempt(), cause);
        queued.publish().settle(Settlement.DEAD_LETTERED);
    }

    private void finished(PublishHandle handle, PublishOutcome outcome) {
        active.remove(handle.publishId(), handle);
        listener.publishFinished(handle.publishId(), outcome);
    }
}
