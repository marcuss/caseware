package com.caseware.fanout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * The worker's promise about one submitted publish: the outcome arrives once every one of its engagements has
 * settled, and until then the publish can be cancelled. The handle does not know how many engagements that will be
 * when it is made, because the rows are read a page at a time; it completes when the last page has been enqueued
 * and everything enqueued has settled.
 *
 * <p>The outcome fails rather than completes when the worker shut down first, or when it could not record what
 * happened to a row. Both mean the same thing to the caller: do not acknowledge this delivery.
 */
public final class PublishHandle {

    enum Settlement { VERIFIED, DROPPED, DEAD_LETTERED, UNRECORDED }

    private final PublishId publishId;
    private final BiConsumer<PublishHandle, PublishOutcome> onFinished;
    private final CompletableFuture<PublishOutcome> outcome = new CompletableFuture<>();
    private final Object lock = new Object();
    private Runnable onCancel = () -> {};
    private int enqueued;
    private int settled;
    private int verified;
    private int dropped;
    private int deadLettered;
    private int unrecorded;
    private boolean allEnqueued;
    private boolean completed;
    private volatile boolean cancelled;

    /** {@code onFinished} runs before the outcome becomes visible, so the worker can forget the handle first. */
    PublishHandle(PublishId publishId, BiConsumer<PublishHandle, PublishOutcome> onFinished) {
        this.publishId = publishId;
        this.onFinished = onFinished;
    }

    public PublishId publishId() {
        return publishId;
    }

    public CompletionStage<PublishOutcome> outcome() {
        return outcome;
    }

    /**
     * Operator stop for this publish: queued rows are dropped and no more are read, loads already running finish
     * and are recorded. A withdrawal is not a reason to call this. The base version this worker establishes is
     * the same whichever version was withdrawn, and the retarget the design does on a withdrawal needs it known.
     */
    public void cancel() {
        synchronized (lock) {
            if (cancelled) return;
            cancelled = true;
        }
        onCancel.run();
    }

    public boolean isCancelled() {
        return cancelled;
    }

    /** Runs when the publish is cancelled, so the worker can stop reading pages for it. */
    void onCancel(Runnable action) {
        synchronized (lock) {
            onCancel = action;
        }
        if (cancelled) action.run();
    }

    /** A page of tasks has been enqueued for this publish. */
    void expect(int tasks) {
        synchronized (lock) {
            enqueued += tasks;
        }
    }

    /** The last page has been enqueued: nothing more will be added to this publish. */
    void enumerated() {
        boolean complete;
        synchronized (lock) {
            allEnqueued = true;
            complete = claimCompletion();
        }
        if (complete) complete();
    }

    /** Tasks enqueued for this publish that have not settled yet, which is what bounds how much is read ahead. */
    int outstanding() {
        synchronized (lock) {
            return enqueued - settled;
        }
    }

    void settle(Settlement how) {
        boolean complete;
        synchronized (lock) {
            switch (how) {
                case VERIFIED -> verified++;
                case DROPPED -> dropped++;
                case DEAD_LETTERED -> deadLettered++;
                case UNRECORDED -> unrecorded++;
            }
            settled++;
            complete = claimCompletion();
        }
        if (complete) complete();
    }

    /** Ends the publish without an outcome: the worker stopped, or could not read the rows it still owed. */
    void abandon(String why) {
        outcome.completeExceptionally(new IllegalStateException(why));
    }

    void fail(Throwable cause) {
        boolean first;
        synchronized (lock) {
            first = !completed;
            completed = true;
        }
        if (first) onFinished.accept(this, snapshot());
        outcome.completeExceptionally(cause);
    }

    private boolean claimCompletion() {
        if (completed || !allEnqueued || settled != enqueued) return false;
        completed = true;
        return true;
    }

    private void complete() {
        PublishOutcome result = snapshot();
        boolean recorded;
        synchronized (lock) {
            recorded = unrecorded == 0;
        }
        onFinished.accept(this, result);
        if (recorded) outcome.complete(result);
        else outcome.completeExceptionally(new IllegalStateException(
                "publish " + publishId + " could not record the outcome of every engagement"));
    }

    private PublishOutcome snapshot() {
        synchronized (lock) {
            return new PublishOutcome(verified, dropped, deadLettered, cancelled);
        }
    }
}
