package com.caseware.fanout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * The worker's promise about one submitted publish: the outcome arrives once every one of its engagements has
 * settled. The handle does not know how many engagements that will be when it is made, because the rows are read a
 * page at a time; it completes when the last page has been enqueued and everything enqueued has settled.
 *
 * <p>A publish ends exactly one of three ways, and {@code completed} is the flag that makes it exactly one:
 * {@link #complete} when everything settled and nothing was left unrecorded, the same path completing the outcome
 * exceptionally when something was, and {@link #fail} when the scan threw or the worker shut down first. All of
 * them run {@code onFinished}, so the worker always forgets the handle, and the last two mean the same thing to
 * the caller: do not acknowledge this delivery.
 */
public final class PublishHandle {

    enum Settlement { VERIFIED, DROPPED, ABANDONED, DEAD_LETTERED, UNRECORDED }

    private final PublishId publishId;
    private final BiConsumer<PublishHandle, PublishOutcome> onFinished;
    private final CompletableFuture<PublishOutcome> outcome = new CompletableFuture<>();
    private final Object lock = new Object();
    private int enqueued;
    private int settled;
    private int verified;
    private int dropped;
    private int abandoned;
    private int deadLettered;
    private int unrecorded;
    private boolean allEnqueued;
    private boolean completed;

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
                case ABANDONED -> abandoned++;
                case DEAD_LETTERED -> deadLettered++;
                case UNRECORDED -> unrecorded++;
            }
            settled++;
            complete = claimCompletion();
        }
        if (complete) complete();
    }

    /**
     * Ends the publish without an outcome: the rows could not be read, or the worker stopped while it still owed
     * work. Taking {@code completed} here is what stops a settlement arriving afterwards, from a load still in
     * flight or from an action the timer fires after the shutdown, from completing the publish normally and
     * telling the adapter to acknowledge a delivery whose rows are still unverified.
     */
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
            return new PublishOutcome(verified, dropped, abandoned, deadLettered);
        }
    }
}
