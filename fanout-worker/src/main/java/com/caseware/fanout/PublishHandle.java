package com.caseware.fanout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * The worker's promise about one submitted publish: the outcome arrives once every one of its engagements has
 * settled, and until then the publish can be cancelled. A worker that shuts down first fails the outcome instead,
 * which tells the caller to keep the publish for delivery to the next process.
 */
public final class PublishHandle {

    enum Settlement { VERIFIED, DROPPED, DEAD_LETTERED }

    private final PublishId publishId;
    private final int total;
    private final BiConsumer<PublishHandle, PublishOutcome> onFinished;
    private final AtomicInteger verified = new AtomicInteger();
    private final AtomicInteger dropped = new AtomicInteger();
    private final AtomicInteger deadLettered = new AtomicInteger();
    private final AtomicInteger settled = new AtomicInteger();
    private final CompletableFuture<PublishOutcome> outcome = new CompletableFuture<>();
    private volatile boolean cancelled;

    /** {@code onFinished} runs before the outcome becomes visible, so the worker can forget the handle first. */
    PublishHandle(PublishId publishId, int total, BiConsumer<PublishHandle, PublishOutcome> onFinished) {
        this.publishId = publishId;
        this.total = total;
        this.onFinished = onFinished;
        if (total == 0) complete();
    }

    public PublishId publishId() {
        return publishId;
    }

    public CompletionStage<PublishOutcome> outcome() {
        return outcome;
    }

    /** Stops spending capacity on this publish. Queued tasks are dropped; loads already running finish and are recorded. */
    public void cancel() {
        cancelled = true;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    void settle(Settlement how) {
        switch (how) {
            case VERIFIED -> verified.incrementAndGet();
            case DROPPED -> dropped.incrementAndGet();
            case DEAD_LETTERED -> deadLettered.incrementAndGet();
        }
        if (settled.incrementAndGet() == total) complete();
    }

    void abandon(String why) {
        outcome.completeExceptionally(new IllegalStateException(why));
    }

    private void complete() {
        PublishOutcome result = new PublishOutcome(verified.get(), dropped.get(), deadLettered.get(), cancelled);
        onFinished.accept(this, result);
        outcome.complete(result);
    }
}
