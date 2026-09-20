package com.caseware.fanout;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Turns one publish's unverified rows into tasks, a page at a time. A publish covers 20,000 rows and the backfill
 * 800,000, so the worker holds about a page of them rather than all of them, and the thread that delivered the
 * publish waits for one page rather than a full cross-shard scan. The next page is read when the publish has
 * fewer than a page of tasks still waiting, which happens as tasks settle.
 */
final class PublishFeed {

    private final String templateId;
    private final PublishHandle handle;
    private final ProjectionStore projection;
    private final int pageSize;
    private final Consumer<VerifyTask> offer;
    private final Consumer<Throwable> onFailure;
    private final ReentrantLock lock = new ReentrantLock();
    private Optional<String> cursor = Optional.empty();
    private boolean reading;
    private boolean done;

    PublishFeed(String templateId, PublishHandle handle, ProjectionStore projection, int pageSize,
                Consumer<VerifyTask> offer, Consumer<Throwable> onFailure) {
        this.templateId = templateId;
        this.handle = handle;
        this.projection = projection;
        this.pageSize = pageSize;
        this.offer = offer;
        this.onFailure = onFailure;
    }

    /** Reads pages until the publish has a page of work waiting, the rows run out, or the publish is cancelled. */
    void readAhead() {
        lock.lock();
        try {
            if (reading) return;
            reading = true;
            try {
                while (!done && !handle.isCancelled() && handle.outstanding() < pageSize) readPage();
                if (handle.isCancelled()) finish();
            } catch (RuntimeException storeFailed) {
                // Not finish(): the publish is not complete, it is failing. Completing it here would tell the
                // adapter to acknowledge a delivery whose rows were never read.
                done = true;
                onFailure.accept(storeFailed);
            } finally {
                reading = false;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Stops reading: the publish is cancelled or the worker is stopping. What is queued still settles. */
    void stop() {
        lock.lock();
        try {
            finish();
        } finally {
            lock.unlock();
        }
    }

    private void readPage() {
        ProjectionStore.Page page = projection.unverifiedRows(templateId, cursor, pageSize);
        cursor = page.nextCursor();
        handle.expect(page.rows().size());
        for (ProjectionRow row : page.rows()) {
            offer.accept(new VerifyTask(handle.publishId(), row.firmId(), row.engagementId(), row.seq()));
        }
        if (cursor.isEmpty()) finish();
    }

    private void finish() {
        if (done) return;
        done = true;
        handle.enumerated();
    }
}
