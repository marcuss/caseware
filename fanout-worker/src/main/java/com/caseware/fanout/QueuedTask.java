package com.caseware.fanout;

/**
 * A task waiting for its turn, with the publish it settles into and two counters. {@code attempt} counts the loads
 * charged to this row, which is what the retry ceiling bounds; {@code storeAttempt} counts tries lost to the
 * worker's own projection store. A wait for the downstream to recover spends neither.
 */
record QueuedTask(PublishHandle publish, VerifyTask task, int attempt, int storeAttempt) {

    static QueuedTask first(PublishHandle publish, VerifyTask task) {
        return new QueuedTask(publish, task, 1, 1);
    }

    QueuedTask nextAttempt() {
        return new QueuedTask(publish, task, attempt + 1, storeAttempt);
    }

    QueuedTask nextStoreAttempt() {
        return new QueuedTask(publish, task, attempt, storeAttempt + 1);
    }
}
