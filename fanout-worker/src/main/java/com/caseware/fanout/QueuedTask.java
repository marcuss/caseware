package com.caseware.fanout;

/** A task waiting for its turn, together with the publish it settles into and which attempt this is. */
record QueuedTask(PublishHandle publish, VerifyTask task, int attempt) {

    QueuedTask nextAttempt() {
        return new QueuedTask(publish, task, attempt + 1);
    }
}
