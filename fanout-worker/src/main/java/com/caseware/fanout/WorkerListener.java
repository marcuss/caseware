package com.caseware.fanout;

import java.time.Duration;

/**
 * The hooks an on-call engineer wants: how far each publish is, how long loads take, why work was dropped or given
 * up, and when the cap moved. Each hook fires after the fact it reports has happened, so a dashboard never shows a
 * retry that is not yet on the timer or a verification that is not yet written. Every method has an empty default.
 */
public interface WorkerListener {

    WorkerListener NONE = new WorkerListener() {};

    default void publishAccepted(PublishId publishId, int engagements) {}

    default void publishFinished(PublishId publishId, PublishOutcome outcome) {}

    default void taskDispatched(VerifyTask task, int attempt) {}

    default void taskVerified(VerifyTask task, Duration loadTime) {}

    /** The row was left alone: it was archived, already settled by someone else, moved on, or its publish was cancelled. */
    default void taskDropped(VerifyTask task, String reason) {}

    default void retryScheduled(VerifyTask task, int nextAttempt, Duration delay, Throwable cause) {}

    default void taskDeadLettered(VerifyTask task, int attempts, Throwable cause) {}

    default void capacityShrunk(int from, int to) {}
}
