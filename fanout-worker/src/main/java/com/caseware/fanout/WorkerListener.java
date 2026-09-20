package com.caseware.fanout;

import java.time.Duration;

/**
 * The hooks an on-call engineer wants: how far each publish is, how long loads take, why work was dropped, given
 * up or put back, and when the worker backed off from the engagement system. Each hook fires after the fact it
 * reports has happened, so a dashboard never shows a retry that is not yet on the timer or a verification that is
 * not yet written. Every method has an empty default, and a hook that throws is logged and ignored: observability
 * may not take down the work it observes.
 */
public interface WorkerListener {

    default void publishAccepted(PublishId publishId) {}

    default void publishFinished(PublishId publishId, PublishOutcome outcome) {}

    default void taskDispatched(VerifyTask task, int attempt) {}

    default void taskVerified(VerifyTask task, Duration loadTime) {}

    /** The row needed nothing: it was archived, or another publish had already claimed it. */
    default void taskDropped(VerifyTask task, String reason) {}

    /**
     * The row is still unverified and this worker stopped trying. Nothing is scheduled to pick it up before the
     * next publish of that template, so this is the count the design's "unverified under 1%" objective is
     * measured against. Usually there is nothing for an operator to replay; the one exception is a dead letter
     * whose mark the row refused, and the reason says so.
     */
    default void taskAbandoned(VerifyTask task, String reason) {}

    /** The task went back on the queue without being charged an attempt: the downstream or the store was at fault. */
    default void taskRequeued(VerifyTask task, Duration delay, String reason) {}

    /** A load charged to this row failed and will be tried again. */
    default void retryScheduled(VerifyTask task, int nextAttempt, Duration delay, Throwable cause) {}

    default void taskDeadLettered(VerifyTask task, int attempts, Throwable cause) {}

    /** The worker could not record what happened to this row, so the publish will fail rather than complete. */
    default void taskUnsettled(VerifyTask task, Throwable cause) {}

    default void capacityShrunk(int from, int to) {}

    /** Dispatch has stopped: the engagement system is failing every load. Nothing is charged to any row meanwhile. */
    default void downstreamPaused(Duration pause, Throwable cause) {}

    default void downstreamResumed() {}

    /** The dispatch loop hit something unexpected. It keeps running; this is the page. */
    default void dispatcherFailed(Throwable cause) {}
}
