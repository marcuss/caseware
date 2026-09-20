package com.caseware.fanout;

import java.time.Duration;
import java.util.Optional;

/**
 * One attempt at one task: re-read the row, load the engagement only if the row still needs it, and write the
 * answer back under the sequence guard. Never throws; every way an attempt can end is a {@link Result}, and the
 * cases are separated by who failed, because the worker owes the engagement system and its own store different
 * treatment.
 */
final class EngagementVerifier {

    sealed interface Result permits Verified, Dropped, Superseded, Failed, StoreFailed {}

    /** The base version was loaded and written back. */
    record Verified(Duration loadTime) implements Result {}

    /** The work is no longer needed: the engagement is archived, or someone else settled the row. */
    record Dropped(String reason) implements Result {}

    /** The row still needs verifying, but a user event landed during the load, so this answer is out of date. */
    record Superseded(String reason) implements Result {}

    /**
     * The engagement system failed, classified by what that says about trying again. {@code seqAtLoad} is the
     * sequence this attempt read before the load, so whatever the worker writes about the failure is guarded by
     * the same value the verification write would have been.
     */
    record Failed(DownstreamFailure.Kind kind, Duration elapsed, long seqAtLoad, Throwable cause) implements Result {}

    /** The worker's own projection store failed. Never the engagement's fault, so never the engagement's penalty. */
    record StoreFailed(Throwable cause) implements Result {}

    private final ProjectionStore projection;
    private final EngagementSystem engagements;

    EngagementVerifier(ProjectionStore projection, EngagementSystem engagements) {
        this.projection = projection;
        this.engagements = engagements;
    }

    Result verify(VerifyTask task) {
        Optional<ProjectionRow> row;
        try {
            row = projection.read(task.engagementId());
        } catch (RuntimeException storeFailed) {
            return new StoreFailed(storeFailed);
        }
        if (row.isEmpty()) return new Dropped("engagement archived");
        if (row.get().verification() != ProjectionRow.Verification.UNVERIFIED) {
            return new Dropped("row already " + row.get().verification());
        }
        // The seq this read saw, not the one at enqueue: a row that moved on while it waited its turn still needs
        // the load, and every write about this attempt, the verification below and the dead-letter mark alike, is
        // guarded by this same fresh seq, so a user who acts during the load still wins.
        long expectedSeq = row.get().seq();

        long started = System.nanoTime();
        String version;
        try {
            version = engagements.loadEffectiveVersion(task.engagementId());
        } catch (DownstreamFailure failure) {
            return new Failed(failure.kind(), since(started), expectedSeq, failure);
        } catch (RuntimeException unclassified) {
            // The adapter broke its contract by not classifying this. Treat it as retryable: the alternative is to
            // dead-letter a healthy engagement over a bug on our side of the call.
            return new Failed(DownstreamFailure.Kind.TRANSIENT, since(started), expectedSeq,
                    new DownstreamFailure(DownstreamFailure.Kind.TRANSIENT, "unclassified failure from the engagement system", unclassified));
        }
        Duration loadTime = since(started);

        boolean landed;
        try {
            landed = projection.recordVerified(task.engagementId(), version, expectedSeq);
        } catch (RuntimeException storeFailed) {
            return new StoreFailed(storeFailed);
        }
        return landed ? new Verified(loadTime) : new Superseded("row moved on during the load");
    }

    private static Duration since(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
    }
}
