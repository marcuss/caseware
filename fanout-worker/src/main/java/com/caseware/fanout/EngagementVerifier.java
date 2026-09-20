package com.caseware.fanout;

import java.time.Duration;
import java.util.Optional;

/**
 * One attempt at one task: re-read the row, load the engagement only if the row still needs it, and write the
 * answer back under the sequence guard. Never throws; every way an attempt can end is a {@link Result}.
 */
final class EngagementVerifier {

    sealed interface Result permits Verified, Dropped, Failed {}

    record Verified(Duration loadTime) implements Result {}

    record Dropped(String reason) implements Result {}

    record Failed(DownstreamFailure.Kind kind, Throwable cause) implements Result {}

    private final ProjectionStore projection;
    private final EngagementSystem engagements;

    EngagementVerifier(ProjectionStore projection, EngagementSystem engagements) {
        this.projection = projection;
        this.engagements = engagements;
    }

    Result verify(VerifyTask task) {
        try {
            return attempt(task);
        } catch (DownstreamFailure failure) {
            return new Failed(failure.kind(), failure);
        } catch (RuntimeException unexpected) {
            return new Failed(DownstreamFailure.Kind.TERMINAL, unexpected);
        }
    }

    private Result attempt(VerifyTask task) throws DownstreamFailure {
        Optional<ProjectionRow> row = projection.read(task.engagementId());
        if (row.isEmpty()) return new Dropped("engagement archived");
        if (row.get().verification() != ProjectionRow.Verification.UNVERIFIED) {
            return new Dropped("row already " + row.get().verification());
        }
        if (row.get().seq() != task.lastSeqAtEnqueue()) return new Dropped("row moved on before the load");

        long started = System.nanoTime();
        String version = engagements.loadEffectiveVersion(task.engagementId());
        Duration loadTime = Duration.ofNanos(System.nanoTime() - started);

        boolean landed = projection.recordVerified(task.engagementId(), version, task.lastSeqAtEnqueue());
        return landed ? new Verified(loadTime) : new Dropped("row moved on during the load");
    }
}
