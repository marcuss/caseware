package com.caseware.fanout;

import java.util.Objects;

/**
 * One unit of work: confirm which template version one engagement is really on, on behalf of one publish.
 * {@code lastSeqAtEnqueue} is the row's sequence number when the task was made. It is what an operator reads in a
 * dead letter, and what guards the dead-letter mark; the verification write itself is guarded by the sequence the
 * attempt re-read, so a row that moved on while it waited its turn is still verified rather than abandoned.
 */
public record VerifyTask(PublishId publishId, FirmId firmId, EngagementId engagementId, long lastSeqAtEnqueue) {
    public VerifyTask {
        Objects.requireNonNull(publishId, "publishId");
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(engagementId, "engagementId");
    }
}
