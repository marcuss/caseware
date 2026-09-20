package com.caseware.fanout;

import java.util.Objects;

/**
 * One unit of work: confirm which template version one engagement is really on, on behalf of one publish.
 * {@code lastSeqAtEnqueue} is the row's sequence number when the task was made, and it is there for the operator
 * reading a dead letter, not as a guard: both of the worker's conditional writes, the verification and the
 * dead-letter mark, are guarded by the sequence the attempt itself re-read. A row that moved on while it waited
 * its turn is therefore still settled, and only a move during the load, which is a user acting on the file while
 * we were reading it, refuses the write.
 */
public record VerifyTask(PublishId publishId, FirmId firmId, EngagementId engagementId, long lastSeqAtEnqueue) {
    public VerifyTask {
        Objects.requireNonNull(publishId, "publishId");
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(engagementId, "engagementId");
    }
}
