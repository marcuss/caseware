package com.caseware.fanout;

import java.util.Objects;

/**
 * One unit of work: confirm which template version one engagement is really on, on behalf of one publish.
 * {@code lastSeqAtEnqueue} is the row's sequence number when the task was made. A row that has moved past it, because
 * the engagement system reported an event in the meantime, is left alone: the newer fact wins over the older load.
 */
public record VerifyTask(PublishId publishId, FirmId firmId, EngagementId engagementId, long lastSeqAtEnqueue) {
    public VerifyTask {
        Objects.requireNonNull(publishId, "publishId");
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(engagementId, "engagementId");
    }
}
