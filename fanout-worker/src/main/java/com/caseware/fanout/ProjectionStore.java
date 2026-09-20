package com.caseware.fanout;

import java.util.List;
import java.util.Optional;

/**
 * The per-region engagement projection (design document, section 1). This is where the worker's idempotency and
 * restart state lives: a row that is {@code VERIFIED} or {@code DEAD_LETTERED} needs no further work, whichever
 * process did it. Every write names the sequence number the worker saw, and the store refuses the write if the row
 * has moved on since, so a load that raced a live user can never overwrite the newer fact.
 */
public interface ProjectionStore {

    /** The rows of this template still marked {@code UNVERIFIED}: the work a publish leaves to the worker. */
    List<ProjectionRow> unverifiedRows(String templateId);

    /** The row as it is now, or empty once the engagement has been archived. */
    Optional<ProjectionRow> read(EngagementId engagementId);

    /**
     * Records the confirmed base version and marks the row {@code VERIFIED}. Lands only if the row is still
     * {@code UNVERIFIED} at {@code expectedSeq}; otherwise writes nothing and returns false.
     */
    boolean recordVerified(EngagementId engagementId, String baseVersion, long expectedSeq);

    /** Marks the row {@code DEAD_LETTERED} under the same condition as {@link #recordVerified}. */
    boolean recordDeadLettered(EngagementId engagementId, long expectedSeq);
}
