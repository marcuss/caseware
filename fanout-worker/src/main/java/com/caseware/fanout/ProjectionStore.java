package com.caseware.fanout;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-region engagement projection (design document, section 1). This is where the worker's idempotency and
 * restart state lives: a row that is {@code VERIFIED} or {@code DEAD_LETTERED} needs no further work, whichever
 * process did it. Every write names the sequence number the worker saw, and the store refuses the write if the row
 * has moved on since, so a load that raced a live user can never overwrite the newer fact.
 *
 * <p>The store owns the recompute of the user-visible fields. Section 1 has two writers, this worker and the hook
 * consumer, and both derive {@code pendingTarget} and {@code state} from the row's base version, its decision log
 * and the lineage graph, under the lineage guard described there. The worker contributes the base version and
 * nothing else: it never computes what the user sees, and it holds no lineage of its own.
 */
public interface ProjectionStore {

    /**
     * One page of the rows of this template still marked {@code UNVERIFIED}: the work a publish leaves to the
     * worker. A publish can cover 20,000 rows and the backfill 800,000, so this is paged rather than a single
     * list: no caller may be asked to hold the whole scan, and no delivery thread may be asked to wait for it.
     *
     * @param after the cursor from the previous page, empty for the first page
     * @param limit the most rows this page may contain
     */
    Page unverifiedRows(String templateId, Optional<String> after, int limit);

    /** A page of rows, with the cursor that continues after them. An empty cursor means the scan is complete. */
    record Page(List<ProjectionRow> rows, Optional<String> nextCursor) {
        public Page {
            rows = List.copyOf(rows);
            Objects.requireNonNull(nextCursor, "nextCursor");
        }

        /** The final page: these rows, and nothing after them. */
        public static Page last(List<ProjectionRow> rows) {
            return new Page(rows, Optional.empty());
        }
    }

    /** The row as it is now, or empty once the engagement has been archived. */
    Optional<ProjectionRow> read(EngagementId engagementId);

    /**
     * Records the confirmed base version and marks the row {@code VERIFIED}, recomputing the user-visible fields
     * as described above. Lands only if the row is still {@code UNVERIFIED} at {@code expectedSeq}; otherwise
     * writes nothing and returns false.
     */
    boolean recordVerified(EngagementId engagementId, String baseVersion, long expectedSeq);

    /** Marks the row {@code DEAD_LETTERED} under the same condition as {@link #recordVerified}. */
    boolean recordDeadLettered(EngagementId engagementId, long expectedSeq);
}
