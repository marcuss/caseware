package com.caseware.fanout;

import java.util.Objects;

/** The facts about one engagement's projection row that the worker acts on. */
public record ProjectionRow(EngagementId engagementId, FirmId firmId, long seq, Verification verification) {

    /** Whether the row's base version has been confirmed against the engagement itself. */
    public enum Verification {
        /** Seeded from a listing, or a publish found it that way. Shown to users as "checking". */
        UNVERIFIED,
        /** Confirmed by a load, or by the engagement system's own events. */
        VERIFIED,
        /** The worker gave up and an operator holds the dead letter. The next event on the engagement resets it. */
        DEAD_LETTERED
    }

    public ProjectionRow {
        Objects.requireNonNull(engagementId, "engagementId");
        Objects.requireNonNull(firmId, "firmId");
        Objects.requireNonNull(verification, "verification");
    }
}
