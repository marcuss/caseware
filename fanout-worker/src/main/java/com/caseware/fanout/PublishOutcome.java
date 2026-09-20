package com.caseware.fanout;

/**
 * How one submission of a publish ended. The counts cover that submission's tasks only: a resubmission after a
 * restart counts what was left, not what an earlier process already did.
 */
public record PublishOutcome(int verified, int dropped, int deadLettered, boolean cancelled) {

    public int total() {
        return verified + dropped + deadLettered;
    }
}
