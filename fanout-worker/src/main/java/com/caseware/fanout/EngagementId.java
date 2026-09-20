package com.caseware.fanout;

/** Identifies an engagement file. Loading one costs about a minute of the engagement team's capacity. */
public record EngagementId(String value) {
    public EngagementId {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("engagement id is blank");
    }

    @Override
    public String toString() {
        return value;
    }
}
