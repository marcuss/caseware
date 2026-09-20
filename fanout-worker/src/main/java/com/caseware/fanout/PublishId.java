package com.caseware.fanout;

/** Identifies one template publish. With an {@link EngagementId} it forms the key of one unit of work. */
public record PublishId(String value) {
    public PublishId {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("publish id is blank");
    }

    @Override
    public String toString() {
        return value;
    }
}
