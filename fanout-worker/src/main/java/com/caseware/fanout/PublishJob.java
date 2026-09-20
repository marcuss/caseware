package com.caseware.fanout;

import java.util.Objects;

/**
 * A template publish as the worker receives it. The job names the template, not the engagements: the worker asks
 * the projection for the rows this publish could not settle without loading them. Delivering the same job twice
 * is safe and expected.
 */
public record PublishJob(PublishId publishId, String templateId) {
    public PublishJob {
        Objects.requireNonNull(publishId, "publishId");
        if (templateId == null || templateId.isBlank()) throw new IllegalArgumentException("template id is blank");
    }
}
