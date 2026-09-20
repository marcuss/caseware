package com.caseware.fanout;

/** The engagement management system, owned by another team with limited capacity. */
public interface EngagementSystem {

    /**
     * Loads the engagement and returns the template version it is really on. Takes about a minute and occupies one
     * unit of that team's capacity for the whole of it, so the worker calls this only for a row that still needs it.
     *
     * @throws DownstreamFailure classified so the caller knows whether trying again can help
     */
    String loadEffectiveVersion(EngagementId engagementId) throws DownstreamFailure;
}
