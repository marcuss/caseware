package com.caseware.fanout;

import java.time.Duration;
import java.util.Objects;

/**
 * How the worker treats failure and how far ahead it reads.
 *
 * @param retries              the ceiling on loads charged to one row, and the backoff between them
 * @param failuresBeforePause  consecutive downstream failures before dispatch stops. A downstream in trouble is
 *                             not a reason to spend every row's retry budget on it
 * @param downstreamBackoff    how long dispatch stops for, and how long a refused or timed-out row waits before it
 *                             is tried again. Neither costs the row an attempt
 * @param loadBudget           the longest a load can take downstream. A load the adapter timed out is assumed to
 *                             still be running there for the rest of this, and its slot is held until it is over
 * @param pageSize             rows read from the projection at a time, and roughly how many tasks one publish
 *                             keeps queued
 */
public record WorkerPolicy(RetryPolicy retries, int failuresBeforePause, Duration downstreamBackoff,
                           Duration loadBudget, int pageSize) {

    public WorkerPolicy {
        Objects.requireNonNull(retries, "retries");
        if (failuresBeforePause < 1) throw new IllegalArgumentException("failuresBeforePause must be at least 1");
        if (downstreamBackoff.isNegative() || downstreamBackoff.isZero()) {
            throw new IllegalArgumentException("downstreamBackoff must be positive");
        }
        if (loadBudget.isNegative()) throw new IllegalArgumentException("loadBudget must not be negative");
        if (pageSize < 1) throw new IllegalArgumentException("pageSize must be at least 1");
    }
}
