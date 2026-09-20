package com.caseware.fanout;

import java.time.Duration;
import java.util.Optional;

/**
 * Exponential backoff with a ceiling on attempts. Attempt 1 is the first try and waits for nothing; attempt 2 waits
 * {@code firstDelay}, and each attempt after that waits twice the previous delay, up to {@code maxDelay}.
 */
public record RetryPolicy(int maxAttempts, Duration firstDelay, Duration maxDelay) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be at least 1");
        if (firstDelay.isNegative() || firstDelay.isZero()) throw new IllegalArgumentException("firstDelay must be positive");
        if (maxDelay.compareTo(firstDelay) < 0) throw new IllegalArgumentException("maxDelay is below firstDelay");
    }

    /** The wait before {@code attempt}, or empty when the ceiling is reached and the task must be given up. */
    public Optional<Duration> delayBefore(int attempt) {
        if (attempt < 2) throw new IllegalArgumentException("only attempts after the first wait");
        if (attempt > maxAttempts) return Optional.empty();
        int doublings = Math.min(attempt - 2, 30);
        Duration delay = firstDelay.multipliedBy(1L << doublings);
        return Optional.of(delay.compareTo(maxDelay) < 0 ? delay : maxDelay);
    }
}
