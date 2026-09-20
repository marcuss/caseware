package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(5));

    @Test
    void delaysDoubleUntilTheMaximum() {
        assertEquals(Optional.of(Duration.ofSeconds(1)), policy.delayBefore(2));
        assertEquals(Optional.of(Duration.ofSeconds(2)), policy.delayBefore(3));
        assertEquals(Optional.of(Duration.ofSeconds(4)), policy.delayBefore(4));
        assertEquals(Optional.of(Duration.ofSeconds(5)), policy.delayBefore(5));
    }

    @Test
    void theCeilingEndsRetries() {
        assertTrue(policy.delayBefore(6).isEmpty());
        assertTrue(new RetryPolicy(1, Duration.ofSeconds(1), Duration.ofSeconds(1)).delayBefore(2).isEmpty());
    }

    @Test
    void theFirstAttemptNeverWaits() {
        assertThrows(IllegalArgumentException.class, () -> policy.delayBefore(1));
    }
}
