package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class DownstreamCapacityTest {

    @Test
    void aRefusedSlotHalvesTheLimitOnceForABurstSeenAtTheSameLimit() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(8);
        capacity.acquire();
        capacity.acquire();

        assertEquals(OptionalInt.of(4), capacity.releaseRefused(8));
        assertEquals(OptionalInt.empty(), capacity.releaseRefused(8), "a second refusal from the same window is ignored");
        assertEquals(4, capacity.limit());
        assertEquals(0, capacity.inFlight(), "both refused slots were returned");
    }

    @Test
    void shrinkingStopsAtOneSlot() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(2);
        capacity.acquire();
        capacity.acquire();

        assertEquals(OptionalInt.of(1), capacity.releaseRefused(2));
        assertEquals(OptionalInt.empty(), capacity.releaseRefused(1));
        assertEquals(1, capacity.limit());
    }

    @Test
    void slotsAreCountedAndTheOperatorCanResize() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(2);

        assertEquals(2, capacity.acquire());
        assertEquals(2, capacity.acquire());
        assertEquals(2, capacity.inFlight());

        capacity.setLimit(3);
        assertEquals(3, capacity.acquire(), "a raised limit frees a slot at once");
        capacity.release();
        capacity.release();
        capacity.release();
        assertEquals(0, capacity.inFlight());
        assertEquals(3, capacity.limit());
    }

    @Test
    void aLimitBelowOneIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new DownstreamCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> new DownstreamCapacity(1).setLimit(0));
    }
}
