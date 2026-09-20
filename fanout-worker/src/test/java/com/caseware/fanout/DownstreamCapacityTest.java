package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import com.caseware.fanout.DownstreamCapacity.Slot;

class DownstreamCapacityTest {

    @Test
    void aBurstOfRefusalsFromOneGenerationOfDispatchesHalvesTheLimitOnce() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(8);
        Slot first = capacity.acquire();
        Slot second = capacity.acquire();

        assertEquals(OptionalInt.of(4), capacity.shrink(first));
        assertEquals(OptionalInt.empty(), capacity.shrink(second), "a slot from the generation just ended changes nothing");
        assertEquals(4, capacity.limit());

        capacity.release();
        capacity.release();
        assertEquals(0, capacity.inFlight(), "both refused slots were returned");
    }

    @Test
    void aRefusalFromTheNextGenerationHalvesAgain() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(8);
        Slot old = capacity.acquire();
        capacity.shrink(old);
        capacity.release();

        Slot fresh = capacity.acquire();
        assertEquals(OptionalInt.of(2), capacity.shrink(fresh), "a load dispatched at the new limit and refused says it is still too high");
    }

    @Test
    void anOperatorResizeStartsAFreshGeneration() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(8);
        Slot dispatched = capacity.acquire();

        capacity.setLimit(8);
        assertEquals(OptionalInt.empty(), capacity.shrink(dispatched), "a straggler refusal never re-halves what the operator just set");
        assertEquals(8, capacity.limit());
    }

    @Test
    void shrinkingStopsAtOneSlot() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(2);
        Slot first = capacity.acquire();
        capacity.release();

        assertEquals(OptionalInt.of(1), capacity.shrink(first));
        assertEquals(OptionalInt.empty(), capacity.shrink(capacity.acquire()));
        assertEquals(1, capacity.limit());
    }

    @Test
    void slotsAreCountedAndTheOperatorCanResize() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(2);

        assertEquals(2, capacity.acquire().limit());
        assertEquals(2, capacity.acquire().limit());
        assertEquals(2, capacity.inFlight());

        capacity.setLimit(3);
        assertEquals(3, capacity.acquire().limit(), "a raised limit frees a slot at once");
        capacity.release();
        capacity.release();
        capacity.release();
        assertEquals(0, capacity.inFlight());
        assertEquals(3, capacity.limit());
    }

    @Test
    void loweringTheLimitDrainsRatherThanInterrupting() throws InterruptedException {
        DownstreamCapacity capacity = new DownstreamCapacity(3);
        capacity.acquire();
        capacity.acquire();
        capacity.acquire();

        capacity.setLimit(1);
        assertEquals(3, capacity.inFlight(), "loads in flight are not interrupted");

        Thread waiter = Thread.ofPlatform().start(() -> {
            try {
                capacity.acquire();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        capacity.release();
        capacity.release();
        while (waiter.getState() != Thread.State.WAITING) Thread.onSpinWait();
        assertEquals(1, capacity.inFlight(), "the excess drained and the waiter is held at the new limit");

        capacity.release();
        waiter.join();
        assertEquals(1, capacity.inFlight());
    }

    @Test
    void aLimitBelowOneIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new DownstreamCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> new DownstreamCapacity(1).setLimit(0));
    }
}
