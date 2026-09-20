package com.caseware.fanout;

import java.util.OptionalInt;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The one place the downstream cap lives. Every load, for every publish in flight, takes a slot here first, so the
 * engagement team sees at most {@code limit} concurrent loads from this worker however many publishes are running.
 */
public final class DownstreamCapacity {

    /**
     * A held slot, tagged with the generation of dispatches it belongs to. A generation ends the moment the limit
     * changes, which is what lets {@link #shrink} halve once for a whole burst of refusals: the first refusal ends
     * the generation, and every other slot from it is then stale.
     */
    public record Slot(int limit, long generation) {}

    private final Lock lock = new ReentrantLock();
    private final Condition slotFreed = lock.newCondition();
    private int limit;
    private int inFlight;
    private long generation;

    public DownstreamCapacity(int limit) {
        this.limit = requireAtLeastOne(limit);
    }

    /** Blocks until a slot is free. The slot carries the limit and generation it was taken in. */
    public Slot acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (inFlight >= limit) slotFreed.await();
            inFlight++;
            return new Slot(limit, generation);
        } finally {
            lock.unlock();
        }
    }

    public void release() {
        lock.lock();
        try {
            inFlight--;
            slotFreed.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Halves the limit because the engagement system refused or timed out the load this slot was taken for, and
     * starts a new generation. A slot from an older generation changes nothing, so one busy window costs one
     * halving, and an operator's {@link #setLimit} is never re-halved by a straggler. Returns the new limit when
     * this call changed it. The cap never grows on its own: that is the operator's decision.
     *
     * <p>The slot is not released here. The caller decides when the downstream is actually free of the load, which
     * for a timeout is not yet.
     */
    public OptionalInt shrink(Slot slot) {
        lock.lock();
        try {
            if (slot.generation() != generation || limit == 1) return OptionalInt.empty();
            limit = Math.max(1, limit / 2);
            generation++;
            slotFreed.signalAll();
            return OptionalInt.of(limit);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The operator's hook. The design halves the cap when engagement opens slow down and restores it by hand.
     * Lowering it never interrupts a load in flight; the excess drains as loads finish.
     */
    public void setLimit(int newLimit) {
        requireAtLeastOne(newLimit);
        lock.lock();
        try {
            limit = newLimit;
            generation++;
            slotFreed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int limit() {
        lock.lock();
        try {
            return limit;
        } finally {
            lock.unlock();
        }
    }

    public int inFlight() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    private static int requireAtLeastOne(int limit) {
        if (limit < 1) throw new IllegalArgumentException("limit must be at least 1, was " + limit);
        return limit;
    }
}
