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

    private final Lock lock = new ReentrantLock();
    private final Condition slotFreed = lock.newCondition();
    private int limit;
    private int inFlight;

    public DownstreamCapacity(int limit) {
        this.limit = requireAtLeastOne(limit);
    }

    /** Blocks until a slot is free. Returns the limit in force at that moment, so a refusal can be attributed to it. */
    public int acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (inFlight >= limit) slotFreed.await();
            inFlight++;
            return limit;
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
     * Returns a slot whose load the engagement system refused, halving the limit first if it is still
     * {@code observedLimit}. The two happen together so the freed slot cannot be refilled at the limit that was just
     * refused, and a burst of refusals from one window shrinks the limit once. Returns the new limit when this call
     * changed it. The cap never grows on its own: that is the operator's decision, through {@link #setLimit}.
     */
    public OptionalInt releaseRefused(int observedLimit) {
        lock.lock();
        try {
            inFlight--;
            slotFreed.signal();
            if (limit != observedLimit || limit == 1) return OptionalInt.empty();
            limit = Math.max(1, limit / 2);
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
