package com.caseware.fanout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A downstream the test controls: it can hold every call until released, refuse calls over its own limit, and fail
 * chosen engagements. Counters are exact, so a test asserts on what happened rather than on timing.
 */
final class FakeEngagementSystem implements EngagementSystem {

    private final Lock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<EngagementId, DownstreamFailure.Kind> poisoned = new HashMap<>();
    private final Map<EngagementId, Integer> loadsPerEngagement = new HashMap<>();
    private int ownLimit = Integer.MAX_VALUE;
    private boolean holding;
    private int nextTicket;
    private int releasedUpTo;
    private int arrivals;
    private int inFlight;
    private int maxInFlight;
    private int rejections;

    @Override
    public String loadEffectiveVersion(EngagementId engagementId) throws DownstreamFailure {
        lock.lock();
        try {
            arrivals++;
            loadsPerEngagement.merge(engagementId, 1, Integer::sum);
            if (inFlight >= ownLimit) {
                rejections++;
                changed.signalAll();
                throw new DownstreamFailure(DownstreamFailure.Kind.OVER_CAPACITY, "engagement system at its limit");
            }
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            int ticket = nextTicket++;
            changed.signalAll();
            while (holding && ticket >= releasedUpTo) changed.awaitUninterruptibly();
            inFlight--;
            changed.signalAll();
            DownstreamFailure.Kind poison = poisoned.get(engagementId);
            if (poison != null) throw new DownstreamFailure(poison, "poisoned " + engagementId);
            return "v-" + engagementId;
        } finally {
            lock.unlock();
        }
    }

    void ownLimit(int limit) {
        run(() -> ownLimit = limit);
    }

    void poison(EngagementId engagementId, DownstreamFailure.Kind kind) {
        run(() -> poisoned.put(engagementId, kind));
    }

    /** From now on every accepted call blocks until {@link #release} or {@link #releaseAll}. */
    void holdCalls() {
        run(() -> holding = true);
    }

    /** Lets the {@code n} earliest held calls finish, in arrival order. */
    void release(int n) {
        run(() -> releasedUpTo += n);
    }

    void releaseAll() {
        run(() -> {
            holding = false;
            releasedUpTo = Integer.MAX_VALUE;
        });
    }

    /** Waits until at least {@code n} calls have arrived, accepted or refused. The timeout is a failure, not a pause. */
    void awaitArrivals(int n) {
        lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (arrivals < n) {
                long left = deadline - System.nanoTime();
                if (left <= 0) throw new AssertionError("expected " + n + " arrivals, saw " + arrivals);
                changed.awaitNanos(left);
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } finally {
            lock.unlock();
        }
    }

    int arrivals() {
        return read(() -> arrivals);
    }

    int inFlight() {
        return read(() -> inFlight);
    }

    int maxInFlight() {
        return read(() -> maxInFlight);
    }

    int rejections() {
        return read(() -> rejections);
    }

    int loadsOf(EngagementId engagementId) {
        return read(() -> loadsPerEngagement.getOrDefault(engagementId, 0));
    }

    private void run(Runnable change) {
        lock.lock();
        try {
            change.run();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private int read(java.util.function.IntSupplier value) {
        lock.lock();
        try {
            return value.getAsInt();
        } finally {
            lock.unlock();
        }
    }
}
