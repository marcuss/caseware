package com.caseware.fanout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.caseware.fanout.DownstreamFailure.Kind;

/**
 * A downstream the test controls: it can hold every call until released, refuse a stated number of calls, fail
 * everything until it is told to recover, and fail chosen engagements. Every failure is asked for by the test, so
 * no assertion rests on which thread won a race.
 */
final class FakeEngagementSystem implements EngagementSystem {

    private final Lock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<EngagementId, Kind> poisoned = new HashMap<>();
    private final Map<EngagementId, Integer> loadsPerEngagement = new HashMap<>();
    private java.util.function.Consumer<EngagementId> duringLoad;
    private Kind failEveryCall;
    private int refusalsLeft;
    private boolean holding;
    private int nextTicket;
    private int releasedUpTo;
    private int arrivals;
    private int inFlight;
    private int maxInFlight;
    private int refusals;

    @Override
    public String loadEffectiveVersion(EngagementId engagementId) throws DownstreamFailure {
        lock.lock();
        try {
            arrivals++;
            loadsPerEngagement.merge(engagementId, 1, Integer::sum);
            changed.signalAll();
            if (refusalsLeft > 0) {
                refusalsLeft--;
                refusals++;
                throw new DownstreamFailure(Kind.OVER_CAPACITY, "engagement system at its limit");
            }
            if (failEveryCall != null) {
                refusals++;
                throw new DownstreamFailure(failEveryCall, "engagement system is " + failEveryCall);
            }
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            int ticket = nextTicket++;
            changed.signalAll();
            while (holding && ticket >= releasedUpTo) changed.awaitUninterruptibly();
            inFlight--;
            if (duringLoad != null) duringLoad.accept(engagementId);
            changed.signalAll();
            Kind poison = poisoned.get(engagementId);
            if (poison != null) throw new DownstreamFailure(poison, "poisoned " + engagementId);
            return "v-" + engagementId;
        } finally {
            lock.unlock();
        }
    }

    /** Runs inside every load, which is how a test makes something happen while the engagement is being read. */
    void duringEachLoad(java.util.function.Consumer<EngagementId> action) {
        run(() -> duringLoad = action);
    }

    /** The next {@code n} calls are refused outright, whoever makes them. */
    void refuseNext(int n) {
        run(() -> refusalsLeft = n);
    }

    /** Every call fails this way until {@link #recover}. */
    void failEveryCall(Kind kind) {
        run(() -> failEveryCall = kind);
    }

    /** The engagement system is healthy again: no outage, no refusals left, and no engagement still failing. */
    void recover() {
        run(() -> {
            failEveryCall = null;
            refusalsLeft = 0;
            poisoned.clear();
        });
    }

    void poison(EngagementId engagementId, Kind kind) {
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
        awaitUntil(() -> arrivals >= n, "at least " + n + " arrivals");
    }

    void awaitUntil(java.util.function.BooleanSupplier condition, String what) {
        lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!condition.getAsBoolean()) {
                long left = deadline - System.nanoTime();
                if (left <= 0) throw new AssertionError("timed out waiting for " + what + "; arrivals=" + arrivals);
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

    int refusals() {
        return read(() -> refusals);
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
