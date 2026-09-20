package com.caseware.fanout;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds dispatch while the engagement system is failing every load, and tells the worker when a failure is the
 * downstream's condition rather than the row's. Without it, a downstream that refuses everything for ten minutes
 * would be answered by feeding it the whole queue as fast as it can refuse, spending each row's retry budget on a
 * condition that has nothing to do with that row and dead-lettering the publish.
 *
 * <p>Failures are counted per distinct engagement since the last success: an outage fails many engagements, a bad
 * row fails only itself. Counting failures instead would let one corrupt file that fails every time it is loaded
 * declare the downstream out, and a row the downstream never says anything bad about can never be dead-lettered,
 * so it would be retried for ever. At {@code failuresBeforePause} distinct engagements the gate closes and reopens
 * after {@code pause}. The set survives reopening, so the next new engagement to fail closes it again; a single
 * success clears it.
 */
final class DownstreamGate {

    interface Events {
        void paused(Duration pause, Throwable cause);

        void resumed();
    }

    private final int failuresBeforePause;
    private final Duration pause;
    private final Timer timer;
    private final Events events;
    private final Lock lock = new ReentrantLock();
    private final Condition opened = lock.newCondition();
    private final Set<EngagementId> failingSinceLastSuccess = new HashSet<>();
    private boolean closed;

    DownstreamGate(int failuresBeforePause, Duration pause, Timer timer, Events events) {
        this.failuresBeforePause = failuresBeforePause;
        this.pause = pause;
        this.timer = timer;
        this.events = events;
    }

    /** Returns true when the downstream, not the row, is what failed: the caller must not charge the row for it. */
    boolean recordFailure(EngagementId engagementId, Throwable cause) {
        boolean justClosed;
        boolean downstreamIsOut;
        lock.lock();
        try {
            failingSinceLastSuccess.add(engagementId);
            downstreamIsOut = failingSinceLastSuccess.size() >= failuresBeforePause;
            justClosed = downstreamIsOut && !closed;
            if (justClosed) closed = true;
        } finally {
            lock.unlock();
        }
        if (justClosed) {
            try {
                timer.after(pause, this::open);
                events.paused(pause, cause);
            } catch (RuntimeException cannotSchedule) {
                open();
            }
        }
        return downstreamIsOut;
    }

    void recordSuccess() {
        lock.lock();
        try {
            failingSinceLastSuccess.clear();
        } finally {
            lock.unlock();
        }
        open();
    }

    /** The dispatcher's wait. Returns at once unless the gate is closed. */
    void awaitOpen() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (closed) opened.await();
        } finally {
            lock.unlock();
        }
    }

    private void open() {
        lock.lock();
        try {
            if (!closed) return;
            closed = false;
            opened.signalAll();
        } finally {
            lock.unlock();
        }
        events.resumed();
    }
}
