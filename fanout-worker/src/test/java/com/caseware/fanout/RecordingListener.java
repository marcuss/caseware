package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/** Records every hook and lets a test wait for a condition over what was recorded. */
final class RecordingListener implements WorkerListener {

    private final Lock lock = new ReentrantLock();
    private final Condition recorded = lock.newCondition();
    private final List<VerifyTask> dispatched = new ArrayList<>();
    private final Map<EngagementId, String> droppedReasons = new LinkedHashMap<>();
    private final List<Duration> retryDelays = new ArrayList<>();
    private final List<int[]> capacityChanges = new ArrayList<>();
    private int verified;
    private int deadLettered;

    @Override
    public void taskDispatched(VerifyTask task, int attempt) {
        record(() -> dispatched.add(task));
    }

    @Override
    public void taskVerified(VerifyTask task, Duration loadTime) {
        record(() -> verified++);
    }

    @Override
    public void taskDropped(VerifyTask task, String reason) {
        record(() -> droppedReasons.put(task.engagementId(), reason));
    }

    @Override
    public void retryScheduled(VerifyTask task, int nextAttempt, Duration delay, Throwable cause) {
        record(() -> retryDelays.add(delay));
    }

    @Override
    public void taskDeadLettered(VerifyTask task, int attempts, Throwable cause) {
        record(() -> deadLettered++);
    }

    @Override
    public void capacityShrunk(int from, int to) {
        record(() -> capacityChanges.add(new int[] {from, to}));
    }

    /** Waits until {@code condition} holds over the recorded hooks. The timeout is a failure, not a pause. */
    void awaitUntil(BooleanSupplier condition, String what) {
        lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!condition.getAsBoolean()) {
                long left = deadline - System.nanoTime();
                if (left <= 0) throw new AssertionError("timed out waiting for " + what);
                recorded.awaitNanos(left);
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } finally {
            lock.unlock();
        }
    }

    List<VerifyTask> dispatched() {
        return read(() -> List.copyOf(dispatched));
    }

    Map<EngagementId, String> droppedReasons() {
        return read(() -> Map.copyOf(droppedReasons));
    }

    List<Duration> retryDelays() {
        return read(() -> List.copyOf(retryDelays));
    }

    List<int[]> capacityChanges() {
        return read(() -> List.copyOf(capacityChanges));
    }

    int verified() {
        return read(() -> verified);
    }

    int deadLettered() {
        return read(() -> deadLettered);
    }

    private void record(Runnable change) {
        lock.lock();
        try {
            change.run();
            recorded.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private <T> T read(java.util.function.Supplier<T> value) {
        lock.lock();
        try {
            return value.get();
        } finally {
            lock.unlock();
        }
    }
}
