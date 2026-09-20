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
    private final List<PublishId> accepted = new ArrayList<>();
    private final List<PublishId> finished = new ArrayList<>();
    private final List<VerifyTask> dispatched = new ArrayList<>();
    private final Map<EngagementId, String> droppedReasons = new LinkedHashMap<>();
    private final Map<EngagementId, String> abandonedReasons = new LinkedHashMap<>();
    private final List<Duration> retryDelays = new ArrayList<>();
    private final List<String> requeueReasons = new ArrayList<>();
    private final List<int[]> capacityChanges = new ArrayList<>();
    private final List<Throwable> unsettled = new ArrayList<>();
    private final List<Throwable> dispatcherFailures = new ArrayList<>();
    private int verified;
    private int pauses;

    @Override
    public void publishAccepted(PublishId publishId) {
        record(() -> accepted.add(publishId));
    }

    @Override
    public void publishFinished(PublishId publishId, PublishOutcome outcome) {
        record(() -> finished.add(publishId));
    }

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
    public void taskAbandoned(VerifyTask task, String reason) {
        record(() -> abandonedReasons.put(task.engagementId(), reason));
    }

    @Override
    public void taskRequeued(VerifyTask task, Duration delay, String reason) {
        record(() -> requeueReasons.add(reason));
    }

    @Override
    public void retryScheduled(VerifyTask task, int nextAttempt, Duration delay, Throwable cause) {
        record(() -> retryDelays.add(delay));
    }

    @Override
    public void taskUnsettled(VerifyTask task, Throwable cause) {
        record(() -> unsettled.add(cause));
    }

    @Override
    public void capacityShrunk(int from, int to) {
        record(() -> capacityChanges.add(new int[] {from, to}));
    }

    @Override
    public void downstreamPaused(Duration pause, Throwable cause) {
        record(() -> pauses++);
    }

    @Override
    public void dispatcherFailed(Throwable cause) {
        record(() -> dispatcherFailures.add(cause));
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

    List<PublishId> accepted() {
        return read(() -> List.copyOf(accepted));
    }

    List<PublishId> finished() {
        return read(() -> List.copyOf(finished));
    }

    List<VerifyTask> dispatched() {
        return read(() -> List.copyOf(dispatched));
    }

    Map<EngagementId, String> droppedReasons() {
        return read(() -> Map.copyOf(droppedReasons));
    }

    Map<EngagementId, String> abandonedReasons() {
        return read(() -> Map.copyOf(abandonedReasons));
    }

    List<Duration> retryDelays() {
        return read(() -> List.copyOf(retryDelays));
    }

    List<String> requeueReasons() {
        return read(() -> List.copyOf(requeueReasons));
    }

    List<int[]> capacityChanges() {
        return read(() -> List.copyOf(capacityChanges));
    }

    List<Throwable> unsettled() {
        return read(() -> List.copyOf(unsettled));
    }

    List<Throwable> dispatcherFailures() {
        return read(() -> List.copyOf(dispatcherFailures));
    }

    int verified() {
        return read(() -> verified);
    }

    int pauses() {
        return read(() -> pauses);
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
