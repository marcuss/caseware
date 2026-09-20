package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A timer whose clock moves only when the test says so. Due actions run on the test's thread, in due order. */
final class ManualTimer implements Timer {

    private record Scheduled(Duration due, long order, Runnable action) {}

    private final List<Scheduled> pending = new ArrayList<>();
    private Duration now = Duration.ZERO;
    private long counter;
    private boolean refusing;

    @Override
    public synchronized void after(Duration delay, Runnable action) {
        if (refusing) throw new IllegalStateException("timer is shut down");
        pending.add(new Scheduled(now.plus(delay), counter++, action));
    }

    /** From now on scheduling throws, the way a shut-down ScheduledExecutorService does. */
    synchronized void refuseEverything() {
        refusing = true;
    }

    void advance(Duration by) {
        List<Scheduled> due;
        synchronized (this) {
            now = now.plus(by);
            due = pending.stream()
                    .filter(s -> s.due().compareTo(now) <= 0)
                    .sorted(Comparator.comparing(Scheduled::due).thenComparingLong(Scheduled::order))
                    .toList();
            pending.removeAll(due);
        }
        due.forEach(s -> s.action().run());
    }

    synchronized int pending() {
        return pending.size();
    }
}
