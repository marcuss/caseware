package com.caseware.fanout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** A timer whose clock moves only when the test says so. Due actions run on the test's thread, in due order. */
final class ManualTimer implements Timer {

    private record Scheduled(Duration due, long order, Runnable action) {}

    private final List<Scheduled> pending = new ArrayList<>();
    private final List<Duration> delays = new ArrayList<>();
    private Duration now = Duration.ZERO;
    private long counter;

    @Override
    public synchronized void after(Duration delay, Runnable action) {
        pending.add(new Scheduled(now.plus(delay), counter++, action));
        delays.add(delay);
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

    /** Every delay scheduled so far, in order. */
    synchronized List<Duration> delays() {
        return List.copyOf(delays);
    }

    synchronized int pending() {
        return pending.size();
    }
}
