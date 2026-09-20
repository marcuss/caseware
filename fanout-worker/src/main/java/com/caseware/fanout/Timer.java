package com.caseware.fanout;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Runs an action after a delay. Retries wait here, which lets a test drive time by hand. */
public interface Timer {

    void after(Duration delay, Runnable action);

    static Timer using(ScheduledExecutorService executor) {
        return (delay, action) -> executor.schedule(action, delay.toMillis(), TimeUnit.MILLISECONDS);
    }
}
