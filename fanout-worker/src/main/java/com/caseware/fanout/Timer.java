package com.caseware.fanout;

import java.time.Duration;

/** Runs an action after a delay. Retries and downstream backoff wait here, which lets a test drive time by hand. */
public interface Timer {

    void after(Duration delay, Runnable action);
}
