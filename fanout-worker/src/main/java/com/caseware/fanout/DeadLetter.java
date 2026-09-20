package com.caseware.fanout;

import java.util.Objects;

/** A task the worker gave up on, with what an operator needs to decide whether to replay it. */
public record DeadLetter(VerifyTask task, int attempts, Throwable cause) {
    public DeadLetter {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(cause, "cause");
    }
}
