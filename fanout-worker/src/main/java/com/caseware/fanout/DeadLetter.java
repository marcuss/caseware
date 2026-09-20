package com.caseware.fanout;

import java.util.Objects;

/**
 * A task the worker gave up on, with what an operator needs to decide whether to replay it. {@code seqAtLoad} is
 * the sequence the last attempt read, which is the value the row had to still be at for the {@code DEAD_LETTERED}
 * mark to land; the task's {@code lastSeqAtEnqueue} says where the row was when the work was first scheduled.
 */
public record DeadLetter(VerifyTask task, int attempts, long seqAtLoad, Throwable cause) {
    public DeadLetter {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(cause, "cause");
    }
}
