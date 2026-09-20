package com.caseware.fanout;

import java.util.ArrayList;
import java.util.List;

final class RecordingDeadLetters implements DeadLetterQueue {

    private final List<DeadLetter> entries = new ArrayList<>();
    private RuntimeException failure;

    /** From now on every send throws, the way an SQS call with expired credentials does. */
    synchronized void failEverySend(RuntimeException failure) {
        this.failure = failure;
    }

    @Override
    public synchronized void send(DeadLetter deadLetter) {
        if (failure != null) throw failure;
        entries.add(deadLetter);
    }

    synchronized int count() {
        return entries.size();
    }

    synchronized DeadLetter only() {
        if (entries.size() != 1) throw new AssertionError("expected one dead letter, found " + entries);
        return entries.get(0);
    }
}
