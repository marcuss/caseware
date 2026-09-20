package com.caseware.fanout;

import java.util.ArrayList;
import java.util.List;

final class RecordingDeadLetters implements DeadLetterQueue {

    private final List<DeadLetter> entries = new ArrayList<>();

    @Override
    public synchronized void send(DeadLetter deadLetter) {
        entries.add(deadLetter);
    }

    synchronized List<DeadLetter> entries() {
        return List.copyOf(entries);
    }

    synchronized DeadLetter only() {
        if (entries.size() != 1) throw new AssertionError("expected one dead letter, found " + entries);
        return entries.get(0);
    }
}
