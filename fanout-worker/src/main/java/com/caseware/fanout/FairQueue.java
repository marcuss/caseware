package com.caseware.fanout;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tasks waiting for a slot, handed out round-robin across firms: each firm with work waiting gets the next slot in
 * turn, so a firm with 40,000 files cannot starve one with 40, and a firm alone in the queue gets every slot.
 * Within a firm, tasks keep their arrival order.
 */
final class FairQueue {

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Map<FirmId, Deque<QueuedTask>> byFirm = new HashMap<>();
    private final Deque<FirmId> turns = new ArrayDeque<>();

    void offer(QueuedTask queued) {
        lock.lock();
        try {
            Deque<QueuedTask> firmTasks = byFirm.computeIfAbsent(queued.task().firmId(), firm -> new ArrayDeque<>());
            if (firmTasks.isEmpty()) turns.addLast(queued.task().firmId());
            firmTasks.addLast(queued);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    QueuedTask take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (turns.isEmpty()) notEmpty.await();
            FirmId firm = turns.pollFirst();
            Deque<QueuedTask> firmTasks = byFirm.get(firm);
            QueuedTask next = firmTasks.pollFirst();
            if (firmTasks.isEmpty()) byFirm.remove(firm);
            else turns.addLast(firm);
            return next;
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return byFirm.values().stream().mapToInt(Deque::size).sum();
        } finally {
            lock.unlock();
        }
    }
}
