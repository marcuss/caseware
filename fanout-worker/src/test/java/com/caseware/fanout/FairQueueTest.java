package com.caseware.fanout;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class FairQueueTest {

    private final FairQueue queue = new FairQueue();
    private final PublishHandle publish = new PublishHandle(new PublishId("p"), (handle, outcome) -> {});

    @Test
    void firmsTakeTurnsWhateverTheirBacklog() throws InterruptedException {
        offer("big", 3);
        offer("small", 2);

        assertEquals(List.of("big", "small", "big", "small", "big"), takeAll());
    }

    @Test
    void aFirmAloneGetsEveryTurn() throws InterruptedException {
        offer("only", 3);

        assertEquals(List.of("only", "only", "only"), takeAll());
    }

    @Test
    void aFirmKeepsItsOwnArrivalOrder() throws InterruptedException {
        offer("f", 3);

        List<String> engagements = new ArrayList<>();
        while (queue.size() > 0) engagements.add(queue.take().task().engagementId().value());
        assertEquals(List.of("f-0", "f-1", "f-2"), engagements);
    }

    private void offer(String firm, int count) {
        for (int i = 0; i < count; i++) {
            VerifyTask task = new VerifyTask(new PublishId("p"), new FirmId(firm), new EngagementId(firm + "-" + i), 1);
            queue.offer(QueuedTask.first(publish, task));
        }
    }

    private List<String> takeAll() throws InterruptedException {
        List<String> firms = new ArrayList<>();
        while (queue.size() > 0) firms.add(queue.take().task().firmId().value());
        return firms;
    }
}
