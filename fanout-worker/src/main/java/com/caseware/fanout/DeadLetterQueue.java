package com.caseware.fanout;

/**
 * Where given-up work goes. The worker sends the dead letter before it marks the projection row, so a crash between
 * the two can duplicate a dead letter but never lose one.
 */
public interface DeadLetterQueue {
    void send(DeadLetter deadLetter);
}
