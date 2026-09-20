package com.caseware.fanout;

import java.util.Objects;

/** A failed downstream call, classified by what it says about trying again. */
public final class DownstreamFailure extends Exception {

    public enum Kind {
        /** Try again later: a 5xx, a dropped connection, a refused connection. Says nothing about the engagement. */
        TRANSIENT,
        /** The engagement system refused the call because it is at its limit. The cap is too high. */
        OVER_CAPACITY,
        /**
         * The adapter gave up waiting. The engagement system is probably still running the load, so the worker
         * keeps the slot until that load's budget has run out rather than pretending the capacity is back.
         */
        TIMED_OUT,
        /** Trying again cannot help: a missing or corrupt engagement, a rejected request. */
        TERMINAL
    }

    private final Kind kind;

    public DownstreamFailure(Kind kind, String message) {
        this(kind, message, null);
    }

    public DownstreamFailure(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    public Kind kind() {
        return kind;
    }
}
