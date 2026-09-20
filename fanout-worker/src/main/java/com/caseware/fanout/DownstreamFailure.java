package com.caseware.fanout;

import java.util.Objects;

/** A failed downstream call, classified by what it says about trying again. */
public final class DownstreamFailure extends Exception {

    public enum Kind {
        /** Try again later: a timeout, a 5xx, a dropped connection. */
        TRANSIENT,
        /** The engagement system refused the call because it is at its limit. Retryable, and a sign the cap is too high. */
        OVER_CAPACITY,
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
