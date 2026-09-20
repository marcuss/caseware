package com.caseware.fanout;

/** Identifies a firm. Fairness between firms is decided on this key. */
public record FirmId(String value) {
    public FirmId {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("firm id is blank");
    }

    @Override
    public String toString() {
        return value;
    }
}
