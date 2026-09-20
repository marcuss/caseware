package com.caseware.fanout;

/**
 * How one submission of a publish ended. The counts cover that submission's tasks only: a resubmission after a
 * restart counts what was left, not what an earlier process already did. A submission that could not record what
 * happened to a row does not produce an outcome at all; its handle fails instead, so the delivery is not
 * acknowledged.
 */
public record PublishOutcome(int verified, int dropped, int deadLettered, boolean cancelled) {}
