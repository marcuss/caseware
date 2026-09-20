package com.caseware.fanout;

/**
 * How one submission of a publish ended. The counts cover that submission's tasks only: a resubmission after a
 * restart counts what was left, not what an earlier process already did. A submission that could not record what
 * happened to a row does not produce an outcome at all; its handle fails instead, so the delivery is not
 * acknowledged.
 *
 * <p>{@code dropped} and {@code abandoned} are deliberately separate numbers. A dropped row needed nothing: it was
 * archived, or another publish had already claimed it. An abandoned row is still {@code UNVERIFIED} and this
 * worker stopped trying without a dead letter for an operator to replay, so nothing is scheduled to look at it
 * before the next publish of that template. Only the second belongs in the design's "unverified under 1%"
 * objective, and an adapter that reports one number should report that one.
 *
 * @param verified     rows whose base version was loaded and written back
 * @param dropped      rows that needed no work from this submission
 * @param abandoned    rows still unverified, given up on without a dead letter
 * @param deadLettered rows an operator now holds a dead letter for
 */
public record PublishOutcome(int verified, int dropped, int abandoned, int deadLettered) {}
