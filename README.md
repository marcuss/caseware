# Caseware take-home: pending template updates

A system that shows firms which of their engagement files have pending product
template updates, with human-readable summaries of the inbound changes so they
can apply or decline.

## Deliverables

| Deliverable | Where |
|---|---|
| Part 1 — design document (Markdown) | [`docs/design.md`](docs/design.md) |
| Diagrams | Inside [`docs/design.md`](docs/design.md): the architecture diagram in §1 and the pending-update state machine in §2, both Mermaid, rendered by GitHub |
| Part 2 — implementation | [`fanout-worker/`](fanout-worker) (Java 21, Maven, JUnit 5) |
| Part 2 — README of decisions and tradeoffs | [`fanout-worker/README.md`](fanout-worker/README.md) |

## Running the tests

```
cd fanout-worker && mvn -B test
```

Java 21 and Maven are the only requirements. JUnit 5 is the single dependency.
40 tests, no sleeps: time is driven by hand and every wait is on a condition.

## The design in four decisions

- A durable per-region projection of `(engagement, template, effective base,
  decision log)`, maintained by events. Publishing a template never loads an
  engagement, because loading one costs a minute of another team's capacity and
  a publish spans about 20,000 files.
- Summaries are generated once per `(template, base, head)` pair rather than once
  per engagement. Template content is not firm-specific, so one summary is reused
  roughly 1,700 times. That single decision is the difference between $166 and
  $277,000 a month.
- The one-minute load is confined to a single rate-limited verifier, which runs
  the backfill of existing engagements and afterwards re-checks only rows the
  system distrusts. That verifier is the Part 2 worker.
- A decline pins one target version rather than the engagement, so a firm that
  declined v5 is offered v7 with a note saying what it already saw.

## Supporting material

Not part of the submission, kept for reference:

- [`docs/design/cost-inputs.md`](docs/design/cost-inputs.md) — the AWS and Anthropic
  rates behind every number in the cost table, pulled from the AWS Price List API.
- [`docs/design/requirements-checklist.md`](docs/design/requirements-checklist.md) —
  every requirement and constant from the brief, used to check the design against it.
- [`docs/design/draft-a.md`](docs/design/draft-a.md) and
  [`docs/design/draft-b.md`](docs/design/draft-b.md) — two independent first drafts,
  one leading with the data model and one with the cost arithmetic, synthesized
  into the final document.
- [`docs/assignment/`](docs/assignment) — the brief and job description as text.
