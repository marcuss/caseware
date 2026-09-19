# Requirements Checklist — Caseware Staff Java Take-Home

**Grounding artifact.** Every later agent in this workflow is held to this list.
Sources: `docs/assignment/take-home-assignment.txt` (verified faithful against the
original PDF at `/root/.claude/uploads/72cbc24f-e5b0-5a9a-abc2-fc5b5058f5cc/213b5058-Staff_Java_Developer_-_Take-Home_Test_2.pdf`),
`docs/assignment/job-description.txt`, and `docs/design/cost-inputs.md` for rates.

The brief is the only authority. Where a reviewer and the brief disagree, quote the brief.

---

## 0. The numbers, verbatim

Every one of these appears in the brief. Using a different number is a factual error,
not a judgment call.

| Constant | Value | Where it bites |
|---|---|---|
| Firms | 4,000 | Tenancy, per-firm partitioning, residency routing |
| Active engagement files | 800,000 | Backfill volume, projection size, fan-out width |
| Products | 40 | Publish rate, number of distinct template lineages |
| Engagements/firm, median | ~40 | The easy case; do not design only for this |
| Engagements/firm, largest | ~40,000 | Hot partition, fairness, UI, bulk decisioning |
| Publish rate | ~1/week/product → ~40/week, ~173/month | Denominator for all inference arithmetic |
| Engagement load time | ~1 minute, **hard constraint** | Kills any design that touches engagements on the publish path |
| Budget | «$X»/month, all-in **including inference** | Deliberately blank; candidate must name a number |
| Freshness target | "within seconds" of publish | Deliberately vague; candidate must decompose it |
| Residency | EU, Canada | Engagement data only; template content is not firm-specific |
| Design doc length | 2–4 pages, diagrams excluded | Hard cap |
| Time budget | 4 hours total | "Please do not over-optimize" |

### 0.1 The arithmetic that decides the architecture

These are illustrative but the *shape* is not negotiable. Any answer that does not
produce numbers of this order has not done the exercise.

**Naive publish-path fan-out (the disqualifying path).**
Average engagements per product = 800,000 / 40 = **20,000**.
20,000 × 1 min = 20,000 minutes = **333 downstream-hours per single publish**.
Even at 100-way concurrency that is **3.3 hours**, against a requirement of "within seconds" —
roughly four orders of magnitude off. *Conclusion: the publish path must never load an engagement.*

**Backfill (unavoidable, because existing engagements are in scope).**
800,000 × 1 min = 800,000 minutes = **13,333 engagement-hours**.
At 100 concurrent slots ≈ **5.6 days**; at 50 slots ≈ **11 days**.
Those slots are taken from the *same* system that serves engagement opens, creation, and
accept/decline — so real usable concurrency is whatever spare capacity the owning team grants.

**Steady-state fan-out cost (cheap, and that is the point).**
173 publishes/month × 20,000 affected engagements ≈ **3.46M projection writes/month**.
DynamoDB on-demand writes @ $0.625/M → **~$2.16/month**. SQS @ $0.40/M → **~$1.38/month**.
Projection storage: 800k rows × ~1 KB ≈ 0.8 GB @ $0.25/GB-mo → **~$0.20/month**. Negligible.

**Inference done right — priced per *diff*, not per engagement.**
A summary is a function of (product, baseVersion → targetVersion). It is identical for every
firm on the same pair, because template content is not firm-specific.
Assume ~12 distinct live base versions per product at publish time:
173 × 12 ≈ **2,080 summaries/month**.
At 30k input / 2k output on Claude Sonnet 5 ($2.00 / $10.00 per M):
2,080 × (0.060 + 0.020) = **~$166/month**. Prompt caching on the shared target-version
context and the Batch API (50% off) push this lower.

**Inference done wrong — priced per engagement.**
3.46M × $0.08 = **~$277,000/month**. Three orders of magnitude over any plausible «$X».
*The gap between $166 and $277,000 is the single most load-bearing number in the submission.*

Add a 5–15% premium for `eu-central-1` / `ca-central-1` where work runs in-region, per
`cost-inputs.md`. Say so rather than silently using `us-east-1` everywhere.

---

## 1. Deliverables and form

**DEL-1 — Design document, 2–4 pages maximum, diagrams excluded from the count.**
Source: *"a short design document (2-4 pages max; diagrams don't count toward the limit)"*
Trap: shipping eight pages because the topic list is long. Length here is itself a graded
signal — the brief says outright it prefers *"a short document with sharp decisions than a
long one that hedges."* Padding with a restatement of the problem burns the page budget on
content the reader wrote.

**DEL-2 — Cover all five numbered topics, visibly.**
Source: *"1. Architecture and data ownership … 5. Key tradeoffs, assumptions, and the one requirement above you would challenge"*
Trap: topics 1–3 get three pages and topics 4–5 get two sentences. Topic 4 (summary
generation *and evaluation*) and topic 5 are where Staff-level judgment is actually visible.

**DEL-3 — Show the arithmetic.**
Source: *"show your arithmetic against the numbers above"*
Trap: "this comfortably fits the budget" with no numbers, or a cost table with no stated
assumptions behind the token counts. The brief asks to *see the work*, not the conclusion.

**DEL-4 — Name the riskiest part of the design.**
Source: *"tell us which part of your design is riskiest to get wrong"*
Trap: naming something safe and peripheral (retry tuning) instead of the genuinely scary
thing — e.g. a projection that silently drifts out of sync with the engagement store and
shows a firm the wrong pending state, or a decline-pinning model that suppresses an update
the firm was legally required to evaluate. Also a trap: naming four risks, which is naming none.

**DEL-5 — State what was deliberately left out.**
Source: *"what you deliberately chose to leave out"*
Trap: listing things that were forgotten rather than chosen, or writing "nothing, this is
complete." The value is in showing the cut was intentional and the cost of the cut is known.

**DEL-6 — Name the one requirement you would challenge.**
Source: *"the one requirement above you would challenge"*
Trap: challenging nothing (reads as compliance, not judgment); challenging the 1-minute load
(that one is labelled a hard constraint, so this reads as not having read carefully);
challenging several requirements when the brief asked for *one*. The strong move is to
challenge "within seconds" — and to do it with arithmetic showing which half of it is free
and which half is expensive and unnecessary.

**DEL-7 — Part 2 is required, in Java.**
Source: *"Part 2: Targeted Implementation (Required)"*
Trap: treating it as optional, or the opposite — building a full application when the brief
says *"This is not a full application build."*

**DEL-8 — Short README explaining implementation tradeoffs.**
Source: *"In a short README, explain your key implementation tradeoffs."*
Trap: a README that documents how to build and run the code instead of *why it is shaped
that way*. Tradeoffs means alternatives considered and rejected, with reasons.

**DEL-9 — The AI-usage narrative, four specific questions.**
Source: *"Where AI helped you / Where you corrected or ignored AI output / How you would guide other engineers using AI on this system / Where AI should not be trusted in this domain"*
Trap: "I used Claude for boilerplate." The brief says *"We are evaluating judgment and
maturity, not just usage,"* and the job description makes defining AI guardrails a core
responsibility. A concrete instance of *correcting* AI output is worth more than the other
three answers combined. The fourth question is domain-specific: where AI must not be trusted
*in audit*, not in general.

**DEL-10 — Four hours, do not over-optimize.**
Source: *"We expect this exercise to take no more than 4 hours total. Please do not over-optimize."*
Trap: a submission whose scope visibly exceeds four hours signals poor prioritisation, not
diligence. Equally a trap: hedging prose ("we could use X, or alternatively Y") to avoid
committing. Sharp decisions, stated once.

**DEL-11 — AWS is the house stack.**
Source: *"You may choose any cloud provider or stack (note we are an AWS shop)"*
Trap: reading the permission as an invitation. Choosing Kafka + GCP without a reason costs
credibility. The JD names SQS, SNS, DynamoDB, Lambda, EKS, S3 — use them, or justify not.

**DEL-12 — Diagrams and optional session history.**
Source: *"4. Any diagrams you created"* / *"3. (Optional) Session History"*
Trap: no diagram at all when diagrams are explicitly free of the page budget — that is
leaving free clarity on the table. Conversely, a decorative box-and-arrow that shows AWS
icons rather than the *mechanism* (who owns which data, where the region boundary falls).

---

## 2. Architecture and data ownership

**ARCH-1 — Do not put engagement data in the shared template database.**
Source: *"the product template database is shared between customer firms … This database does not currently retain any information about Engagement files"*
Trap: the obvious-looking solution — add a table mapping engagements to template versions
next to the templates — collapses tenant isolation and breaks residency in one move. This
sentence is the exercise's central boundary test; the brief flags it with *"Notably."*

**ARCH-2 — Engagement state lives in per-firm, per-region stores.**
Source: *"Engagement files … are stored in their own customer-specific databases"*
Trap: one global table of 800k engagement rows. Convenient, and a residency violation for
every EU and Canada firm.

**ARCH-3 — The publish path must not load engagements.**
Source: *"~1 minute per file; this should be treated as a hard constraint"* + *"within seconds of a template being published"*
Trap: fanning out to "check each engagement's current version" on publish. See §0.1 — this is
~333 downstream-hours per publish. A mediocre answer proposes it and then adds a thread pool,
as if concurrency closed a four-order-of-magnitude gap.

**ARCH-4 — Therefore: a queryable projection of engagement → (template, version, decision state).**
Source: *"the stored form of an engagement is not a directly queryable record of its current state"*
Trap: hand-waving the projection into existence. It needs a named owner, a write path (events
from the engagement system), a repair path (reconciliation), a residency story, and an answer
for what the UI shows while a row is missing or stale.

**ARCH-5 — Hook the publish at the source of truth, not in application code.**
Source: *"You may assume you can add hooks or events to any actions taken by either the product template storage system, or by the engagement management system"*
Trap: "the publishing service emits an SNS event after committing" — a dual write. If the
commit succeeds and the publish fails, that update is invisible forever. Transactional outbox
or CDC off the template DB.

**ARCH-6 — Event delivery is unreliable; design for it explicitly.**
Source: *"Address migration and backfill, unreliable event delivery … where they matter to your design"*
Trap: assuming exactly-once from SQS/SNS. Needs idempotency keys, at-least-once tolerance,
and a periodic reconciliation sweep that can detect and repair drift without a full backfill.
Second trap: claiming reconciliation exists without saying what it compares against, given
that reading ground truth costs one minute per engagement.

**ARCH-7 — The engagement-loading system is also the user-facing critical path.**
Source: *"The same system which loads engagements is also responsible for handling engagement creation, and for processing the user's accept/decline decision."*
Trap: treating the 1-minute load as merely slow instead of as a *shared, contended capacity
budget owned by another team*. Any backfill or refresh that saturates it degrades engagement
opens and accept/decline for live users. Needs an explicit capacity allocation and the
ability to yield.

**ARCH-8 — The skew is 1,000:1 and must be designed for.**
Source: *"the median firm has «~40», the largest «~40,000»"*
Trap: designing for the median. The 40k-firm is a hot partition on fan-out, will starve the
other 3,999 during backfill without fairness controls, and will not review 40,000
notifications one at a time — which raises firm-level or fleet-level decisioning as a real
product question, not a footnote.

**ARCH-9 — Residency: template content global, engagement linkage in-region.**
Source: *"some firms are contractually or legally required to keep their data in-region («EU», «Canada»). Product template content is not firm-specific."*
Trap: two opposite failures. (a) Not noticing that the second sentence is a *gift*: because
template content is not firm-specific, the diff and its LLM summary can be generated once,
globally, and replicated — no client-confidential data ever reaches a model. (b) Over-reacting
and standing up per-region inference and per-region template stores, multiplying cost for no
compliance gain. Say plainly which data classes cross regions and which never do.

**ARCH-10 — No maintenance window; everything is already live.**
Source: *"All of these systems are already live. There is no maintenance window"*
Trap: a migration plan with a cutover. Needs backward-compatible schema changes, shadow
writes, feature-flagged read paths, and a rollback that does not lose accumulated state.

**ARCH-11 — Must work for existing engagements, not just new ones.**
Source: *"the pending-update indicator needs to work for existing engagements, not only newly created ones"*
Trap: designing a clean create-time hook and relegating backfill to "we will also backfill."
Backfill is the expensive half: 13,333 engagement-hours against contended capacity. It needs
its own arithmetic, a prioritisation order (which firms/engagements first), resumability,
and a defined user experience while it is in flight.

**ARCH-12 — Observability and SLOs, stated as measurable objectives.**
Source: *"Address … observability and SLOs"*
Trap: "we will emit CloudWatch metrics and add dashboards." An SLO is a number: e.g. p99
staleness of the pending-update indicator, measured how, alerting at what threshold, with
what error budget. Also: the hardest thing to observe here is *silent* projection drift, which
no queue-depth dashboard will catch.

**ARCH-13 — Failure recovery, including what the user sees.**
Source: *"Address … failure recovery where they matter to your design"*
Trap: covering only infrastructure recovery. In an audit product, showing "no pending updates"
when the truth is unknown is worse than showing "checking" — a false negative here means a
firm never evaluated an update it was obliged to evaluate. Distinguish *unknown* from *none*
in the data model, not just in the UI.

---

## 3. Version semantics — the part most answers get wrong

**VER-1 — Versions form a graph, not a line.**
Source: *"Template versions do not form a simple line. Markets branch"*
Trap: `if (latest > current)`. Any integer or semver comparison is wrong the first time a
market branch exists. Needs lineage/ancestry: is the target version a descendant of the
engagement's base, on a branch this engagement is eligible for?

**VER-2 — Versions are withdrawn after publication.**
Source: *"versions are occasionally withdrawn after publication"*
Trap: an append-only event model with no retraction. Withdrawal must retract pending
indicators, invalidate the cached summary, and handle the case where a user already saw —
or already declined — the now-withdrawn version. Also: a withdrawal is itself an event that
must fan out within the same freshness budget, and it is the one case where being slow is
actively harmful.

**VER-3 — Define precisely what "declined" pins.**
Source: *"what "declined" actually pins"*
Trap: `declined: boolean`. The brief is explicitly asking for a semantic decision. Candidate
must choose and defend: does decline pin the engagement's base version (stay here), suppress
that specific target version, suppress that specific *change set*, or mute until something
materially new arrives? Each choice has a different failure mode, and the brief's next
sentence tests it.

**VER-4 — A declined version's changes can reappear inside a later version.**
Source: *"a firm that declined v5 may later be offered v7, which contains v5's changes"*
Trap: suppressing v7 because v5 was declined, which hides content the firm never evaluated;
or re-presenting v7 as if nothing had been declined, which loses the record of a deliberate
professional decision. Also a trap: computing the v7 diff against v5 — the engagement is not
on v5, it declined v5. The diff must be computed from the engagement's actual base.

**VER-5 — Several updates accumulate before the user decides.**
Source: *"Consider how you handle several updates accumulating before the user makes the accept/decline choice"*
Trap: presenting a stack of N per-version summaries and N decisions. The user needs one
squashed base→latest diff and one decision. Requires the summary cache to be keyed on the
*pair*, not on the publish event, and requires handling the case where a new publish arrives
while the user has the summary open.

**VER-6 — The stored version is the *creation* version, and may not be the effective one.**
Source: *"Engagement files currently store the template ID and version used for their creation."*
Trap: assuming that field tracks the current effective version after prior applies. The brief
says "for their creation" and puts applying out of scope — which leaves an ambiguity the
candidate should surface rather than silently assume away. The projection must own effective
base version going forward, fed by the accept/decline hook.

---

## 4. Summaries: generation *and evaluation*

**SUM-1 — Human-readable for non-technical audit professionals.**
Source: *"the users are non-technical and would strongly prefer something human-readable"*
Trap: rendering a prettier JSON diff. Also: summarising at the wrong altitude — an auditor
needs to know a materiality threshold changed or a required procedure was added, not that
1,400 nodes changed.

**SUM-2 — The LLM summarises the template diff, never engagement content.**
Source: *"Engagement content is client-confidential audit workpaper material"* + *"Product template content is not firm-specific"*
Trap: feeding engagement context to the model "for a more tailored summary." That converts a
clean compliance story into a client-confidentiality incident and a residency problem.

**SUM-3 — Inference cost scales with distinct version pairs, not engagements.**
Source: *"All-in run cost for the feature, inclusive of any inference spend"*
Trap: per-engagement generation. ~$277k/month versus ~$166/month (§0.1). Because template
content is not firm-specific, one summary per (product, base→target) serves every firm on
that pair — the cache is global and the hit rate is enormous.

**SUM-4 — A March summary must be defensible in a November regulatory review.**
Source: *"a summary shown to a user in March may need to be defended during a regulatory review in November"*
Trap: treating the summary as a cache entry. It is an audit artifact. It must be immutable
once shown, stored with the exact deterministic diff it was derived from, the prompt version,
the model identifier and version, generation timestamp, and a retention period that matches
audit retention — not 30 days of log retention. Specific failure this prevents: regenerating
on a cache miss in November with a newer model and producing text the firm never saw.

**SUM-5 — Evaluation, not just generation.**
Source: *"Generation and evaluation of the human-readable summaries"*
Trap: the most commonly skipped requirement in the brief. "Evaluation" is in the section
title. Needs a real strategy: groundedness checks against the deterministic diff (did the
summary assert a change that is not in the diff? did it omit a change flagged as material?),
a golden set with human-reviewed references, a regression gate before any prompt or model
change ships, and sampling with SME review in production. Saying "we will have humans review
outputs" is not an evaluation strategy.

**SUM-6 — The deterministic diff is the source of truth; the LLM is advisory.**
Source: *"AI at Caseware is not intended to replace professional judgment"* + *"a quick and reliable way to compare two different versions … and extract a JSON diff"*
Trap: reading the Context section as scene-setting rather than as a constraint. The design
must make the summary provably derived from, and traceable back to, the deterministic diff,
and must never place the LLM between the practitioner and the decision. The diff already
exists and is reliable — the model's only job is rendering it into language.

**SUM-7 — Separate material changes from cosmetic churn.**
Source: *"users need a summary of the pending inbound changes in order to make that apply/decline decision"*
Trap: summarising everything uniformly. The summary exists to support a specific decision.
Structural pre-processing of the JSON diff — classifying and filtering before the model sees
it — is both the quality lever and the token-cost lever, and it is deterministic and testable.

---

## 5. Part 2 — the fan-out worker

**IMP-1 — Bounded concurrency that respects another team's capacity.**
Source: *"the downstream call you depend on takes ~1 minute per engagement and is owned by another team with limited capacity"*
Trap: a fixed thread pool sized by intuition, per worker instance — which multiplies by the
number of instances and becomes an unintentional DDoS on a neighbouring team. The limit must
be global across workers, and ideally negotiated/configurable rather than hard-coded.
Adaptive backpressure on downstream signals beats a static number.

**IMP-2 — Idempotency.**
Source: *"Demonstrate appropriate handling of concurrency, failure, idempotency and downstream capacity"*
Trap: mentioning idempotency in the README without implementing it. There must be a real key,
a real store, and a stated answer for the crash-after-call-before-record window.

**IMP-3 — Failure taxonomy, retries, and poison handling.**
Source: same clause
Trap: retrying every exception identically. A validation failure retried 5 times is 5 wasted
minutes of a scarce downstream resource. Needs retryable/non-retryable classification,
exponential backoff with jitter, a retry budget, and a DLQ with an operator story.

**IMP-4 — A 1-minute blocking call has protocol consequences.**
Source: *"takes ~1 minute per engagement"*
Trap: SQS visibility timeout shorter than the call, producing duplicate in-flight work under
load — the precise scenario idempotency is supposed to cover, so getting this wrong while
claiming idempotency is a coherence failure. Also: graceful shutdown mid-flight, heartbeat
extension, and resumability of a partially-complete fan-out. Java 21+ virtual threads are a
natural fit for blocking I/O at this shape (JD prefers Java 21/25), but the bound must come
from downstream capacity, not from thread count.

**IMP-5 — Interfaces and contracts, testable in isolation.**
Source: *"Focus on interfaces, contracts and correctness"*
Trap: AWS SDK calls wired directly into business logic, so nothing can be tested without
mocking half of AWS. The downstream dependency, the rate limiter, and the idempotency store
should be interfaces with in-memory fakes.

**IMP-6 — Tests or validation logic.**
Source: *"tests or validation logic are a plus"*
Trap: skipping them because they are "a plus." For a component whose entire purpose is
correctness under concurrency and failure, tests are the demonstration. Most valuable: a test
that proves the concurrency cap holds, and one that proves a replayed publish issues no
duplicate downstream calls.

**IMP-7 — Part 2 must be coherent with Part 1.**
Source: *"Implement one narrow component … the template-publish fan-out worker"*
Trap: the single most common structural failure. Part 1 argues the publish path must never
make the 1-minute call; Part 2 then implements a worker that makes it per engagement, with no
explanation. The submission must state which flow this worker serves — backfill, reconciliation,
or a deliberate subset refresh — and why that flow tolerates the latency the indicator path
cannot.

**IMP-8 — Narrow scope.**
Source: *"This is not a full application build."*
Trap: Spring Boot scaffolding, Dockerfiles, and CI config that consume the four hours and
demonstrate nothing about concurrency or correctness.

---

## 6. Cost

**COST-1 — Name a budget; the brief left it blank on purpose.**
Source: *"must stay under «$X»/month"*
Trap: treating «$X» as unanswerable and skipping the section. This is a deliberate ambiguity
test — the expected move is to propose a defensible figure, state the reasoning (e.g. a
fraction of the feature's revenue or of existing platform spend), and show headroom against it.

**COST-2 — Identify the dominant term and its sensitivity.**
Source: *"show your arithmetic against the numbers above"*
Trap: a flat table of line items with no statement of which one matters. Infrastructure here
is single-digit dollars; inference dominates; and the inference term is driven almost entirely
by one architectural choice (per-pair versus per-engagement). State what happens if «$X» turns
out to be 10× smaller, and which lever moves first — smaller model, batch API, tighter diff
pre-filtering.

**COST-3 — Use only verified rates.**
Source: `docs/design/cost-inputs.md`
Trap: quoting DynamoDB on-demand writes at the stale $1.25/M, or guessing model pricing from
memory. Every rate must come from the cost-inputs table; regional premium 5–15% for
`eu-central-1` / `ca-central-1` should be acknowledged where work runs in-region.

**COST-4 — Backfill has a cost *and* a capacity price.**
Source: *"Address migration and backfill"* + the 1-minute constraint
Trap: costing only steady state. The one-off backfill is 13,333 engagement-hours of a
contended, externally-owned resource — the scarce currency is that team's capacity, not dollars.

---

## 7. Deliberate ambiguities the candidate must resolve out loud

The brief flags these with guillemets or with open phrasing. Silence on any of them reads as
not having noticed. Resolving one with a stated assumption and a reason reads as Staff-level.

1. **«$X» monthly budget.** Propose a number, justify it, show headroom. (COST-1)
2. **"within seconds."** Seconds from publish to *what*? Decompose: the pending-update
   *indicator* can be seconds and costs almost nothing; the human-readable *summary* requires
   generation and is read minutes to days later. Committing to seconds for both is expensive
   and buys nothing. This is the strongest candidate for DEL-6.
3. **"several updates accumulating."** One squashed decision from the engagement's base to
   latest, or a queue of per-version decisions? Pick one and say why. (VER-5)
4. **What "declined" pins.** Version, change set, or base? The brief asks directly. (VER-3)
5. **Does the stored template version update when an update is applied?** Applying is out of
   scope, but the record matters for computing the next diff. (VER-6)
6. **Withdrawal semantics.** Does withdrawing v6 revert a pending indicator? What if it was
   already applied — out of scope to apply, but not out of scope to *indicate*. (VER-2)
7. **"active" engagement files.** 800,000 is the active count. What about prior-year and
   archived files — out of scope for the indicator, or just out of scope for backfill?
   Bounding this shrinks the backfill.
8. **Branch eligibility.** Can an engagement created from one market's branch be offered an
   update from another branch? Presumably updates flow along a lineage; state the assumption.
9. **Who decides, at what granularity.** A firm with 40,000 engagements will not make 40,000
   decisions. Is there a firm-level or cohort-level policy? Product question, but it changes
   the data model. (ARCH-8)
10. **Does residency cover derived metadata?** An engagement ID plus a template version plus a
    pending flag is arguably firm data. The conservative answer — keep the projection in-region —
    should be stated as a choice, not assumed silently. (ARCH-9)
11. **Retention period for summaries.** "November" implies at least a year; audit retention is
    typically far longer. Name a number. (SUM-4)
12. **Localisation.** Content teams work "in different markets" and the platform ships in 16
    languages. Are summaries needed per locale? That multiplies the inference term directly
    and nothing in the brief settles it.

---

## 8. Level check — what "Staff" means for this submission

From the job description, the traits actually being assessed:

- *"reduce ambiguity"* — §7 is where this is demonstrated or missed.
- *"influence across teams"* — ARCH-7 and IMP-1 are both negotiations with another team's
  capacity. The submission should read as though that conversation is anticipated.
- *"Define standards and guardrails for how AI-assisted and agentic tooling is used"* — DEL-9
  is not a formality; it maps to a named core responsibility.
- *"balance delivery speed with long-term platform health"* — DEL-5 and DEL-10.
- *"Clarity of communication"* is an explicit evaluation criterion. Anything a reviewer has to
  read twice is a defect in the artifact, not in the reader. Adversarial reviewers in this
  workflow should flag and simplify hard-to-follow passages on every pass.
