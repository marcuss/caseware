export const meta = {
  name: 'design-review-1',
  description: 'Adversarial review round 1 over the design document (requirement coverage and architectural soundness), followed by a revision',
  whenToUse: 'Run first, on the synthesized document. Takes no args; the round number is fixed in this script.',
  phases: [
    { title: 'Review', detail: 'Three independent adversarial reviewers: grounding, technical skeptic, clarity' },
    { title: 'Revise', detail: 'Apply the findings to the document' },
  ],
}

const REPO = '/home/user/caseware'
const BRIEF = REPO + '/docs/assignment/take-home-assignment.txt'
const BRIEF_PDF = '/root/.claude/uploads/72cbc24f-e5b0-5a9a-abc2-fc5b5058f5cc/213b5058-Staff_Java_Developer_-_Take-Home_Test_2.pdf'
const COSTS = REPO + '/docs/design/cost-inputs.md'
const CHECKLIST = REPO + '/docs/design/requirements-checklist.md'
const DOC = REPO + '/docs/design.md'

const round = 1

const GROUND_RULE = [
  'GROUNDING RULE, non-negotiable: before you write anything, read the assignment brief at',
  BRIEF,
  '(the original PDF is at ' + BRIEF_PDF + ' if you want to check the text extraction).',
  'Also read ' + COSTS + ' for verified AWS and Anthropic rates; never invent a rate.',
  'The brief is the only authority on what is being asked. Quote it when you disagree with someone.',
].join(' ')

const CHECKLIST_NOTE = 'A requirements checklist built from the brief is at ' + CHECKLIST + '. Read it, but treat the brief itself as the authority.'

const HARD_LIMITS = [
  'HARD LIMITS from the brief: the design document is 2 to 4 pages maximum, diagrams excluded.',
  'Treat that as roughly 1,600 to 2,400 words of prose. The brief says, in its own words, that they',
  'would rather see a short document with sharp decisions than a long one that hedges.',
  'The whole exercise is meant to take a candidate 4 hours. Every section must earn its space.',
  'The document must cover all five required items: (1) architecture and data ownership,',
  '(2) correctness and production evolution, (3) scale, cost and operational characteristics with',
  'arithmetic shown, (4) generation and evaluation of the human-readable summaries, (5) key tradeoffs,',
  'assumptions, and the one requirement the author would challenge. It must also address migration and',
  'backfill, unreliable event delivery, observability and SLOs, data residency, and failure recovery',
  'where they matter, name the riskiest part of the design, and name what was deliberately left out.',
  'On the summaries: a summary shown to a user in March may need to be defended in a regulatory review',
  'in November.',
].join(' ')

const FINDINGS_SCHEMA = {
  type: 'object',
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          severity: { type: 'string', enum: ['blocking', 'major', 'minor'] },
          lens: { type: 'string' },
          where: { type: 'string', description: 'section or quoted phrase in the document' },
          problem: { type: 'string', description: 'what is wrong, specifically' },
          fix: { type: 'string', description: 'the concrete change to make' },
        },
        required: ['severity', 'where', 'problem', 'fix'],
      },
    },
    verdict: { type: 'string', description: 'one paragraph: would a Caseware staff-level reviewer advance this candidate, and why' },
    wordCount: { type: 'number', description: 'measured prose word count, diagrams and code blocks excluded' },
  },
  required: ['findings', 'verdict'],
}

const LENSES = [
  {
    key: 'grounding',
    model: 'opus',
    brief: [
      'You are the grounding reviewer. Re-read the assignment brief at ' + BRIEF + ' start to finish',
      'before you read the document; do not review from memory. Then go requirement by requirement',
      'through ' + CHECKLIST + ' and check the document against each one.',
      'Flag anything the brief asks for that is missing, answered in passing, or quietly redefined into',
      'something easier. Flag any place the document contradicts a stated constraint. Flag invented facts:',
      'numbers, product behaviours or guarantees that appear nowhere in the brief and are not labelled as',
      'assumptions. Be literal and unsentimental.',
    ].join(' '),
  },
  {
    key: 'technical-skeptic',
    model: 'fable',
    brief: [
      'You are the adversarial technical reviewer, and your job is to try to break this design, not to',
      'admire it. Attack in this order: correctness under concurrent and out-of-order events; what happens',
      'when a template is withdrawn after publication; what a decline actually pins when versions branch and',
      'a later version supersedes a declined one; duplicate and lost events; the backfill racing live traffic;',
      'the projection drifting from the engagements it describes and how anyone would ever notice;',
      'residency leaks where firm-scoped data crosses a region boundary; and the arithmetic, which you should',
      'recompute yourself against ' + COSTS + ' rather than trust. State the concrete sequence of events that',
      'produces a wrong answer for a user. A finding without a failure sequence is not a finding.',
    ].join(' '),
  },
  {
    key: 'clarity',
    model: 'opus',
    brief: [
      'You are the clarity reviewer, and you are always vigilant. Your standing mandate: anything explained',
      'in a way that is hard to follow is a defect, and the fix is to simplify. Read the document as a busy',
      'reviewer who has read the brief once and has fifteen minutes.',
      'Flag: sentences you had to read twice; a term used before it is defined; an idea explained abstractly',
      'that a concrete example would settle in one line; a diagram that does not match the prose beside it;',
      'a section that buries its decision under context; padding, hedging and throat-clearing; any passage',
      'where the reader cannot tell what was decided. For each one, give the simpler version, not just the',
      'complaint. Also measure the prose word count and report it, and flag over-length as blocking,',
      'naming which section should lose the words.',
    ].join(' '),
  },
]

const EMPHASIS = {
  1: 'This round weights requirement coverage and architectural soundness above polish. A missing requirement outranks an awkward sentence.',
  2: 'This round weights the arithmetic, the scale and cost section, and failure recovery. Recompute every number in the document. An unsupported number is a blocking finding.',
  3: 'This round weights the staff-level signal and the reading experience. Judge it as the hiring reviewer: are the decisions sharp, are the tradeoffs owned, is the challenged requirement well argued, and does it come in under the page limit? Nothing may be left vague at this point.',
}

phase('Review')
const reviews = await parallel(LENSES.map(function (lens) {
  return function () {
    return agent([
      GROUND_RULE,
      CHECKLIST_NOTE,
      HARD_LIMITS,
      lens.brief,
      EMPHASIS[round],
      'The document under review is ' + DOC + '. Read all of it.',
      round > 1 ? 'Earlier rounds have already run. Do not re-file a finding that the document now handles; check first. Regressions introduced by the previous revision are fair game and should be marked blocking.' : '',
      'Return findings ordered most severe first. Be specific about location. Every finding needs a',
      'concrete fix, written as the change to make, not as advice. Do not pad the list: a short list of',
      'real defects is worth more than a long list of preferences. If something is genuinely good, say',
      'so in the verdict rather than inventing a complaint about it.',
    ].join('\n'), { model: lens.model, effort: 'high', phase: 'Review', label: 'r' + round + ':' + lens.key, schema: FINDINGS_SCHEMA })
  }
}))

const live = reviews.filter(Boolean)
const findings = live.flatMap(function (r) { return r.findings })
const verdicts = live.map(function (r) { return r.verdict })
const blocking = findings.filter(function (f) { return f.severity === 'blocking' }).length
const words = live.map(function (r) { return r.wordCount }).filter(Boolean)
log('Round ' + round + ': ' + findings.length + ' findings (' + blocking + ' blocking)' + (words.length ? ', word count ~' + Math.round(words.reduce(function (a, b) { return a + b }, 0) / words.length) : ''))

phase('Revise')
const revision = await agent([
  GROUND_RULE,
  CHECKLIST_NOTE,
  HARD_LIMITS,
  'You are the author revising ' + DOC + ' after review round ' + round + '.',
  'Three reviewers went at it independently. Their findings, as JSON:',
  JSON.stringify(findings, null, 1),
  'Their verdicts:',
  JSON.stringify(verdicts, null, 1),
  'Apply every blocking and major finding. Apply the minor ones that make the document shorter or',
  'clearer. You may reject a finding, but only if it is wrong or it contradicts the brief, and you must',
  'say which and why in your return value.',
  'Two standing rules while you revise. First, the document must not grow: if a fix adds words, find the',
  'words elsewhere. Second, when a reviewer says a passage is hard to follow, rewrite the passage rather',
  'than appending a clarification to it.',
  'Write the revised document back to ' + DOC + '. Return: what you changed, what you rejected and why,',
  'and the new prose word count.',
].join('\n'), { model: 'opus', effort: 'high', phase: 'Revise', label: 'revise:r' + round })

return { round: round, findings: findings.length, blocking: blocking, verdicts: verdicts, revision: revision }
