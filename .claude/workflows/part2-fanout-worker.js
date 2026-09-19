export const meta = {
  name: 'part2-fanout-worker',
  description: 'Implement, adversarially review and document the Part 2 template-publish fan-out worker in Java',
  whenToUse: 'Run after the design document settles, since the worker has to match the design it came from. Takes no args.',
  phases: [
    { title: 'Implement', detail: 'Java 21 Maven module, interfaces and contracts first, with tests' },
    { title: 'Review', detail: 'Three adversarial reviewers: concurrency, contracts, tests' },
    { title: 'Fix', detail: 'Apply the findings and get the build green' },
    { title: 'Document', detail: 'README explaining the implementation tradeoffs' },
  ],
}

const REPO = '/home/user/caseware'
const BRIEF = REPO + '/docs/assignment/take-home-assignment.txt'
const BRIEF_PDF = '/root/.claude/uploads/72cbc24f-e5b0-5a9a-abc2-fc5b5058f5cc/213b5058-Staff_Java_Developer_-_Take-Home_Test_2.pdf'
const DOC = REPO + '/docs/design.md'
const MODULE = REPO + '/fanout-worker'

const GROUND_RULE = [
  'GROUNDING RULE, non-negotiable: before you write anything, read the assignment brief at',
  BRIEF + ' (the original PDF is at ' + BRIEF_PDF + '), and read the design document at ' + DOC,
  'so the worker matches the system it belongs to. Part 2 is what the brief asks for in its own words:',
  'implement one narrow component in Java, the template-publish fan-out worker. A single publish may',
  'require processing a large number of engagements, and the downstream call it depends on takes about',
  'one minute per engagement and is owned by another team with limited capacity.',
  'The brief asks for appropriate handling of concurrency, failure, idempotency and downstream capacity,',
  'a focus on interfaces, contracts and correctness, and says tests or validation logic are a plus.',
  'It also says, in its own words, that this is not a full application build.',
].join(' ')

const SCOPE = [
  'SCOPE DISCIPLINE. This is a 4 hour exercise and the reviewer reads the code in fifteen minutes.',
  'No Spring, no AWS SDK wiring, no infrastructure. The downstream engagement system, the projection',
  'store and the queue are interfaces the worker depends on, with small in-memory fakes in the tests.',
  'Java 21, Maven, JUnit 5. Prefer the standard library. Anything that is not the fan-out worker is out',
  'of scope; say so in the README instead of building it.',
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
          file: { type: 'string' },
          where: { type: 'string', description: 'class, method or line' },
          problem: { type: 'string' },
          failureSequence: { type: 'string', description: 'the interleaving or sequence of events that produces the bug' },
          fix: { type: 'string' },
        },
        required: ['severity', 'file', 'problem', 'fix'],
      },
    },
    verdict: { type: 'string' },
  },
  required: ['findings', 'verdict'],
}

phase('Implement')
const impl = await agent([
  GROUND_RULE,
  SCOPE,
  'You are writing the implementation. Create a Maven module at ' + MODULE + ' (Java 21, JUnit 5).',
  'Design the interfaces first and let them carry the contract: what a publish job is, what one unit of',
  'work is, what the downstream call looks like, what the worker promises about each.',
  'The behaviour that matters, and what the reviewer will look for:',
  '- Bounded concurrency against a downstream with limited capacity, with the cap owned in one place and',
  '  shared across all in-flight publishes rather than per publish.',
  '- Idempotency: the same publish delivered twice, or a worker restarted mid-run, must not double-process',
  '  an engagement. Be explicit about the key and about where the dedup state lives.',
  '- Failure handling that distinguishes retryable from terminal, with backoff, a retry ceiling and a',
  '  dead-letter path. A permanently failing engagement must not stall the rest of the publish.',
  '- Backpressure and fairness: one firm with 40,000 engagements must not starve a firm with 40.',
  '- Graceful shutdown and progress that survives a restart, since a large publish outlives one process.',
  '- Observability hooks at the points an on-call engineer would actually want them.',
  'Write tests that prove the properties rather than the happy path: duplicate delivery, restart mid-run,',
  'a downstream that rejects over its cap, a poison engagement, cancellation, and the fairness claim.',
  'Run the build yourself with: cd ' + MODULE + ' && mvn -q -B test. It must pass before you finish.',
  'Return a summary of the interfaces you defined and the decisions a reviewer would question.',
].join('\n'), { model: 'fable', effort: 'xhigh', label: 'implement:worker' })

const LENSES = [
  {
    key: 'concurrency',
    model: 'fable',
    brief: [
      'You are the adversarial concurrency reviewer. Try to break this worker. Look for: races on shared',
      'state; a concurrency cap that is not actually global; a semaphore or permit leaked on an exception',
      'path; interrupt and cancellation handling that loses work or swallows the flag; unbounded queue',
      'growth; deadlock or livelock between the shutdown path and in-flight work; a retry that re-enters',
      'the limiter without releasing; work that is silently dropped when the pool rejects it; and progress',
      'state written in an order that is not crash-safe. For every finding give the concrete interleaving.',
    ].join(' '),
  },
  {
    key: 'contracts',
    model: 'opus',
    brief: [
      'You are the contracts and correctness reviewer. The brief asks for interfaces, contracts and',
      'correctness, so judge those first. Is each interface small, honest about what it promises, and',
      'implementable by a real downstream? Is the idempotency key correct under a template withdrawal and',
      'under two publishes of the same product racing? Does the code do what its javadoc says? Are errors',
      'modelled or stringly-typed? Is anything here out of scope for a narrow component, or missing that a',
      'reviewer would consider essential? Flag anything the design document promises that the code',
      'contradicts.',
    ].join(' '),
  },
  {
    key: 'tests',
    model: 'opus',
    brief: [
      'You are the test reviewer, and you are always vigilant about code that is hard to follow: anything',
      'a reviewer would have to read twice is a defect, and the fix is to simplify it. Check that the tests',
      'prove the claimed properties rather than restate the implementation, that they would actually fail',
      'if the property broke, and that they are deterministic rather than sleep-based. Check the naming and',
      'the shape of the code as a fifteen-minute read. Name any claimed property with no test behind it.',
      'For each finding, give the simpler version, not just the complaint.',
    ].join(' '),
  },
]

phase('Review')
const reviews = await parallel(LENSES.map(function (lens) {
  return function () {
    return agent([
      GROUND_RULE,
      SCOPE,
      lens.brief,
      'The code under review is the Maven module at ' + MODULE + '. Read all of it, and run',
      'cd ' + MODULE + ' && mvn -q -B test yourself rather than trusting that it passes.',
      'Every finding needs a concrete fix written as the change to make. Do not pad the list.',
    ].join('\n'), { model: lens.model, effort: 'high', phase: 'Review', label: 'review:' + lens.key, schema: FINDINGS_SCHEMA })
  }
}))

const live = reviews.filter(Boolean)
const findings = live.flatMap(function (r) { return r.findings })
const blocking = findings.filter(function (f) { return f.severity === 'blocking' }).length
log(findings.length + ' findings (' + blocking + ' blocking)')

phase('Fix')
const fixed = await agent([
  GROUND_RULE,
  SCOPE,
  'You are the author fixing ' + MODULE + ' after review. The findings, as JSON:',
  JSON.stringify(findings, null, 1),
  'The verdicts:',
  JSON.stringify(live.map(function (r) { return r.verdict }), null, 1),
  'Apply every blocking and major finding, and the minor ones that make the code simpler. You may reject',
  'a finding if it is wrong or out of scope, and you must say which and why.',
  'Where a reviewer says something is hard to follow, simplify it rather than commenting it.',
  'Add a regression test for every blocking finding that was a real bug.',
  'The build must be green when you are done: cd ' + MODULE + ' && mvn -q -B test.',
  'Return what you changed, what you rejected and why, and the final test count.',
].join('\n'), { model: 'opus', effort: 'high', label: 'fix:worker' })

phase('Document')
const readme = await agent([
  GROUND_RULE,
  'Write ' + MODULE + '/README.md. The brief asks, in its own words, for a short README explaining the key',
  'implementation tradeoffs. Short means short: one page.',
  'Cover how to run it, the interfaces and what each one promises, and then the tradeoffs that a reviewer',
  'would otherwise ask about in the live design review: how concurrency is capped and why there, what the',
  'idempotency key is and what it does not protect against, what happens on restart, how a poison',
  'engagement is isolated, how fairness across firms is enforced, and what was deliberately left out',
  'because this is not a full application build.',
  'Base it on the code as it now stands; read it first. Do not describe behaviour the code does not have.',
  'Write plainly. No marketing, no bullet lists of adjectives.',
  'What the implementer and the fixer reported, for context:',
  JSON.stringify({ implementation: impl, fixes: fixed }, null, 1),
].join('\n'), { model: 'opus', effort: 'high', label: 'document:readme' })

return { findings: findings.length, blocking: blocking, fixed: fixed, readme: readme }
