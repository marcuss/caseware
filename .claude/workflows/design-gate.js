export const meta = {
  name: 'design-gate',
  description: 'Final grounding check on the design document before submission',
  whenToUse: 'Run last, after humanizing. Takes no args. Fixes blocking problems in place.',
  phases: [{ title: 'Gate', detail: 'Re-read the brief, then judge the document as the hiring reviewer' }],
}

const REPO = '/home/user/caseware'
const BRIEF = REPO + '/docs/assignment/take-home-assignment.txt'
const BRIEF_PDF = '/root/.claude/uploads/72cbc24f-e5b0-5a9a-abc2-fc5b5058f5cc/213b5058-Staff_Java_Developer_-_Take-Home_Test_2.pdf'
const COSTS = REPO + '/docs/design/cost-inputs.md'
const DOC = REPO + '/docs/design.md'

phase('Gate')
const gate = await agent([
  'GROUNDING RULE, non-negotiable: read the assignment brief at ' + BRIEF + ' end to end before you',
  'read the document (the original PDF is at ' + BRIEF_PDF + '). Rates live in ' + COSTS + '; never invent one.',
  'You are the final gate, and you are the last reader before this is submitted.',
  'Then read ' + DOC + ' end to end.',
  'The document is 2 to 4 pages maximum, diagrams excluded, which is roughly 1,600 to 2,400 words of prose.',
  'It must cover architecture and data ownership; correctness and production evolution; scale, cost and',
  'operational characteristics with the arithmetic shown; generation and evaluation of the human-readable',
  'summaries, including a summary shown in March being defended in a regulatory review in November;',
  'and key tradeoffs, assumptions and the one requirement the author would challenge. It must also address',
  'migration and backfill, unreliable event delivery, observability and SLOs, data residency and failure',
  'recovery where they matter, name the riskiest part of the design, and name what was deliberately left out.',
  'Answer four questions and nothing else. One: name any required item that is still missing or thin.',
  'Two: name anything the humanizing pass damaged, meaning a claim, number or decision that changed or',
  'went vague. Three: give the measured prose word count and say whether it is inside the limit.',
  'Four: give your honest verdict as the hiring reviewer, including the weakest paragraph in the document.',
  'If you find a blocking problem, fix it in the file yourself, minimally, and say what you fixed.',
].join('\n'), { model: 'fable', effort: 'xhigh', label: 'gate:final' })

return { gate: gate }
