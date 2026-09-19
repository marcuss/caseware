export const meta = {
  name: 'design-humanize',
  description: 'Apply the blader/humanizer skill to the finished design document without changing any claim',
  whenToUse: 'Run once, after the review rounds have settled the content. Takes no args.',
  phases: [{ title: 'Humanize', detail: 'File-mode pass over docs/design.md' }],
}

const REPO = '/home/user/caseware'
const BRIEF = REPO + '/docs/assignment/take-home-assignment.txt'
const DOC = REPO + '/docs/design.md'
const HUMANIZER = '/tmp/claude-0/-home-user-caseware/72cbc24f-e5b0-5a9a-abc2-fc5b5058f5cc/scratchpad/humanizer/SKILL.md'

phase('Humanize')
const result = await agent([
  'You are applying the humanizer skill to a finished technical document.',
  'Read the skill in full first: ' + HUMANIZER + '. It is the authority on what to change.',
  'If that path does not exist, clone it: git clone --depth 1 https://github.com/blader/humanizer.git into a',
  'temporary directory and read its SKILL.md.',
  'Operate in its file mode on ' + DOC + ': run the full process, write only the final text back to the',
  'file, and return a short summary of the patterns you removed.',
  'Domain constraints that override nothing in the skill but bound your edits: this is a technical design',
  'document submitted for a staff engineering role, so the voice stays plain and neutral. Do not change a',
  'single claim, number, name, or decision. Leave Mermaid blocks, code blocks, tables and inline code',
  'untouched. Every requirement the document currently satisfies must still be satisfied when you are done.',
  'Then read the brief at ' + BRIEF + ' once more and confirm the humanized version still answers it.',
].join('\n'), { model: 'opus', effort: 'high', label: 'humanize:blader' })

return { result: result }
