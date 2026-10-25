// Run from any directory: bun test/codex-attach.ts
// Uses the checkout debug binary, temporary state, and no external hub.
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { mkdtempSync, writeFileSync, readFileSync, readdirSync, rmSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { createHash } from 'node:crypto'
import { createServer } from 'node:net'
import assert from 'node:assert/strict'

const bin = process.env.CROSSTALK_BIN ?? resolve(import.meta.dir, '../client/target/debug/crosstalk')
const pluginRoot = resolve(import.meta.dir, '../plugin-codex')
const mcp = JSON.parse(readFileSync(join(pluginRoot, '.mcp.json'), 'utf8')).mcpServers.crosstalk
const home = mkdtempSync('/tmp/crosstalk-attach-')
const config = join(home, 'config.json')
const a = '11111111-1111-4111-8111-111111111111'
const b = '22222222-2222-4222-8222-222222222222'
const missing = '33333333-3333-4333-8333-333333333333'
const wrong = '44444444-4444-4444-8444-444444444444'
const fake = '55555555-5555-4555-8555-555555555555'
writeFileSync(config, JSON.stringify({ codex: '/usr/bin/false', sessions: { [a]: 'codex' } }))
const env = { ...process.env, CROSSTALK_HOME: home, CROSSTALK_HUB: 'http://127.0.0.1:1', CROSSTALK_SESSION: 'codex' }
const children: ReturnType<typeof Bun.spawn>[] = []
const client = new Client({ name: 'codex-attach-test', version: '1' })
// Exercise the packaged launcher, not just the binary. Codex resolves a relative
// cwd from the plugin root; legacy .mcp.json args do not expand PLUGIN_ROOT.
const transport = new StdioClientTransport({
  command: mcp.command,
  args: mcp.args,
  cwd: resolve(pluginRoot, mcp.cwd ?? '.'),
  env: { ...env, CROSSTALK_BIN: bin, CROSSTALK_CODEX_CONFIG: config },
  stderr: 'pipe',
})
transport.stderr?.on('data', () => {})
let mock: ReturnType<typeof createServer> | undefined

async function cli(session: string, ...args: string[]) {
  const p = Bun.spawn([bin, ...args], { env: { ...env, CROSSTALK_SESSION: session }, stdout: 'pipe', stderr: 'pipe' })
  const [out, err, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited])
  if (code) throw new Error(err)
  return out.trim()
}
async function daemon(session: string, thread: string) {
  const child = Bun.spawn([bin, 'daemon', '--codex-thread', thread], { env: { ...env, CROSSTALK_SESSION: session }, stdin: 'ignore', stdout: 'ignore', stderr: 'ignore' })
  children.push(child)
  for (let i = 0; i < 100; i++) {
    try { const s = JSON.parse(await cli(session, 'status')); assert.equal(s.threadId, thread); return child } catch {}
    if (child.exitCode !== null) throw new Error('daemon exited')
    await Bun.sleep(30)
  }
  throw new Error('daemon startup timeout')
}
async function call(name: string, args: Record<string, unknown>) {
  const result = await client.callTool({ name, arguments: args })
  const text = (result.content as { text: string }[]).map(c => c.text).join('\n')
  return { error: !!result.isError, text }
}
async function rejected(args: Record<string, unknown>, pattern: RegExp) {
  const r = await call('crosstalk_whoami', args)
  assert.equal(r.error, true, r.text)
  assert.match(r.text, pattern)
}
async function identity(thread: string) {
  const r = await call('crosstalk_whoami', { thread_id: thread })
  assert.equal(r.error, false, r.text)
  return r.text.split('\n')[0]
}
function ok(label: string) { console.log('ok  ' + label) }

try {
  await client.connect(transport)
  assert.equal(client.getServerCapabilities()?.experimental?.['claude/channel'], undefined)
  assert.deepEqual(readdirSync(home), ['config.json'])
  const listed = await client.listTools()
  assert(!listed.tools.some(t => t.name === 'crosstalk_inbox'))
  for (const tool of listed.tools) assert(tool.inputSchema.required?.includes('thread_id'))
  ok('attach creates no identity or daemon; every tool requires thread_id; inbox hidden')
  await rejected({}, /thread_id is required/)
  await rejected({ thread_id: '../../codex' }, /thread|uuid|invalid/i)
  await rejected({ thread_id: 123 }, /thread_id is required/)
  await rejected({ thread_id: missing }, /no crosstalk daemon/)
  ok('missing, malformed, non-string thread and absent daemon fail closed')

  const first = await daemon('codex', a)
  await daemon('codex-' + b, b)
  const [aid, bid] = await Promise.all([identity(a), identity(b)])
  assert.match(aid, /^crosstalk_/)
  assert.notEqual(aid, bid)
  assert.equal(aid, (await cli('codex', 'whoami')).split('\n')[0])
  assert.equal(bid, (await cli('codex-' + b, 'whoami')).split('\n')[0])
  await rejected({ thread_id: missing }, /no crosstalk daemon/)
  ok('one MCP process routes two chats independently, including legacy codex mapping; no environment fallback')
  const inbox = await call('crosstalk_inbox', { thread_id: a })
  assert.equal(inbox.error, true)
  ok('direct calls cannot drain inbox either')

  await daemon('codex-' + wrong, b)
  await rejected({ thread_id: wrong }, /not the daemon of this chat/)
  ok('wrong thread owner refused')

  mkdirSync(join(home, 'run'), { recursive: true })
  const path = join(home, 'run', createHash('sha256').update('codex-' + fake).digest('hex').slice(0, 16) + '.sock')
  let mockStatus = { daemon: true, session: 'other', threadId: fake }
  const commands: string[] = []
  mock = createServer(socket => {
    let data = ''
    socket.on('data', chunk => {
      data += chunk
      if (!data.includes('\n')) return
      const { cmd } = JSON.parse(data)
      commands.push(cmd)
      socket.end(cmd === 'ping' ? 'pong\n' : JSON.stringify(mockStatus) + '\n')
    })
  })
  await new Promise<void>((resolve, reject) => { mock!.once('error', reject); mock!.listen(path, resolve) })
  await rejected({ thread_id: fake }, /not the daemon of this chat/)
  mockStatus = { daemon: false, session: 'codex-' + fake, threadId: fake }
  await rejected({ thread_id: fake }, /not the daemon of this chat/)
  assert(!commands.includes('whoami'))
  ok('wrong session and MCP-mode owner refused before forwarding command')

  await cli('codex', 'stop')
  await first.exited
  await rejected({ thread_id: a }, /no crosstalk daemon/)
  assert.equal(await identity(b), bid)
  await daemon('codex', a)
  assert.equal(await identity(a), aid)
  ok('restart keeps identity and same MCP bridge reconnects; other chat unaffected')
  console.log('All Codex attach tests passed')
} finally {
  await client.close().catch(() => {})
  await transport.close().catch(() => {})
  if (mock) await new Promise<void>(resolve => mock!.close(() => resolve()))
  for (const child of children) if (child.exitCode === null) child.kill('SIGTERM')
  await Promise.all(children.map(child => child.exited))
  rmSync(home, { recursive: true, force: true })
}
