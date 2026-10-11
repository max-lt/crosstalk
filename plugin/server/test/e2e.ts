// Two channel processes pair and exchange messages through a hub.
// Usage: CROSSTALK_HUB=http://127.0.0.1:8787 bun test/e2e.ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { z } from 'zod'

const hub = process.env.CROSSTALK_HUB
if (!hub) throw new Error('set CROSSTALK_HUB')
const home = mkdtempSync(join(tmpdir(), 'crosstalk-e2e-'))
const main = join(import.meta.dir, '..', 'src', 'main.ts')

const ChannelEvent = z.object({
  method: z.literal('notifications/claude/channel'),
  params: z.object({ content: z.string(), meta: z.record(z.string(), z.string()).optional() }),
})
type Event = z.infer<typeof ChannelEvent>['params']

class Peer {
  client: Client
  events: Event[] = []
  private waiters: ((e: Event) => void)[] = []

  constructor(readonly session: string) {
    this.client = new Client({ name: `e2e-${session}`, version: '0' })
  }

  async start(): Promise<void> {
    this.client.setNotificationHandler(ChannelEvent, n => {
      this.events.push(n.params)
      this.waiters.splice(0).forEach(w => w(n.params))
    })
    const transport = new StdioClientTransport({
      command: 'bun',
      args: [main],
      env: { ...process.env, CROSSTALK_HOME: home, CROSSTALK_SESSION: this.session, CROSSTALK_HUB: hub! },
      stderr: 'pipe',
    })
    transport.stderr?.on('data', (chunk: Buffer) => process.stderr.write(`[${this.session}] ${chunk}`))
    await this.client.connect(transport)
  }

  async call(name: string, args: Record<string, unknown> = {}): Promise<string> {
    const result = await this.client.callTool({ name, arguments: args })
    const body = (result.content as { text: string }[]).map(c => c.text).join('\n')
    if (result.isError) throw new Error(`${name}: ${body}`)
    return body
  }

  nextEvent(timeoutMs = 10_000): Promise<Event> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${this.session}: no event within ${timeoutMs}ms`)), timeoutMs)
      this.waiters.push(e => {
        clearTimeout(timer)
        resolve(e)
      })
    })
  }

  async waitConnected(): Promise<string> {
    for (let i = 0; i < 100; i++) {
      const who = await this.call('crosstalk_whoami')
      if (who.includes('connected')) return who.split('\n')[0]
      await Bun.sleep(100)
    }
    throw new Error(`${this.session} never connected to the hub`)
  }

  async stop(): Promise<void> {
    await this.client.close()
  }
}

function expect(cond: unknown, what: string): void {
  if (!cond) throw new Error(`expected ${what}`)
  console.log(`ok  ${what}`)
}

const a = new Peer('alice')
const b = new Peer('bob')
await Promise.all([a.start(), b.start()])
const [aId, bId] = await Promise.all([a.waitConnected(), b.waitConnected()])
expect(aId.startsWith('crosstalk_') && bId.startsWith('crosstalk_') && aId !== bId, 'two distinct ids')

// Pairing.
const introEvent = b.nextEvent()
const introStatus = await a.call('crosstalk_connect', { target: bId, description: 'Alice, working on the hub' })
expect(introStatus.includes('delivered'), `intro delivered: ${introStatus}`)
const intro = await introEvent
expect(intro.meta?.kind === 'intro' && intro.meta.from === aId && intro.content === 'Alice, working on the hub', 'bob got the intro')

let threw = false
try {
  await b.call('crosstalk_send', { to: aId, text: 'too early' })
} catch {
  threw = true
}
expect(threw, 'send before accept is refused locally')

const acceptedEvent = a.nextEvent()
await b.call('crosstalk_accept', { id: aId, description: 'Bob, on the plugin' })
const accepted = await acceptedEvent
expect(accepted.meta?.kind === 'accepted' && accepted.content === 'Bob, on the plugin', 'alice got the acceptance')
expect((await a.call('crosstalk_peers')).includes(`accepted    ${bId}`), 'alice lists bob as accepted')

// Messages both ways.
const toBob = b.nextEvent()
expect((await a.call('crosstalk_send', { to: bId, text: 'hello bob' })) === 'delivered', 'alice -> bob delivered')
const m1 = await toBob
expect(m1.meta?.kind === 'message' && m1.content === 'hello bob' && m1.meta.peer === 'Alice, working on the hub', 'bob got the message with peer description')
const toAlice = a.nextEvent()
await b.call('crosstalk_send', { to: aId, text: 'hi alice' })
expect((await toAlice).content === 'hi alice', 'alice got the reply')

// Identity survives a restart of the channel process.
await b.stop()
const b2 = new Peer('bob')
await b2.start()
expect((await b2.waitConnected()) === bId, 'bob keeps his id after restart')
const toBob2 = b2.nextEvent()
expect((await a.call('crosstalk_send', { to: bId, text: 'still there?' })) === 'delivered', 'alice -> restarted bob delivered')
expect((await toBob2).content === 'still there?', 'restarted bob got the message')

// Forget.
const forgotEvent = a.nextEvent()
await b2.call('crosstalk_forget', { id: aId })
expect((await forgotEvent).meta?.kind === 'forgot', 'alice learned bob forgot her')
threw = false
try {
  await a.call('crosstalk_send', { to: bId, text: 'gone' })
} catch {
  threw = true
}
expect(threw, 'send to a forgotten pairing is refused')

// Offline peer.
await b2.stop()
await Bun.sleep(300)
const a2status = await a.call('crosstalk_connect', { target: bId, description: 'Alice again' })
expect(a2status.includes('offline'), `intro to offline peer reports offline: ${a2status}`)

await a.stop()
console.log('\nall good')
process.exit(0)
