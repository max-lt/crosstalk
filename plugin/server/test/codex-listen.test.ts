import { expect, test } from 'bun:test'
import { Identity } from '../src/crypto'
import { queueMessage } from '../src/codex-listen'
import { target, checkOwner } from '../../codex/channel'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('launcher requires an exact chat and refuses another chat owner', () => {
  const thread = '01a14655-601c-7b32-b401-40252592968e'
  expect(target(undefined, thread)).toEqual({ session: `codex-${thread}`, thread })
  expect(() => target('codex', undefined)).toThrow()
  expect(() => target('../other', thread)).toThrow()
  expect(() => checkOwner({ pid: 123, session: 'codex', threadId: 'other', hubs: [] }, 'codex', thread)).toThrow()
  expect(() => checkOwner({ pid: 123, session: 'codex', threadId: thread, hubs: [] }, 'codex', thread)).not.toThrow()
})

test('only authenticated messages from an accepted peer on its recorded hub become queue input', () => {
  const sender = Identity.generate(), receiver = Identity.generate()
  const env = sender.seal(receiver.id, { kind: 'msg', text: 'hello $(touch /tmp/not-executed)' })
  const peers = { [sender.id]: { state: 'accepted' as const, hub: 'example.com', since: 0 } }
  expect(queueMessage(receiver, env, 'example.com', peers)).toContain('hello $(touch /tmp/not-executed)')
  expect(queueMessage(receiver, env, 'wrong.example', peers)).toBeUndefined()
  expect(queueMessage(receiver, env, 'example.com', {})).toBeUndefined()
  expect(queueMessage(receiver, env, 'example.com', { [sender.id]: { ...peers[sender.id], state: 'forgotten' } })).toBeUndefined()
  expect(() => queueMessage(receiver, { ...env, sig: '00'.repeat(64) }, 'example.com', peers)).toThrow()
  expect(queueMessage(receiver, sender.seal(receiver.id, { kind: 'intro', text: 'accept me' }), 'example.com', peers)).toBeUndefined()
})

test('daemon lifecycle is idempotent and cannot stop another chat', async () => {
  const home = mkdtempSync(join(tmpdir(), 'crosstalk-channel-test-'))
  const thread = '11111111-1111-4111-8111-111111111111'
  const run = async (command: string, id = thread) => {
    const child = Bun.spawn([process.execPath, join(import.meta.dir, '../../codex/channel.ts'), command, 'probe', id], {
      env: { ...process.env, CROSSTALK_HOME: home, CROSSTALK_HUB: 'http://127.0.0.1:1' },
      stdout: 'pipe', stderr: 'pipe',
    })
    const [code, out, err] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
    return { code, out, err }
  }
  try {
    expect((await run('start')).code).toBe(0)
    const first = JSON.parse((await run('status')).out)
    expect(first.threadId).toBe(thread)
    expect((await run('start')).out).toContain('Already running')
    expect(JSON.parse((await run('status')).out).pid).toBe(first.pid)
    expect((await run('stop', '22222222-2222-4222-8222-222222222222')).code).toBe(1)
    expect(JSON.parse((await run('status')).out).pid).toBe(first.pid)
    expect((await run('stop')).out).toContain('Stopped')
    expect(JSON.parse((await run('status')).out).running).toBe(false)
  } finally {
    await run('stop')
    rmSync(home, { recursive: true, force: true })
  }
}, 20_000)
