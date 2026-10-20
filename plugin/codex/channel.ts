#!/usr/bin/env bun
import { spawn } from 'node:child_process'
import { Database } from 'bun:sqlite'
import { closeSync, mkdirSync, openSync } from 'node:fs'
import { join } from 'node:path'
import { controlRequest, controlSocketPath } from '../server/src/control'
import { defaultHome } from '../server/src/store'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
export type Status = { pid: number; session: string; threadId: string; hubs: unknown }

export function target(session: string | undefined, thread: string | undefined) {
  if (!thread || !UUID.test(thread)) throw new Error('Set CODEX_THREAD_ID or pass the exact chat UUID as the third argument')
  session ??= `codex-${thread}`
  if (!/^[a-zA-Z0-9_-]+$/.test(session)) throw new Error('Invalid session name')
  return { session, thread }
}

export function checkOwner(status: Status, session: string, thread: string) {
  if (status.session !== session || status.threadId !== thread || !Number.isInteger(status.pid)) {
    throw new Error('This session belongs to a different chat or an unmanaged process; leave it running and use a separate session')
  }
}

async function main() {
  const [command, sessionArg, threadArg] = process.argv.slice(2)
  if (!command || ['help', '--help', '-h'].includes(command)) {
    console.log('Usage: crosstalk-codex <start|status|stop> [session] [thread-uuid]\nDefaults: CROSSTALK_SESSION or codex-<thread>; CODEX_THREAD_ID.\nOne daemon per session. No config edits or login service are installed.')
    return
  }
  if (!['start', 'status', 'stop'].includes(command)) throw new Error('Expected start, status, or stop')
  const { session, thread } = target(sessionArg ?? process.env.CROSSTALK_SESSION, threadArg ?? process.env.CODEX_THREAD_ID)
  const dir = join(defaultHome(), 'sessions', session)
  // Serialize launcher mutations and refuse the old listener's lifetime lock.
  // The daemon socket remains the source of truth after this launcher exits.
  let lock: Database | undefined
  if (command !== 'status') {
    mkdirSync(dir, { recursive: true, mode: 0o700 })
    lock = new Database(join(dir, 'codex-listener-lock.sqlite'))
    try { lock.exec('BEGIN IMMEDIATE') }
    catch { lock.close(); throw new Error('Another launcher or the old codex-listen process owns this session. Retry after it stops.') }
  }
  try {
  const socket = controlSocketPath(dir)
  const probe = async (stopping = false): Promise<Status | null> => {
    let reply: string
    try { reply = await controlRequest(socket, { cmd: 'status', args: [] }, 1000) }
    catch (err) {
      if (['ENOENT', 'ECONNREFUSED'].includes((err as NodeJS.ErrnoException).code ?? '')) return null
      throw err
    }
    if (!reply && stopping) return null
    if (reply.startsWith('error:')) throw new Error(reply)
    const status = JSON.parse(reply) as Status
    checkOwner(status, session, thread)
    return status
  }
  const current = await probe()
  if (command === 'status') { console.log(JSON.stringify(current ?? { running: false, session, threadId: thread })); return }
  if (command === 'stop') {
    if (!current) { console.log('Already stopped'); return }
    const reply = await controlRequest(socket, { cmd: 'stop', args: [] }, 3000)
    if (reply.startsWith('error:')) throw new Error(reply)
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100)
      if (!await probe(true)) { console.log('Stopped'); return }
    }
    throw new Error('Stop requested, but the daemon is still running')
  }
  if (current) { console.log(`Already running: ${JSON.stringify(current)}`); return }
  const check = Bun.spawn(['codex', 'queue', '--help'], { stdout: 'ignore', stderr: 'ignore' })
  if (await check.exited !== 0) throw new Error('This Codex build must support codex queue')
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  const logfile = join(dir, 'codex-channel.log')
  const fd = openSync(logfile, 'a', 0o600)
  const child = spawn(process.execPath, [join(import.meta.dir, '../server/src/main.ts'), '--daemon', '--codex-thread', thread], {
    detached: true,
    stdio: ['ignore', fd, fd],
    env: { ...process.env, CROSSTALK_SESSION: session },
  })
  closeSync(fd)
  let failure: Error | undefined
  child.once('error', err => { failure = err })
  child.unref()
  for (let i = 0; i < 100; i++) {
    await Bun.sleep(100)
    if (failure) throw failure
    if (child.exitCode !== null) throw new Error(`Daemon exited. Read ${logfile}`)
    const status = await probe()
    if (status) { console.log(`Started: ${JSON.stringify(status)}\nLog: ${logfile}`); return }
  }
  throw new Error(`Startup not confirmed. Read ${logfile} and run status before retrying`)
  } finally { lock?.close() }
}

if (import.meta.main) main().catch(err => { console.error(`crosstalk-codex: ${err.message}`); process.exit(1) })
