#!/usr/bin/env bun
// crosstalk CLI: drives the channel process of the Claude Code session it runs in.
// From the Claude Code prompt: `! crosstalk whoami`. No model turn.
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { controlRequest, controlSocketPath } from './control'
import { defaultHome } from './store'

const USAGE = `usage: crosstalk <command> [args]

  whoami                              this session's id and hubs
  peers                               peers and their pairing state
  connect [<hub>:]<id> [description]  send a pairing request
  accept <id> [description]           accept a pending request
  send <id> <text...>                 message an accepted peer
  inbox [wait_seconds]                read pending events
  forget [<id>]                       drop a peer, or the whole identity
  add-hub <url>                       add a relay hub
  status                              pid, session, thread and hub state (JSON)
  stop                                stop a daemon

The session comes from CROSSTALK_SESSION, else from the Claude Code
process this shell runs in (CLAUDE_PID), else the only running session. Sockets live in $CROSSTALK_HOME/sessions/<id>/.`

// Claude Code exports CLAUDE_PID to the shells it spawns; the SessionStart hook
// wrote that pid's session id. CLAUDE_CODE_SESSION_ID can be stale after --continue.
function sessionFromClaude(home: string): string | undefined {
  const pid = process.env.CLAUDE_PID
  if (pid) {
    const file = join(home, 'pid', pid)
    if (existsSync(file)) return readFileSync(file, 'utf8').trim()
  }
  return process.env.CLAUDE_CODE_SESSION_ID
}

async function liveSockets(home: string): Promise<string[]> {
  const root = join(home, 'sessions')
  if (!existsSync(root)) return []
  const found: string[] = []
  for (const sid of readdirSync(root)) {
    const path = controlSocketPath(join(root, sid))
    if (!existsSync(path)) continue
    try {
      await controlRequest(path, { cmd: 'ping', args: [] }, 1000)
      found.push(path)
    } catch {}
  }
  return found
}

async function socketPath(home: string): Promise<string> {
  const sid = process.env.CROSSTALK_SESSION ?? sessionFromClaude(home)
  if (sid) return controlSocketPath(join(home, 'sessions', sid))
  const live = await liveSockets(home)
  if (live.length === 1) return live[0]
  if (live.length === 0) throw new Error('no running crosstalk session found')
  throw new Error(`${live.length} sessions running; set CROSSTALK_SESSION or run from inside Claude Code`)
}

async function main(): Promise<void> {
  const [cmd, ...args] = process.argv.slice(2)
  if (!cmd || cmd === 'help' || cmd === '--help' || cmd === '-h') {
    console.log(USAGE)
    return
  }
  const path = await socketPath(defaultHome())
  if (!existsSync(path)) throw new Error(`no channel process for this session (${path})`)
  console.log(await controlRequest(path, { cmd, args }))
}

main().catch(err => {
  console.error(`crosstalk: ${(err as Error).message}`)
  process.exit(1)
})
