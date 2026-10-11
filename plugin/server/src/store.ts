import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { bytesToHex, hexToBytes } from '@noble/hashes/utils'
import { Identity } from './crypto'

export type PeerState = 'pending-out' | 'pending-in' | 'accepted' | 'forgotten'

export interface PeerRecord {
  hub: string
  state: PeerState
  description?: string
  since: number
}

export function defaultHome(): string {
  return process.env.CROSSTALK_HOME ?? join(homedir(), '.crosstalk')
}

// Resolution order: the pid file written by the SessionStart hook (right on
// --continue), then the env var Claude Code sets, then a fixed name.
export async function resolveSessionId(home: string): Promise<string> {
  if (process.env.CROSSTALK_SESSION) return process.env.CROSSTALK_SESSION
  const pidFile = join(home, 'pid', String(process.ppid))
  const deadline = Date.now() + 2000
  while (Date.now() < deadline) {
    if (existsSync(pidFile)) {
      const sid = readFileSync(pidFile, 'utf8').trim()
      if (sid) return sid
    }
    await Bun.sleep(100)
  }
  return process.env.CLAUDE_CODE_SESSION_ID ?? 'default'
}

export class Store {
  readonly dir: string
  peers: Record<string, PeerRecord>

  constructor(readonly home: string, readonly sessionId: string) {
    this.dir = join(home, 'sessions', sessionId)
    mkdirSync(this.dir, { recursive: true, mode: 0o700 })
    this.peers = readJson(join(this.dir, 'peers.json'), {})
  }

  loadOrCreateIdentity(): Identity {
    const file = join(this.dir, 'key')
    if (existsSync(file)) return new Identity(hexToBytes(readFileSync(file, 'utf8').trim()))
    const identity = Identity.generate()
    writeFileSync(file, bytesToHex(identity.seed), { mode: 0o600 })
    return identity
  }

  savePeers(): void {
    writeFileSync(join(this.dir, 'peers.json'), JSON.stringify(this.peers, null, 2), { mode: 0o600 })
  }

  hubs(): string[] {
    return readJson<string[]>(join(this.home, 'hubs.json'), [])
  }

  addHub(url: string): void {
    const hubs = this.hubs()
    if (hubs.includes(url)) return
    hubs.push(url)
    writeFileSync(join(this.home, 'hubs.json'), JSON.stringify(hubs, null, 2), { mode: 0o600 })
  }

  wipe(): void {
    rmSync(this.dir, { recursive: true, force: true })
    mkdirSync(this.dir, { recursive: true, mode: 0o700 })
    this.peers = {}
  }
}

function readJson<T>(file: string, fallback: T): T {
  if (!existsSync(file)) return fallback
  return JSON.parse(readFileSync(file, 'utf8'))
}
