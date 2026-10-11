import { DurableObject } from 'cloudflare:workers'
import { ed25519 } from '@noble/curves/ed25519'
import { bytesToHex, hexToBytes, randomBytes, utf8ToBytes } from '@noble/hashes/utils'
import { base58 } from '@scure/base'
import { authMessage, ID_PREFIX, type ClientMessage, type Envelope, type HubMessage } from './protocol'

export interface Env {
  PEER: DurableObjectNamespace<Peer>
}

interface Attachment {
  authed: boolean
  nonce: string
  host: string
}

const SENDS_PER_MINUTE = 60
const MAX_ENVELOPE_BYTES = 64 * 1024

// One instance per peer id. Holds only live sockets; nothing survives the last disconnect.
export class Peer extends DurableObject<Env> {
  private sentAt: number[] = []

  async fetch(request: Request): Promise<Response> {
    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)
    const nonce = bytesToHex(randomBytes(32))
    const host = request.headers.get('host') ?? ''
    this.ctx.acceptWebSocket(server)
    server.serializeAttachment({ authed: false, nonce, host } satisfies Attachment)
    server.send(JSON.stringify({ type: 'challenge', nonce } satisfies HubMessage))
    return new Response(null, { status: 101, webSocket: client })
  }

  async webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer): Promise<void> {
    if (typeof raw !== 'string' || raw.length > MAX_ENVELOPE_BYTES) {
      ws.close(4000, 'bad frame')
      return
    }
    let msg: ClientMessage
    try {
      msg = JSON.parse(raw)
    } catch {
      ws.close(4000, 'bad json')
      return
    }
    const att = ws.deserializeAttachment() as Attachment
    if (!att.authed) {
      this.authenticate(ws, att, msg)
      return
    }
    if (msg.type !== 'send') {
      this.reply(ws, { type: 'error', message: `unexpected ${msg.type}` })
      return
    }
    await this.relay(ws, msg.env)
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
    ws.close()
  }

  // Exposed over DO RPC to the sender's Peer instance.
  async deliver(env: Envelope): Promise<boolean> {
    const live = this.ctx.getWebSockets().filter(w => (w.deserializeAttachment() as Attachment).authed)
    if (live.length === 0) return false
    const frame = JSON.stringify({ type: 'deliver', env } satisfies HubMessage)
    for (const w of live) w.send(frame)
    return true
  }

  private authenticate(ws: WebSocket, att: Attachment, msg: ClientMessage): void {
    const id = this.ctx.id.name!
    if (msg.type !== 'auth' || !verify(id, att.host, att.nonce, msg.sig)) {
      ws.close(4003, 'bad signature')
      return
    }
    ws.serializeAttachment({ ...att, authed: true } satisfies Attachment)
    this.reply(ws, { type: 'ready', id })
  }

  private async relay(ws: WebSocket, env: Envelope): Promise<void> {
    const id = this.ctx.id.name!
    if (env?.v !== 1 || env.from !== id || typeof env.to !== 'string' || !env.to.startsWith(ID_PREFIX)) {
      this.reply(ws, { type: 'status', ref: env?.id ?? '', status: 'rejected' })
      return
    }
    if (this.throttled()) {
      this.reply(ws, { type: 'status', ref: env.id, status: 'throttled' })
      return
    }
    const target = this.env.PEER.get(this.env.PEER.idFromName(env.to))
    const delivered = await target.deliver(env)
    this.reply(ws, { type: 'status', ref: env.id, status: delivered ? 'delivered' : 'offline' })
  }

  private throttled(): boolean {
    const now = Date.now()
    this.sentAt = this.sentAt.filter(t => now - t < 60_000)
    if (this.sentAt.length >= SENDS_PER_MINUTE) return true
    this.sentAt.push(now)
    return false
  }

  private reply(ws: WebSocket, msg: HubMessage): void {
    ws.send(JSON.stringify(msg))
  }
}

function verify(id: string, host: string, nonce: string, sigHex: string): boolean {
  try {
    const pub = base58.decode(id.slice(ID_PREFIX.length))
    return ed25519.verify(hexToBytes(sigHex), utf8ToBytes(authMessage(host, nonce)), pub)
  } catch {
    return false
  }
}
