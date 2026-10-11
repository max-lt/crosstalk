import type { Identity } from './crypto'
import { authMessage, type DeliveryStatus, type Envelope, type HubMessage } from './protocol'

export type HubStatus = 'connecting' | 'connected' | 'disconnected'

const SEND_TIMEOUT_MS = 10_000
const MAX_BACKOFF_MS = 30_000

export function normalizeHubUrl(input: string): string {
  const url = new URL(input.includes('://') ? input : `https://${input}`)
  if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new Error(`unsupported hub scheme: ${url.protocol}`)
  return url.origin
}

export function hubHost(url: string): string {
  return new URL(url).host
}

export class HubConnection {
  status: HubStatus = 'disconnected'
  private ws?: WebSocket
  private pending = new Map<string, (status: DeliveryStatus) => void>()
  private attempts = 0
  private stopped = false

  constructor(
    readonly url: string,
    private readonly identity: Identity,
    private readonly onDeliver: (env: Envelope, hub: string) => void,
    private readonly log: (line: string) => void,
  ) {}

  get host(): string {
    return hubHost(this.url)
  }

  start(): void {
    this.stopped = false
    this.connect()
  }

  stop(): void {
    this.stopped = true
    this.ws?.close()
  }

  send(env: Envelope): Promise<DeliveryStatus> {
    if (this.status !== 'connected' || !this.ws) return Promise.resolve('offline')
    return new Promise(resolve => {
      const timer = setTimeout(() => {
        this.pending.delete(env.id)
        resolve('timeout')
      }, SEND_TIMEOUT_MS)
      this.pending.set(env.id, status => {
        clearTimeout(timer)
        resolve(status)
      })
      this.ws!.send(JSON.stringify({ type: 'send', env }))
    })
  }

  private connect(): void {
    const url = new URL(this.url)
    url.protocol = url.protocol === 'http:' ? 'ws:' : 'wss:'
    url.pathname = `/ws/${this.identity.id}`
    this.status = 'connecting'
    const ws = new WebSocket(url)
    this.ws = ws
    ws.onmessage = event => this.onMessage(ws, JSON.parse(String(event.data)))
    ws.onclose = event => {
      this.status = 'disconnected'
      for (const resolve of this.pending.values()) resolve('offline')
      this.pending.clear()
      if (this.stopped) return
      const delay = Math.min(MAX_BACKOFF_MS, 1000 * 2 ** this.attempts++)
      this.log(`hub ${this.host} closed (${event.code} ${event.reason}), retry in ${delay}ms`)
      setTimeout(() => this.connect(), delay)
    }
    ws.onerror = () => {}
  }

  private onMessage(ws: WebSocket, msg: HubMessage): void {
    switch (msg.type) {
      case 'challenge':
        // Sign the host we dialed, not one the hub claims, so a hub can't replay our auth elsewhere.
        ws.send(JSON.stringify({ type: 'auth', sig: this.identity.sign(authMessage(this.host, msg.nonce)) }))
        return
      case 'ready':
        this.status = 'connected'
        this.attempts = 0
        this.log(`hub ${this.host} ready`)
        return
      case 'deliver':
        this.onDeliver(msg.env, this.host)
        return
      case 'status':
        this.pending.get(msg.ref)?.(msg.status)
        this.pending.delete(msg.ref)
        return
      case 'error':
        this.log(`hub ${this.host}: ${msg.message}`)
    }
  }
}
