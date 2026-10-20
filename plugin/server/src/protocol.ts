// Wire format shared with worker/src/protocol.ts, plus the encrypted payload.

export const ID_PREFIX = 'crosstalk_'
export const ID_RE = /^crosstalk_[1-9A-HJ-NP-Za-km-z]{43,44}$/

export interface Envelope {
  v: 1
  id: string
  from: string
  to: string
  ts: number
  nonce: string
  ct: string
  sig: string
}

export type DeliveryStatus = 'delivered' | 'offline' | 'rejected' | 'throttled' | 'timeout'

export type ClientMessage = { type: 'auth'; sig: string } | { type: 'send'; env: Envelope } | { type: 'ping' }

export type HubMessage =
  | { type: 'challenge'; nonce: string }
  | { type: 'ready'; id: string }
  | { type: 'deliver'; env: Envelope }
  | { type: 'status'; ref: string; status: Exclude<DeliveryStatus, 'timeout'> }
  | { type: 'error'; message: string }
  | { type: 'pong' }

export type PayloadKind = 'intro' | 'accept' | 'msg' | 'forget'

export interface Payload {
  kind: PayloadKind
  text: string
}

export function authMessage(host: string, nonce: string): string {
  return `crosstalk-auth-v1|${host}|${nonce}`
}
