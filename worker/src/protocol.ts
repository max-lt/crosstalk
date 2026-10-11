// Wire format between a channel process and a hub. The hub never reads `ct`.

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

export type DeliveryStatus = 'delivered' | 'offline' | 'rejected' | 'throttled'

export type ClientMessage = { type: 'auth'; sig: string } | { type: 'send'; env: Envelope }

export type HubMessage =
  | { type: 'challenge'; nonce: string }
  | { type: 'ready'; id: string }
  | { type: 'deliver'; env: Envelope }
  | { type: 'status'; ref: string; status: DeliveryStatus }
  | { type: 'error'; message: string }

export function authMessage(host: string, nonce: string): string {
  return `crosstalk-auth-v1|${host}|${nonce}`
}
