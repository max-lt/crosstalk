import { ed25519, edwardsToMontgomeryPriv, edwardsToMontgomeryPub, x25519 } from '@noble/curves/ed25519'
import { xchacha20poly1305 } from '@noble/ciphers/chacha'
import { hkdf } from '@noble/hashes/hkdf'
import { sha256 } from '@noble/hashes/sha2'
import { bytesToHex, concatBytes, hexToBytes, randomBytes, utf8ToBytes } from '@noble/hashes/utils'
import { base58, base64 } from '@scure/base'
import { ID_PREFIX, ID_RE, type Envelope, type Payload, type PayloadKind } from './protocol'

const KINDS: PayloadKind[] = ['intro', 'accept', 'msg', 'forget']
const MAX_CLOCK_SKEW_MS = 5 * 60_000

export function idFromPublicKey(pub: Uint8Array): string {
  return ID_PREFIX + base58.encode(pub)
}

export function publicKeyFromId(id: string): Uint8Array {
  if (!ID_RE.test(id)) throw new Error(`not a crosstalk id: ${id}`)
  return base58.decode(id.slice(ID_PREFIX.length))
}

export class Identity {
  readonly publicKey: Uint8Array
  readonly id: string

  constructor(readonly seed: Uint8Array) {
    this.publicKey = ed25519.getPublicKey(seed)
    this.id = idFromPublicKey(this.publicKey)
  }

  static generate(): Identity {
    return new Identity(ed25519.utils.randomPrivateKey())
  }

  sign(message: string): string {
    return bytesToHex(ed25519.sign(utf8ToBytes(message), this.seed))
  }

  seal(to: string, payload: Payload): Envelope {
    const toPub = publicKeyFromId(to)
    const nonce = randomBytes(24)
    const key = this.sharedKey(toPub, this.publicKey, toPub)
    const ct = xchacha20poly1305(key, nonce).encrypt(utf8ToBytes(JSON.stringify(payload)))
    const env: Omit<Envelope, 'sig'> = {
      v: 1,
      id: bytesToHex(randomBytes(16)),
      from: this.id,
      to,
      ts: Date.now(),
      nonce: bytesToHex(nonce),
      ct: base64.encode(ct),
    }
    return { ...env, sig: this.sign(signedText(env)) }
  }

  // Throws on any envelope not addressed to us, forged, stale or malformed.
  open(env: Envelope): Payload {
    if (env.v !== 1 || env.to !== this.id) throw new Error('envelope not for us')
    if (Math.abs(Date.now() - env.ts) > MAX_CLOCK_SKEW_MS) throw new Error('envelope too old')
    const fromPub = publicKeyFromId(env.from)
    if (!ed25519.verify(hexToBytes(env.sig), utf8ToBytes(signedText(env)), fromPub)) throw new Error('bad signature')
    const key = this.sharedKey(fromPub, fromPub, this.publicKey)
    const plain = xchacha20poly1305(key, hexToBytes(env.nonce)).decrypt(base64.decode(env.ct))
    const payload = JSON.parse(new TextDecoder().decode(plain))
    if (!KINDS.includes(payload?.kind) || typeof payload.text !== 'string') throw new Error('bad payload')
    return { kind: payload.kind, text: payload.text }
  }

  // Salt is sender||receiver so both ends derive the same key for one direction.
  private sharedKey(theirPub: Uint8Array, fromPub: Uint8Array, toPub: Uint8Array): Uint8Array {
    const shared = x25519.getSharedSecret(edwardsToMontgomeryPriv(this.seed), edwardsToMontgomeryPub(theirPub))
    return hkdf(sha256, shared, concatBytes(fromPub, toPub), utf8ToBytes('crosstalk-v1'), 32)
  }
}

function signedText(env: Omit<Envelope, 'sig'>): string {
  return [env.v, env.id, env.from, env.to, env.ts, env.nonce, env.ct].join('|')
}
