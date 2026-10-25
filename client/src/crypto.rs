use crate::protocol::{is_id, Envelope, Payload, ID_PREFIX};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_CLOCK_SKEW_MS: i64 = 5 * 60_000;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

pub fn id_from_public_key(public: &[u8; 32]) -> String {
    format!("{ID_PREFIX}{}", bs58::encode(public).into_string())
}

pub fn public_key_from_id(id: &str) -> Result<VerifyingKey, String> {
    if !is_id(id) {
        return Err(format!("not a crosstalk id: {id}"));
    }
    let bytes = bs58::decode(&id[ID_PREFIX.len()..]).into_vec().map_err(|e| e.to_string())?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| "bad key length".to_string())?;
    VerifyingKey::from_bytes(&arr).map_err(|e| e.to_string())
}

pub struct Identity {
    signing: SigningKey,
    pub public: [u8; 32],
    pub id: String,
}

impl Identity {
    pub fn from_seed(seed: [u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(&seed);
        let public = signing.verifying_key().to_bytes();
        let id = id_from_public_key(&public);
        Identity { signing, public, id }
    }

    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        Identity::from_seed(seed)
    }

    pub fn seed(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    pub fn sign(&self, message: &str) -> String {
        hex::encode(self.signing.sign(message.as_bytes()).to_bytes())
    }

    pub fn seal(&self, to: &str, payload: &Payload) -> Result<Envelope, String> {
        let to_key = public_key_from_id(to)?;
        let mut nonce = [0u8; 24];
        rand::rng().fill_bytes(&mut nonce);
        let mut id = [0u8; 16];
        rand::rng().fill_bytes(&mut id);
        let key = self.shared_key(&to_key, &self.public, &to_key.to_bytes());
        let cipher = XChaCha20Poly1305::new((&key).into());
        let plain = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
        let ct =
            cipher.encrypt(&XNonce::from(nonce), plain.as_ref()).map_err(|_| "encrypt failed".to_string())?;
        let mut env = Envelope {
            v: 1,
            id: hex::encode(id),
            from: self.id.clone(),
            to: to.to_string(),
            ts: now_ms(),
            nonce: hex::encode(nonce),
            ct: B64.encode(ct),
            sig: String::new(),
        };
        env.sig = self.sign(&env.signed_text());
        Ok(env)
    }

    // Errors on any envelope not addressed to us, forged, stale or malformed.
    pub fn open(&self, env: &Envelope) -> Result<Payload, String> {
        if env.v != 1 || env.to != self.id {
            return Err("envelope not for us".into());
        }
        if (now_ms() - env.ts).abs() > MAX_CLOCK_SKEW_MS {
            return Err("envelope too old".into());
        }
        let from_key = public_key_from_id(&env.from)?;
        let sig_bytes = hex::decode(&env.sig).map_err(|_| "bad signature")?;
        let sig = Signature::from_slice(&sig_bytes).map_err(|_| "bad signature")?;
        from_key.verify(env.signed_text().as_bytes(), &sig).map_err(|_| "bad signature")?;
        let key = self.shared_key(&from_key, &from_key.to_bytes(), &self.public);
        let nonce: [u8; 24] =
            hex::decode(&env.nonce).map_err(|_| "bad nonce")?.try_into().map_err(|_| "bad nonce")?;
        let ct = B64.decode(&env.ct).map_err(|_| "bad ciphertext")?;
        let cipher = XChaCha20Poly1305::new((&key).into());
        let plain = cipher.decrypt(&XNonce::from(nonce), ct.as_ref()).map_err(|_| "decrypt failed")?;
        serde_json::from_slice::<Payload>(&plain).map_err(|_| "bad payload".into())
    }

    // Salt is sender||receiver so both ends derive the same key for one direction.
    fn shared_key(&self, their: &VerifyingKey, from_pub: &[u8; 32], to_pub: &[u8; 32]) -> [u8; 32] {
        let secret = x25519_dalek::StaticSecret::from(self.signing.to_scalar_bytes());
        let their_mont = x25519_dalek::PublicKey::from(their.to_montgomery().to_bytes());
        let shared = secret.diffie_hellman(&their_mont);
        let mut salt = Vec::with_capacity(64);
        salt.extend_from_slice(from_pub);
        salt.extend_from_slice(to_pub);
        let hk = Hkdf::<Sha256>::new(Some(&salt), shared.as_bytes());
        let mut out = [0u8; 32];
        hk.expand(b"crosstalk-v1", &mut out).expect("32 bytes is a valid hkdf length");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PayloadKind;

    #[test]
    fn seal_and_open_round_trip() {
        let a = Identity::generate();
        let b = Identity::generate();
        let env = a.seal(&b.id, &Payload { kind: PayloadKind::Msg, text: "hi".into() }).unwrap();
        let payload = b.open(&env).unwrap();
        assert_eq!(payload.kind, PayloadKind::Msg);
        assert_eq!(payload.text, "hi");
        assert!(a.open(&env).is_err());
        let mut forged = env.clone();
        let mut ct = B64.decode(&env.ct).unwrap();
        ct[0] ^= 1;
        forged.ct = B64.encode(ct);
        assert!(b.open(&forged).is_err());
        let mut stale = env.clone();
        stale.ts -= MAX_CLOCK_SKEW_MS + 1;
        stale.sig = a.sign(&stale.signed_text());
        assert!(b.open(&stale).is_err());
    }
}
