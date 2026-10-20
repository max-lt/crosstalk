// Wire format shared with worker/src/protocol.ts and plugin/server/src/protocol.ts.
use serde::{Deserialize, Serialize};

pub const ID_PREFIX: &str = "crosstalk_";

pub fn is_id(s: &str) -> bool {
    let Some(rest) = s.strip_prefix(ID_PREFIX) else { return false };
    (43..=44).contains(&rest.len()) && rest.bytes().all(|b| b.is_ascii_alphanumeric() && !b"0OIl".contains(&b))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u8,
    pub id: String,
    pub from: String,
    pub to: String,
    pub ts: i64,
    pub nonce: String,
    pub ct: String,
    pub sig: String,
}

impl Envelope {
    pub fn signed_text(&self) -> String {
        format!("{}|{}|{}|{}|{}|{}|{}", self.v, self.id, self.from, self.to, self.ts, self.nonce, self.ct)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadKind {
    Intro,
    Accept,
    Msg,
    Forget,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payload {
    pub kind: PayloadKind,
    pub text: String,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ClientMessage {
    Auth { sig: String },
    Send { env: Envelope },
    Ping,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HubMessage {
    Challenge { nonce: String },
    Ready {},
    Deliver { env: Envelope },
    Status { r#ref: String, status: String },
    Error { message: String },
    Pong,
}

pub fn auth_message(host: &str, nonce: &str) -> String {
    format!("crosstalk-auth-v1|{host}|{nonce}")
}
