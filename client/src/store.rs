use crate::crypto::Identity;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerState {
    PendingOut,
    PendingIn,
    Accepted,
    Forgotten,
}

impl PeerState {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerState::PendingOut => "pending-out",
            PeerState::PendingIn => "pending-in",
            PeerState::Accepted => "accepted",
            PeerState::Forgotten => "forgotten",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerRecord {
    pub hub: String,
    pub state: PeerState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub since: i64,
}

pub fn default_home() -> PathBuf {
    if let Ok(home) = std::env::var("CROSSTALK_HOME") {
        return PathBuf::from(home);
    }
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into())).join(".crosstalk")
}

pub fn session_dir(home: &Path, session: &str) -> PathBuf {
    home.join("sessions").join(session)
}

fn read_pid_file(home: &Path, pid: &str) -> Option<String> {
    let sid = fs::read_to_string(home.join("pid").join(pid)).ok()?;
    let sid = sid.trim().to_string();
    (!sid.is_empty()).then_some(sid)
}

// Resolution order: the pid file written by the SessionStart hook (right on
// --continue), then the env var Claude Code sets, then a fixed name.
pub async fn resolve_session_id(home: &Path) -> String {
    if let Ok(sid) = std::env::var("CROSSTALK_SESSION") {
        return sid;
    }
    let ppid = std::os::unix::process::parent_id().to_string();
    for _ in 0..20 {
        if let Some(sid) = read_pid_file(home, &ppid) {
            return sid;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::env::var("CLAUDE_CODE_SESSION_ID").unwrap_or_else(|_| "default".into())
}

// Written by the SessionStart hook: maps the Claude Code process to its session id, so the
// channel server finds the right identity even when --continue hands it a fresh
// CLAUDE_CODE_SESSION_ID. Files older than a day are stale processes.
pub fn write_pid_file(home: &Path, pid: &str, session: &str) -> std::io::Result<()> {
    let dir = home.join("pid");
    fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
    fs::write(dir.join(pid), session)?;
    let day = Duration::from_secs(24 * 3600);
    for entry in fs::read_dir(&dir)?.flatten() {
        let stale = entry.metadata().and_then(|m| m.modified()).map(|t| t.elapsed().unwrap_or_default() > day).unwrap_or(false);
        if stale {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

// Session for a CLI run from a shell Claude Code spawned (CLAUDE_PID is exported there).
pub fn session_from_claude(home: &Path) -> Option<String> {
    if let Ok(pid) = std::env::var("CLAUDE_PID") {
        if let Some(sid) = read_pid_file(home, &pid) {
            return Some(sid);
        }
    }
    std::env::var("CLAUDE_CODE_SESSION_ID").ok()
}

pub struct Store {
    pub home: PathBuf,
    pub dir: PathBuf,
    pub peers: BTreeMap<String, PeerRecord>,
}

fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    file.write_all(contents.as_bytes())
}

impl Store {
    pub fn open(home: PathBuf, session: &str) -> std::io::Result<Store> {
        let dir = session_dir(&home, session);
        fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
        let peers = match fs::read_to_string(dir.join("peers.json")) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => BTreeMap::new(),
        };
        Ok(Store { home, dir, peers })
    }

    pub fn load_or_create_identity(&self) -> std::io::Result<Identity> {
        let file = self.dir.join("key");
        if let Ok(text) = fs::read_to_string(&file) {
            let bytes = hex::decode(text.trim()).map_err(|e| std::io::Error::other(e.to_string()))?;
            let seed: [u8; 32] = bytes.try_into().map_err(|_| std::io::Error::other("bad key length"))?;
            return Ok(Identity::from_seed(seed));
        }
        let identity = Identity::generate();
        write_private(&file, &hex::encode(identity.seed()))?;
        Ok(identity)
    }

    pub fn save_peers(&self) {
        let text = serde_json::to_string_pretty(&self.peers).expect("peers serialize");
        if let Err(err) = write_private(&self.dir.join("peers.json"), &text) {
            eprintln!("[crosstalk] cannot save peers: {err}");
        }
    }

    pub fn hubs(&self) -> Vec<String> {
        fs::read_to_string(self.home.join("hubs.json")).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default()
    }

    pub fn add_hub(&self, url: &str) {
        let mut hubs = self.hubs();
        if hubs.iter().any(|h| h == url) {
            return;
        }
        hubs.push(url.to_string());
        let text = serde_json::to_string_pretty(&hubs).expect("hubs serialize");
        if let Err(err) = write_private(&self.home.join("hubs.json"), &text) {
            eprintln!("[crosstalk] cannot save hubs: {err}");
        }
    }

    pub fn wipe(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
        let _ = fs::DirBuilder::new().recursive(true).mode(0o700).create(&self.dir);
        self.peers.clear();
    }
}
