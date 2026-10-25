use crate::crypto::{now_ms, Identity};
use crate::hub::{hub_host, normalize_hub_url, HubConnection};
use crate::protocol::{is_id, Envelope, Payload, PayloadKind, ID_PREFIX};
use crate::store::{PeerRecord, PeerState, Store};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

pub const DEFAULT_HUB: &str = "https://crosstalk.lechat.dev";
const DESCRIPTION_MAX: usize = 140;
const MESSAGE_MAX: usize = 16_000;
const SEEN_MAX: usize = 2000;
const INBOX_MAX: usize = 100;

pub type Meta = BTreeMap<String, String>;

/// Where inbound events go besides the inbox: a channel notification, a Codex queue, or nowhere.
pub type Sink = Box<dyn Fn(String, Meta) + Send + Sync>;

struct Event {
    content: String,
    meta: Meta,
}

pub struct App {
    pub store: Store,
    pub identity: Arc<Identity>,
    hubs: BTreeMap<String, HubConnection>,
    persistent: HashSet<String>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    inbox: VecDeque<Event>,
    inbox_notify: Arc<Notify>,
    self_description: String,
    delivered: mpsc::UnboundedSender<(Envelope, String)>,
    sink: Sink,
}

fn cap(text: &str, max: usize) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if line.chars().count() > max {
        format!("{}...", line.chars().take(max - 3).collect::<String>())
    } else {
        line
    }
}

fn attr(text: &str) -> String {
    text.chars().filter(|c| !matches!(c, '"' | '<' | '>')).collect()
}

fn parse_target(target: &str) -> Result<(Option<String>, String), String> {
    let at = target.find(ID_PREFIX).ok_or_else(|| format!("not a crosstalk id: {target}"))?;
    let id = target[at..].trim().to_string();
    if !is_id(&id) {
        return Err(format!("not a crosstalk id: {id}"));
    }
    let host = target[..at].trim().trim_end_matches(':').trim().to_string();
    Ok(((!host.is_empty()).then_some(host), id))
}

impl App {
    pub fn new(store: Store, identity: Identity, sink: Sink) -> (App, mpsc::UnboundedReceiver<(Envelope, String)>) {
        let (delivered, rx) = mpsc::unbounded_channel();
        let app = App {
            store,
            identity: Arc::new(identity),
            hubs: BTreeMap::new(),
            persistent: HashSet::new(),
            seen: HashSet::new(),
            seen_order: VecDeque::new(),
            inbox: VecDeque::new(),
            inbox_notify: Arc::new(Notify::new()),
            self_description: "a crosstalk session".into(),
            delivered,
            sink,
        };
        (app, rx)
    }

    /// Drains the inbox, after waiting up to `wait` for a first event when it is empty.
    pub async fn inbox(app: &tokio::sync::Mutex<App>, wait: std::time::Duration) -> Option<String> {
        let notify = app.lock().await.inbox_notify.clone();
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if let Some(text) = app.lock().await.drain_inbox() {
            return Some(text);
        }
        if wait.is_zero() {
            return None;
        }
        let _ = tokio::time::timeout(wait, notified).await;
        app.lock().await.drain_inbox()
    }

    fn connect_hub(&mut self, url: &str) -> &HubConnection {
        let host = hub_host(url);
        if !self.hubs.contains_key(&host) {
            let hub = HubConnection::start(url.to_string(), self.identity.clone(), self.delivered.clone());
            self.hubs.insert(host.clone(), hub);
        }
        &self.hubs[&host]
    }

    fn hub_by_host(&mut self, host: &str) -> &HubConnection {
        if !self.hubs.contains_key(host) {
            eprintln!("[crosstalk] joining hub {host} for this session");
            self.connect_hub(&format!("https://{host}"));
        }
        &self.hubs[host]
    }

    fn hub_for(&mut self, host: Option<&str>) -> Result<&HubConnection, String> {
        match host {
            Some(h) => Ok(self.hub_by_host(h)),
            None => self.hubs.values().next().ok_or_else(|| "no hub connection".to_string()),
        }
    }

    pub fn start(&mut self) {
        let mut urls: Vec<String> = std::env::var("CROSSTALK_HUB").ok().into_iter().collect();
        urls.extend(self.store.hubs());
        if urls.is_empty() {
            urls.push(DEFAULT_HUB.into());
        }
        for url in urls {
            self.persistent.insert(hub_host(&url));
            self.connect_hub(&url);
        }
    }

    pub fn stop(&mut self) {
        for hub in self.hubs.values() {
            hub.stop();
        }
    }

    pub fn whoami(&self) -> String {
        let mut lines = vec![self.identity.id.clone(), String::new()];
        for hub in self.hubs.values() {
            let tag = if self.persistent.contains(&hub.host) { "" } else { " (this session only)" };
            lines.push(format!("@ {}  {}{}", hub.host, hub.status(), tag));
        }
        lines.join("\n")
    }

    pub fn hub_status(&self) -> BTreeMap<String, String> {
        self.hubs.values().map(|h| (h.host.clone(), h.status().to_string())).collect()
    }

    pub fn add_hub(&mut self, input: &str) -> Result<String, String> {
        let url = normalize_hub_url(input)?;
        self.store.add_hub(&url);
        self.persistent.insert(hub_host(&url));
        self.connect_hub(&url);
        Ok(format!("hub {} added", hub_host(&url)))
    }

    fn sealed(&self, to: &str, kind: PayloadKind, text: &str) -> Result<Envelope, String> {
        self.identity.seal(to, &Payload { kind, text: text.to_string() })
    }

    /// Prepares an envelope and the hub to send it on; the caller awaits the send without holding the app lock.
    pub fn connect(&mut self, target: &str, description: &str) -> Result<Outgoing, String> {
        let (host, id) = parse_target(target)?;
        if id == self.identity.id {
            return Err("that is this session".into());
        }
        let hub_host = self.hub_for(host.as_deref())?.host.clone();
        if self.store.peers.get(&id).map(|p| p.state) == Some(PeerState::Accepted) {
            return Ok(Outgoing::Done(format!("{id} is already a peer")));
        }
        self.store.peers.insert(id.clone(), PeerRecord { hub: hub_host.clone(), state: PeerState::PendingOut, description: None, since: now_ms() });
        self.store.save_peers();
        self.self_description = cap(description, DESCRIPTION_MAX);
        let env = self.sealed(&id, PayloadKind::Intro, &self.self_description)?;
        Ok(Outgoing::Send { hub: hub_host.clone(), env, done: Box::new(move |status| format!("intro {status} via {hub_host}. The other side must accept before messages flow.")) })
    }

    pub fn accept(&mut self, id: &str, description: &str) -> Result<Outgoing, String> {
        let peer = match self.store.peers.get(id) {
            // An accepted peer may be re-accepted: that resends the accept if the first one was lost.
            Some(p) if matches!(p.state, PeerState::PendingIn | PeerState::Accepted) => p.clone(),
            _ => return Err(format!("{id} has no pending intro")),
        };
        self.self_description = cap(description, DESCRIPTION_MAX);
        let env = self.sealed(id, PayloadKind::Accept, &self.self_description)?;
        if let Some(p) = self.store.peers.get_mut(id) {
            p.state = PeerState::Accepted;
        }
        self.store.save_peers();
        let line = format!("accepted {id} ({})", peer.description.as_deref().unwrap_or("no description"));
        Ok(Outgoing::Send { hub: peer.hub, env, done: Box::new(move |status| format!("{line}, reply {status}")) })
    }

    pub fn send(&mut self, to: &str, text: &str) -> Result<Outgoing, String> {
        let peer = match self.store.peers.get(to) {
            Some(p) if p.state == PeerState::Accepted => p.clone(),
            _ => return Err(format!("{to} is not an accepted peer. Run crosstalk peers.")),
        };
        if text.chars().count() > MESSAGE_MAX {
            return Err(format!("message over {MESSAGE_MAX} characters"));
        }
        let env = self.sealed(to, PayloadKind::Msg, text)?;
        Ok(Outgoing::Send { hub: peer.hub, env, done: Box::new(|status| status) })
    }

    pub fn peers(&self) -> String {
        let entries: Vec<String> = self
            .store
            .peers
            .iter()
            .filter(|(_, p)| p.state != PeerState::Forgotten)
            .map(|(id, p)| {
                let desc = p.description.as_ref().map(|d| format!("  {d}")).unwrap_or_default();
                format!("{:<11} {id} @ {}{desc}", p.state.as_str(), p.hub)
            })
            .collect();
        if entries.is_empty() {
            return "No peers. Share your id from crosstalk whoami, or run crosstalk connect <id>.".into();
        }
        entries.join("\n")
    }

    pub fn forget(&mut self, id: Option<&str>) -> Result<Outgoing, String> {
        let Some(id) = id else {
            self.stop();
            self.hubs.clear();
            self.store.wipe();
            return Ok(Outgoing::Done("identity deleted. Restart the session to get a new id.".into()));
        };
        let peer = self.store.peers.get(id).cloned().ok_or_else(|| format!("{id} is not a peer"))?;
        let env = (peer.state == PeerState::Accepted).then(|| self.sealed(id, PayloadKind::Forget, "")).transpose()?;
        self.store.peers.insert(id.to_string(), PeerRecord { state: PeerState::Forgotten, description: None, ..peer.clone() });
        self.store.save_peers();
        let line = format!("forgot {id}");
        Ok(match env {
            Some(env) => Outgoing::Send { hub: peer.hub, env, done: Box::new(move |_| line) },
            None => Outgoing::Done(line),
        })
    }

    pub async fn send_outgoing(app: &tokio::sync::Mutex<App>, out: Outgoing) -> String {
        match out {
            Outgoing::Done(text) => text,
            Outgoing::Send { hub, env, done } => {
                let sender = app.lock().await.hub_by_host(&hub).sender();
                done(sender.send(env).await)
            }
        }
    }

    fn drain_inbox(&mut self) -> Option<String> {
        if self.inbox.is_empty() {
            return None;
        }
        let lines: Vec<String> = self
            .inbox
            .drain(..)
            .map(|e| {
                let peer = e.meta.get("peer").map(|p| format!(" ({p})")).unwrap_or_default();
                format!("[{}] from {} via {}{peer}\n{}", e.meta["kind"], e.meta["from"], e.meta["hub"], e.content)
            })
            .collect();
        Some(lines.join("\n\n"))
    }

    // Kept for the inbox, since some hosts ignore the push, and handed to the sink.
    fn notify(&mut self, content: String, meta: Meta) {
        self.inbox.push_back(Event { content: content.clone(), meta: meta.clone() });
        if self.inbox.len() > INBOX_MAX {
            self.inbox.pop_front();
        }
        self.inbox_notify.notify_waiters();
        (self.sink)(content, meta);
    }

    fn remember(&mut self, id: &str) -> bool {
        if !self.seen.insert(id.to_string()) {
            return false;
        }
        self.seen_order.push_back(id.to_string());
        if self.seen_order.len() > SEEN_MAX {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }

    /// Returns an accept to send back when the pairing settles on our side.
    pub fn on_deliver(&mut self, env: Envelope, hub: String) -> Option<Outgoing> {
        if !self.remember(&env.id) {
            return None;
        }
        let payload = match self.identity.open(&env) {
            Ok(p) => p,
            Err(err) => {
                eprintln!("[crosstalk] dropped envelope from {}: {err}", env.from);
                return None;
            }
        };
        let from = env.from.clone();
        let peer = self.store.peers.get(&from).cloned();
        let meta = |kind: &str| -> Meta {
            [("from".to_string(), from.clone()), ("hub".to_string(), hub.clone()), ("kind".to_string(), kind.to_string())].into_iter().collect()
        };
        let accept_back = |app: &App| -> Option<Outgoing> {
            let env = app.sealed(&from, PayloadKind::Accept, &app.self_description).ok()?;
            Some(Outgoing::Send { hub: hub.clone(), env, done: Box::new(|_| String::new()) })
        };
        match payload.kind {
            PayloadKind::Intro => {
                let description = cap(&payload.text, DESCRIPTION_MAX);
                match peer.as_ref().map(|p| p.state) {
                    Some(PeerState::Forgotten) => None,
                    // An accepted peer introducing itself again lost our accept: send it again.
                    Some(PeerState::Accepted) => {
                        self.store.peers.get_mut(&from).unwrap().description = Some(description);
                        self.store.save_peers();
                        accept_back(self)
                    }
                    // Both sides introduced each other: that is mutual consent.
                    Some(PeerState::PendingOut) => {
                        let p = self.store.peers.get_mut(&from).unwrap();
                        p.state = PeerState::Accepted;
                        p.description = Some(description.clone());
                        self.store.save_peers();
                        self.notify(description, meta("accepted"));
                        accept_back(self)
                    }
                    _ => {
                        self.store.peers.insert(from.clone(), PeerRecord { hub: hub.clone(), state: PeerState::PendingIn, description: Some(description.clone()), since: now_ms() });
                        self.store.save_peers();
                        self.notify(description, meta("intro"));
                        None
                    }
                }
            }
            PayloadKind::Accept => {
                if peer.as_ref().map(|p| p.state) != Some(PeerState::PendingOut) {
                    return None;
                }
                let description = cap(&payload.text, DESCRIPTION_MAX);
                let p = self.store.peers.get_mut(&from).unwrap();
                p.state = PeerState::Accepted;
                p.description = Some(description.clone());
                self.store.save_peers();
                self.notify(description, meta("accepted"));
                None
            }
            PayloadKind::Msg => {
                let state = peer.as_ref().map(|p| p.state);
                // A signed message from a peer we asked to pair with means it accepted us and the accept was lost.
                if state == Some(PeerState::PendingOut) {
                    self.store.peers.get_mut(&from).unwrap().state = PeerState::Accepted;
                    self.store.save_peers();
                    self.notify(String::new(), meta("accepted"));
                } else if state != Some(PeerState::Accepted) {
                    return None;
                }
                let mut m = meta("message");
                m.insert("peer".into(), attr(peer.and_then(|p| p.description).as_deref().unwrap_or("")));
                self.notify(payload.text, m);
                None
            }
            PayloadKind::Forget => {
                if peer.is_none() || peer.as_ref().map(|p| p.state) == Some(PeerState::Forgotten) {
                    return None;
                }
                self.store.peers.remove(&from);
                self.store.save_peers();
                self.notify(String::new(), meta("forgot"));
                None
            }
        }
    }
}

/// A command's result: either final text, or an envelope to send on a hub and a formatter for the status.
pub enum Outgoing {
    Done(String),
    Send { hub: String, env: Envelope, done: Box<dyn FnOnce(String) -> String + Send> },
}
