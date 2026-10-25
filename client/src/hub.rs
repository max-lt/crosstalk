use crate::crypto::Identity;
use crate::protocol::{auth_message, ClientMessage, Envelope, HubMessage};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

pub fn normalize_hub_url(input: &str) -> Result<String, String> {
    let text = if input.contains("://") { input.to_string() } else { format!("https://{input}") };
    let url = url::Url::parse(&text).map_err(|e| e.to_string())?;
    if url.scheme() != "https" && url.scheme() != "http" {
        return Err(format!("unsupported hub scheme: {}", url.scheme()));
    }
    Ok(url.origin().ascii_serialization())
}

pub fn hub_host(url: &str) -> String {
    url::Url::parse(url).ok().and_then(|u| u.host_str().map(|h| match u.port() {
        Some(p) => format!("{h}:{p}"),
        None => h.to_string(),
    })).unwrap_or_default()
}

pub type Delivered = mpsc::UnboundedSender<(Envelope, String)>;

enum Command {
    Send(Envelope, oneshot::Sender<String>),
    Stop,
}

pub struct HubConnection {
    pub host: String,
    status: Arc<Mutex<&'static str>>,
    tx: mpsc::UnboundedSender<Command>,
}

impl HubConnection {
    pub fn start(url: String, identity: Arc<Identity>, delivered: Delivered) -> HubConnection {
        let host = hub_host(&url);
        let status = Arc::new(Mutex::new("connecting"));
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(url.clone(), host.clone(), identity, delivered, status.clone(), rx));
        HubConnection { host, status, tx }
    }

    pub fn status(&self) -> &'static str {
        *self.status.lock().unwrap()
    }

    /// A handle that sends without borrowing the connection, so no lock is held across the await.
    pub fn sender(&self) -> HubSender {
        HubSender(self.tx.clone())
    }

    pub fn stop(&self) {
        let _ = self.tx.send(Command::Stop);
    }
}

pub struct HubSender(mpsc::UnboundedSender<Command>);

impl HubSender {
    pub async fn send(&self, env: Envelope) -> String {
        let (reply, wait) = oneshot::channel();
        if self.0.send(Command::Send(env, reply)).is_err() {
            return "offline".into();
        }
        wait.await.unwrap_or_else(|_| "offline".into())
    }
}

async fn run(
    url: String,
    host: String,
    identity: Arc<Identity>,
    delivered: Delivered,
    status: Arc<Mutex<&'static str>>,
    mut rx: mpsc::UnboundedReceiver<Command>,
) {
    let mut attempts: u32 = 0;
    loop {
        *status.lock().unwrap() = "connecting";
        let ws_url = {
            let mut u = url::Url::parse(&url).expect("validated hub url");
            let _ = u.set_scheme(if u.scheme() == "http" { "ws" } else { "wss" });
            u.set_path(&format!("/ws/{}", identity.id));
            u
        };
        // Commands keep getting answers while the dial is in progress; a dead network
        // must not hang the caller.
        let connect = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(ws_url.as_str()));
        tokio::pin!(connect);
        let dialed = loop {
            tokio::select! {
                result = &mut connect => break result,
                cmd = rx.recv() => match cmd {
                    Some(Command::Send(_, reply)) => { let _ = reply.send("offline".into()); }
                    Some(Command::Stop) | None => return,
                },
            }
        };
        let stopped = match dialed {
            Ok(Ok((ws, _))) => {
                attempts = 0;
                session(ws, &host, &identity, &delivered, &status, &mut rx).await
            }
            Ok(Err(err)) => {
                eprintln!("[crosstalk] hub {host}: {err}");
                false
            }
            Err(_) => {
                eprintln!("[crosstalk] hub {host}: connect timed out");
                false
            }
        };
        *status.lock().unwrap() = "disconnected";
        if stopped {
            return;
        }
        let delay = Duration::from_secs(1 << attempts.min(5)).min(MAX_BACKOFF);
        attempts += 1;
        eprintln!("[crosstalk] hub {host} closed, retry in {}ms", delay.as_millis());
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            cmd = rx.recv() => match cmd {
                Some(Command::Send(_, reply)) => { let _ = reply.send("offline".into()); }
                Some(Command::Stop) | None => return,
            },
        }
    }
}

// Returns true when the connection ended because of a stop command.
async fn session(
    ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    host: &str,
    identity: &Identity,
    delivered: &Delivered,
    status: &Arc<Mutex<&'static str>>,
    rx: &mut mpsc::UnboundedReceiver<Command>,
) -> bool {
    let (mut sink, mut stream) = ws.split();
    let mut pending: HashMap<String, (oneshot::Sender<String>, tokio::time::Instant)> = HashMap::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await;
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    let mut pong_due: Option<tokio::time::Instant> = None;
    let stopped = loop {
        let pong_deadline = pong_due.map(tokio::time::sleep_until);
        tokio::select! {
            frame = stream.next() => {
                let Some(Ok(Message::Text(text))) = frame else { break false };
                let Ok(msg) = serde_json::from_str::<HubMessage>(&text) else { continue };
                match msg {
                    HubMessage::Challenge { nonce } => {
                        // Sign the host we dialed, not one the hub claims, so a hub can't replay our auth elsewhere.
                        let auth = ClientMessage::Auth { sig: identity.sign(&auth_message(host, &nonce)) };
                        if sink.send(Message::Text(serde_json::to_string(&auth).unwrap().into())).await.is_err() { break false }
                    }
                    HubMessage::Ready { .. } => {
                        *status.lock().unwrap() = "connected";
                        eprintln!("[crosstalk] hub {host} ready");
                    }
                    HubMessage::Pong => pong_due = None,
                    HubMessage::Deliver { env } => { let _ = delivered.send((env, host.to_string())); }
                    HubMessage::Status { r#ref, status } => {
                        if let Some((reply, _)) = pending.remove(&r#ref) { let _ = reply.send(status); }
                    }
                    HubMessage::Error { message } => eprintln!("[crosstalk] hub {host}: {message}"),
                }
            }
            cmd = rx.recv() => match cmd {
                Some(Command::Send(env, reply)) => {
                    if *status.lock().unwrap() != "connected" { let _ = reply.send("offline".into()); continue }
                    let id = env.id.clone();
                    let frame = serde_json::to_string(&ClientMessage::Send { env }).unwrap();
                    if sink.send(Message::Text(frame.into())).await.is_err() { let _ = reply.send("offline".into()); break false }
                    pending.insert(id, (reply, tokio::time::Instant::now() + SEND_TIMEOUT));
                }
                Some(Command::Stop) | None => { let _ = sink.close().await; break true }
            },
            _ = ping.tick() => {
                if sink.send(Message::Text(serde_json::to_string(&ClientMessage::Ping).unwrap().into())).await.is_err() { break false }
                if pong_due.is_none() { pong_due = Some(tokio::time::Instant::now() + PONG_TIMEOUT); }
            }
            _ = async { pong_deadline.unwrap().await }, if pong_deadline.is_some() => {
                eprintln!("[crosstalk] hub {host}: no pong, reconnecting");
                break false
            }
            _ = sweep.tick() => {
                let now = tokio::time::Instant::now();
                let expired: Vec<String> = pending.iter().filter(|(_, (_, due))| *due <= now).map(|(id, _)| id.clone()).collect();
                for id in expired {
                    if let Some((reply, _)) = pending.remove(&id) { let _ = reply.send("timeout".into()); }
                }
            }
        }
    };
    for (_, (reply, _)) in pending.drain() {
        let _ = reply.send("offline".into());
    }
    stopped
}
