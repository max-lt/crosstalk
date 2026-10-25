// One WebSocket per hub, with authentication, keepalive and reconnection.
use crate::crypto::Identity;
use crate::protocol::{auth_message, ClientMessage, Envelope, HubMessage};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = SplitSink<Socket, Message>;
type Pending = HashMap<String, (oneshot::Sender<String>, Instant)>;

pub fn normalize_hub_url(input: &str) -> Result<String, String> {
    let text = if input.contains("://") { input.to_string() } else { format!("https://{input}") };
    let url = url::Url::parse(&text).map_err(|e| e.to_string())?;
    if url.scheme() != "https" && url.scheme() != "http" {
        return Err(format!("unsupported hub scheme: {}", url.scheme()));
    }
    Ok(url.origin().ascii_serialization())
}

pub fn hub_host(url: &str) -> String {
    let Ok(url) = url::Url::parse(url) else { return String::new() };
    let Some(host) = url.host_str() else { return String::new() };
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
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
        let link = Link { url, host: host.clone(), identity, delivered, status: status.clone() };
        tokio::spawn(link.run(rx));
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

/// The connection task's state. Lives as long as the hub is configured.
struct Link {
    url: String,
    host: String,
    identity: Arc<Identity>,
    delivered: Delivered,
    status: Arc<Mutex<&'static str>>,
}

/// Why a session on the socket ended.
enum Ended {
    Stopped,
    Dropped,
}

impl Link {
    fn set_status(&self, status: &'static str) {
        *self.status.lock().unwrap() = status;
    }

    fn ws_url(&self) -> url::Url {
        let mut url = url::Url::parse(&self.url).expect("validated hub url");
        let _ = url.set_scheme(if url.scheme() == "http" { "ws" } else { "wss" });
        url.set_path(&format!("/ws/{}", self.identity.id));
        url
    }

    async fn run(self, mut rx: mpsc::UnboundedReceiver<Command>) {
        let mut attempts: u32 = 0;
        loop {
            self.set_status("connecting");
            let Some(dialed) = self.dial(&mut rx).await else { return };
            let ended = match dialed {
                Ok(socket) => {
                    attempts = 0;
                    self.session(socket, &mut rx).await
                }
                Err(err) => {
                    eprintln!("[crosstalk] hub {}: {err}", self.host);
                    Ended::Dropped
                }
            };
            self.set_status("disconnected");
            if matches!(ended, Ended::Stopped) {
                return;
            }
            let delay = Duration::from_secs(1 << attempts.min(5)).min(MAX_BACKOFF);
            attempts += 1;
            eprintln!("[crosstalk] hub {} closed, retry in {}ms", self.host, delay.as_millis());
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                cmd = rx.recv() => if answer_offline(cmd) { return },
            }
        }
    }

    // Commands keep getting answers while the dial is in progress; a dead network must not
    // hang the caller. None means a stop arrived.
    async fn dial(&self, rx: &mut mpsc::UnboundedReceiver<Command>) -> Option<Result<Socket, String>> {
        let url = self.ws_url();
        let connect = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url.as_str()));
        tokio::pin!(connect);
        loop {
            tokio::select! {
                result = &mut connect => {
                    return Some(match result {
                        Ok(Ok((socket, _))) => Ok(socket),
                        Ok(Err(err)) => Err(err.to_string()),
                        Err(_) => Err("connect timed out".into()),
                    });
                }
                cmd = rx.recv() => if answer_offline(cmd) { return None },
            }
        }
    }

    async fn session(&self, socket: Socket, rx: &mut mpsc::UnboundedReceiver<Command>) -> Ended {
        let (mut sink, mut stream) = socket.split();
        let mut pending: Pending = HashMap::new();
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await;
        let mut sweep = tokio::time::interval(Duration::from_secs(1));
        let mut pong_due: Option<Instant> = None;
        let ended = loop {
            let pong_deadline = pong_due.map(tokio::time::sleep_until);
            tokio::select! {
                frame = stream.next() => {
                    let Some(Ok(Message::Text(text))) = frame else { break Ended::Dropped };
                    let Ok(msg) = serde_json::from_str::<HubMessage>(&text) else { continue };
                    if self.on_hub_message(msg, &mut sink, &mut pending, &mut pong_due).await.is_err() {
                        break Ended::Dropped;
                    }
                }
                cmd = rx.recv() => match cmd {
                    Some(Command::Send(env, reply)) => {
                        if self.forward(env, reply, &mut sink, &mut pending).await.is_err() { break Ended::Dropped }
                    }
                    Some(Command::Stop) | None => {
                        let _ = sink.close().await;
                        break Ended::Stopped;
                    }
                },
                _ = ping.tick() => {
                    if send_frame(&mut sink, &ClientMessage::Ping).await.is_err() { break Ended::Dropped }
                    pong_due.get_or_insert(Instant::now() + PONG_TIMEOUT);
                }
                _ = async { pong_deadline.unwrap().await }, if pong_deadline.is_some() => {
                    eprintln!("[crosstalk] hub {}: no pong, reconnecting", self.host);
                    break Ended::Dropped;
                }
                _ = sweep.tick() => expire(&mut pending),
            }
        };
        for (_, (reply, _)) in pending.drain() {
            let _ = reply.send("offline".into());
        }
        ended
    }

    async fn on_hub_message(
        &self,
        msg: HubMessage,
        sink: &mut Sink,
        pending: &mut Pending,
        pong_due: &mut Option<Instant>,
    ) -> Result<(), ()> {
        match msg {
            HubMessage::Challenge { nonce } => {
                // Sign the host we dialed, not one the hub claims, so a hub cannot replay our
                // auth elsewhere.
                let sig = self.identity.sign(&auth_message(&self.host, &nonce));
                send_frame(sink, &ClientMessage::Auth { sig }).await?;
            }
            HubMessage::Ready {} => {
                self.set_status("connected");
                eprintln!("[crosstalk] hub {} ready", self.host);
            }
            HubMessage::Pong => *pong_due = None,
            HubMessage::Deliver { env } => {
                let _ = self.delivered.send((env, self.host.clone()));
            }
            HubMessage::Status { r#ref, status } => {
                if let Some((reply, _)) = pending.remove(&r#ref) {
                    let _ = reply.send(status);
                }
            }
            HubMessage::Error { message } => eprintln!("[crosstalk] hub {}: {message}", self.host),
        }
        Ok(())
    }

    async fn forward(
        &self,
        env: Envelope,
        reply: oneshot::Sender<String>,
        sink: &mut Sink,
        pending: &mut Pending,
    ) -> Result<(), ()> {
        if *self.status.lock().unwrap() != "connected" {
            let _ = reply.send("offline".into());
            return Ok(());
        }
        let id = env.id.clone();
        if send_frame(sink, &ClientMessage::Send { env }).await.is_err() {
            let _ = reply.send("offline".into());
            return Err(());
        }
        pending.insert(id, (reply, Instant::now() + SEND_TIMEOUT));
        Ok(())
    }
}

async fn send_frame(sink: &mut Sink, msg: &ClientMessage) -> Result<(), ()> {
    let text = serde_json::to_string(msg).expect("client message serializes");
    sink.send(Message::Text(text.into())).await.map_err(|_| ())
}

/// Answers a command received while there is no connection. True when it was a stop.
fn answer_offline(cmd: Option<Command>) -> bool {
    match cmd {
        Some(Command::Send(_, reply)) => {
            let _ = reply.send("offline".into());
            false
        }
        Some(Command::Stop) | None => true,
    }
}

fn expire(pending: &mut Pending) {
    let now = Instant::now();
    let expired: Vec<String> =
        pending.iter().filter(|(_, (_, due))| *due <= now).map(|(id, _)| id.clone()).collect();
    for id in expired {
        if let Some((reply, _)) = pending.remove(&id) {
            let _ = reply.send("timeout".into());
        }
    }
}
