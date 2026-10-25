mod app;
mod claude_hook;
mod codex;
mod control;
mod crypto;
mod hub;
mod mcp;
mod protocol;
mod store;

use app::{App, Meta, Outgoing, Sink};
use clap::{Parser, Subcommand};
use control::{ControlRequest, ControlServer, Ownership};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use store::{default_home, resolve_session_id, session_dir, session_from_claude, Store};
use tokio::sync::{Mutex, Notify};

const DEFAULT_DESCRIPTION: &str = "a crosstalk session";
// One control request blocks at most this long; --follow chains requests.
const INBOX_WAIT_MAX: u64 = 300;

/// Message other people's agent sessions, across accounts.
#[derive(Parser)]
#[command(name = "crosstalk", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show this session's id and hub connections
    Whoami,
    /// List peers and their pairing state
    Peers,
    /// Send a pairing request to another session
    Connect {
        /// [<hub host>:]crosstalk_<id>
        target: String,
        /// One line about you and this session, shown to the other person
        description: Vec<String>,
    },
    /// Accept a pending pairing request
    Accept {
        id: String,
        /// One line about you and this session, sent back to them
        description: Vec<String>,
    },
    /// Send a message to an accepted peer
    Send { id: String, text: Vec<String> },
    /// Read pending events (for hosts without channel push)
    Inbox {
        /// Wait up to this many seconds for the first event
        #[arg(long, value_name = "SECONDS", default_value_t = 0, conflicts_with = "follow")]
        wait: u64,
        /// Keep printing events as they arrive, until stopped
        #[arg(long)]
        follow: bool,
    },
    /// Drop a peer
    Forget {
        /// The peer to drop
        #[arg(required_unless_present = "all", conflicts_with = "all")]
        id: Option<String>,
        /// Delete this session's identity and all its peers instead
        #[arg(long)]
        all: bool,
    },
    /// Add a relay hub to your hub list and connect to it
    AddHub { url: String },
    /// Pid, session, thread and hub state of the channel process, as JSON
    Status,
    /// Stop a daemon
    Stop,
    /// Run the channel process as a stdio MCP server (what the Claude Code plugin starts)
    Mcp,
    /// Run the channel process alone: hub connection and control socket, no MCP
    Daemon {
        /// Wake this Codex chat with `codex queue` on every inbound event
        #[arg(long, value_name = "UUID")]
        codex_thread: Option<String>,
    },
    /// Claude Code hook: session start message, and event delivery when channel push is off
    ClaudeHook,
    /// Codex SessionStart hook: start this chat's daemon (hook JSON on stdin)
    CodexHook {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    rustls::crypto::ring::default_provider().install_default().expect("first and only provider install");
    let result = match cli.command {
        Command::Whoami => client("whoami", vec![]).await,
        Command::Peers => client("peers", vec![]).await,
        Command::Connect { target, description } => client("connect", with_text(target, description, DEFAULT_DESCRIPTION)).await,
        Command::Accept { id, description } => client("accept", with_text(id, description, DEFAULT_DESCRIPTION)).await,
        Command::Send { id, text } => client("send", with_text(id, text, "")).await,
        Command::Inbox { follow: true, .. } => follow_inbox().await,
        Command::Inbox { wait, .. } => client("inbox", vec![wait.min(INBOX_WAIT_MAX).to_string()]).await,
        Command::Forget { id, all: _ } => client("forget", id.into_iter().collect()).await,
        Command::AddHub { url } => client("add-hub", vec![url]).await,
        Command::Status => client("status", vec![]).await,
        Command::Stop => client("stop", vec![]).await,
        Command::Mcp => run_channel(false, None).await,
        Command::Daemon { codex_thread } => run_channel(true, codex_thread).await,
        Command::ClaudeHook => claude_hook::run().await,
        Command::CodexHook { config } => codex::hook(&config).await,
    };
    if let Err(err) = result {
        eprintln!("crosstalk: {err}");
        std::process::exit(1);
    }
}

fn with_text(first: String, words: Vec<String>, default: &str) -> Vec<String> {
    let text = words.join(" ");
    vec![first, if text.is_empty() { default.to_string() } else { text }]
}

// ---- CLI client ----

async fn socket_path(home: &Path) -> Result<PathBuf, String> {
    if let Some(sid) = std::env::var("CROSSTALK_SESSION").ok().or_else(|| session_from_claude(home)) {
        return Ok(control::control_socket_path(&session_dir(home, &sid)));
    }
    let mut live = vec![];
    if let Ok(entries) = std::fs::read_dir(home.join("sessions")) {
        for entry in entries.flatten() {
            let path = control::control_socket_path(&entry.path());
            if path.exists() && control::is_alive(&path).await {
                live.push(path);
            }
        }
    }
    match live.len() {
        1 => Ok(live.remove(0)),
        0 => Err("no running crosstalk session found".into()),
        n => Err(format!("{n} sessions running; set CROSSTALK_SESSION or run from inside Claude Code")),
    }
}

async fn request(cmd: &str, args: Vec<String>) -> Result<String, String> {
    let path = socket_path(&default_home()).await?;
    if !path.exists() {
        return Err(format!("no channel process for this session ({})", path.display()));
    }
    let timeout = Duration::from_secs(INBOX_WAIT_MAX + 5);
    let reply = control::request(&path, &ControlRequest { cmd: cmd.into(), args }, timeout).await?;
    match reply.strip_prefix("error: ") {
        Some(err) => Err(err.to_string()),
        None => Ok(reply),
    }
}

async fn client(cmd: &str, args: Vec<String>) -> Result<(), String> {
    println!("{}", request(cmd, args).await?);
    Ok(())
}

// Each batch of events is one stdout write, so a line-oriented reader (Monitor, tail) gets them as they land.
async fn follow_inbox() -> Result<(), String> {
    loop {
        let events = request("inbox", vec![INBOX_WAIT_MAX.to_string()]).await?;
        if events != "inbox empty" {
            println!("{events}\n");
        }
    }
}

// ---- Channel process ----

// Codex has no channel push: each event becomes a queued user turn in one explicit thread.
// Failed queue commands are retried in order; the hub never redelivers, so nothing else dedupes.
fn codex_waker(thread: String) -> Sink {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            loop {
                let run = tokio::process::Command::new("codex").args(["queue", "--thread", &thread, "--message", &message]).stdout(std::process::Stdio::null()).output();
                match tokio::time::timeout(Duration::from_secs(30), run).await {
                    Ok(Ok(out)) if out.status.success() => break,
                    Ok(Ok(out)) => eprintln!("[crosstalk] codex queue failed ({}): {}; retry in 5s", out.status, String::from_utf8_lossy(&out.stderr).trim()),
                    Ok(Err(err)) => eprintln!("[crosstalk] codex queue failed ({err}); retry in 5s"),
                    Err(_) => eprintln!("[crosstalk] codex queue timed out; retry in 5s"),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });
    Box::new(move |content, meta: Meta| {
        // Routing details go to the daemon log, not into the chat.
        eprintln!("[crosstalk] codex event {}", json!(meta));
        let _ = tx.send(codex::format_event(&content, &meta));
    })
}

/// What a control command needs besides its arguments.
struct Channel {
    app: Mutex<App>,
    stop: Notify,
    session: String,
    daemon: bool,
    codex_thread: Option<String>,
}

async fn run_channel(daemon: bool, codex_thread: Option<String>) -> Result<(), String> {
    if daemon && std::env::var("CROSSTALK_SESSION").is_err() {
        return Err("daemon needs CROSSTALK_SESSION".into());
    }
    if codex_thread.as_deref().is_some_and(|t| !codex::is_thread_id(t)) {
        return Err("--codex-thread needs a thread uuid".into());
    }
    let home = default_home();
    let session = resolve_session_id(&home).await;
    let store = Store::open(home, &session).map_err(|e| e.to_string())?;
    let identity = store.load_or_create_identity().map_err(|e| e.to_string())?;
    eprintln!("[crosstalk] session {session} is {}{}", identity.id, if daemon { " (daemon)" } else { "" });

    let writer = mcp::stdout_writer();
    let sink: Sink = match (&codex_thread, daemon) {
        (Some(thread), true) => codex_waker(thread.clone()),
        (None, true) => Box::new(|_, _| {}),
        (_, false) => {
            let (tx, pump) = mcp::channel_sink(writer.clone());
            tokio::spawn(pump);
            Box::new(move |content, meta| {
                let _ = tx.send((content, meta));
            })
        }
    };

    let (app, mut delivered) = App::new(store, identity, sink);
    let session_dir = app.store.dir.clone();
    let channel = Arc::new(Channel { app: Mutex::new(app), stop: Notify::new(), session, daemon, codex_thread });

    // Inbound envelopes from every hub.
    tokio::spawn({
        let channel = channel.clone();
        async move {
            while let Some((env, hub)) = delivered.recv().await {
                let out = channel.app.lock().await.on_deliver(env, hub);
                if let Some(out) = out {
                    App::send_outgoing(&channel.app, out).await;
                }
            }
        }
    });

    let ownership = if daemon { Ownership::Exclusive } else { Ownership::Shared };
    let control = ControlServer::start(&session_dir, ownership, {
        let channel = channel.clone();
        move |req| {
            let channel = channel.clone();
            async move { handle(&channel, req).await }
        }
    })
    .await?;
    channel.app.lock().await.start();

    if daemon {
        stop_wait(&channel.stop).await;
    } else {
        // Claude Code ends the server by closing stdin; without this the hub sockets keep the process alive.
        let serve = mcp::serve(writer, {
            let channel = channel.clone();
            move |cmd, args| {
                let channel = channel.clone();
                async move { handle(&channel, ControlRequest { cmd, args }).await }
            }
        });
        tokio::select! {
            _ = serve => {}
            _ = stop_wait(&channel.stop) => {}
        }
    }
    if let Some(c) = &control {
        c.stop();
    }
    channel.app.lock().await.stop();
    Ok(())
}

async fn stop_wait(stop: &Notify) {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
        _ = stop.notified() => {}
    }
}

async fn handle(channel: &Arc<Channel>, req: ControlRequest) -> Result<String, String> {
    let app = &channel.app;
    let args = req.args;
    let need = |n: usize| if args.len() < n { Err(format!("{} needs {n} argument{}", req.cmd, if n > 1 { "s" } else { "" })) } else { Ok(()) };
    let rest = |from: usize, default: &str| {
        let text = args[from..].join(" ");
        if text.is_empty() { default.to_string() } else { text }
    };
    let out: Outgoing = match req.cmd.as_str() {
        "ping" => return Ok("pong".into()),
        "whoami" => return Ok(app.lock().await.whoami()),
        "peers" => return Ok(app.lock().await.peers()),
        "status" => {
            let guard = app.lock().await;
            return Ok(json!({
                "pid": std::process::id(),
                "session": channel.session,
                "id": guard.identity.id,
                "daemon": channel.daemon,
                "threadId": channel.codex_thread,
                "hubs": guard.hub_status(),
            })
            .to_string());
        }
        "stop" => {
            if !channel.daemon {
                return Err("stop only applies to a daemon".into());
            }
            // The reply must reach the client before the socket goes away.
            let channel = channel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                channel.stop.notify_one();
            });
            return Ok("stopping".into());
        }
        "add-hub" => {
            need(1)?;
            return app.lock().await.add_hub(&args[0]);
        }
        "inbox" => {
            let wait = args.first().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0).min(INBOX_WAIT_MAX);
            return Ok(App::inbox(app, Duration::from_secs(wait)).await.unwrap_or_else(|| "inbox empty".into()));
        }
        "connect" => {
            need(1)?;
            app.lock().await.connect(&args[0], &rest(1, DEFAULT_DESCRIPTION))?
        }
        "accept" => {
            need(1)?;
            app.lock().await.accept(&args[0], &rest(1, DEFAULT_DESCRIPTION))?
        }
        "send" => {
            need(2)?;
            app.lock().await.send(&args[0], &rest(1, ""))?
        }
        "forget" => app.lock().await.forget(args.first().map(String::as_str))?,
        other => return Err(format!("unknown command {other}")),
    };
    Ok(App::send_outgoing(app, out).await)
}
