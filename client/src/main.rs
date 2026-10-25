mod app;
mod claude_hook;
mod control;
mod codex;
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
use tokio::sync::Mutex;

/// Message other people's agent sessions, across accounts.
#[derive(Parser)]
#[command(name = "crosstalk", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Claude Code Stop and UserPromptSubmit hook: deliver pending events when channel push is off
    ClaudeHook,
    /// Start this chat's channel from a Codex SessionStart hook (JSON on stdin)
    CodexHook {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the channel process as a stdio MCP server (what the Claude Code plugin starts)
    Mcp,
    /// Run the channel process alone: hub connection and control socket, no MCP
    Daemon {
        /// Wake this Codex chat with `codex queue` on every inbound event
        #[arg(long, value_name = "UUID")]
        codex_thread: Option<String>,
    },
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
        #[arg(default_value_t = 0)]
        wait_seconds: u64,
    },
    /// Drop a peer, or without id delete this session's identity
    Forget { id: Option<String> },
    /// Add a relay hub to your hub list and connect to it
    AddHub { url: String },
    /// Pid, session, thread and hub state of the channel process, as JSON
    Status,
    /// Stop a daemon
    Stop,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    rustls::crypto::ring::default_provider().install_default().expect("first and only provider install");
    let result = match cli.command {
        Command::ClaudeHook => claude_hook::run().await,
        Command::CodexHook { config } => codex::hook(&config).await,
        Command::Mcp => run_channel(false, None).await,
        Command::Daemon { codex_thread } => run_channel(true, codex_thread).await,
        Command::Whoami => client("whoami", vec![]).await,
        Command::Peers => client("peers", vec![]).await,
        Command::Connect { target, description } => client("connect", with_text(target, description, "a Claude Code session")).await,
        Command::Accept { id, description } => client("accept", with_text(id, description, "a Claude Code session")).await,
        Command::Send { id, text } => client("send", with_text(id, text, "")).await,
        Command::Inbox { wait_seconds } => client("inbox", vec![wait_seconds.to_string()]).await,
        Command::Forget { id } => client("forget", id.into_iter().collect()).await,
        Command::AddHub { url } => client("add-hub", vec![url]).await,
        Command::Status => client("status", vec![]).await,
        Command::Stop => client("stop", vec![]).await,
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

async fn client(cmd: &str, args: Vec<String>) -> Result<(), String> {
    let path = socket_path(&default_home()).await?;
    if !path.exists() {
        return Err(format!("no channel process for this session ({})", path.display()));
    }
    let reply = control::request(&path, &ControlRequest { cmd: cmd.into(), args }, Duration::from_secs(60)).await?;
    if let Some(err) = reply.strip_prefix("error: ") {
        return Err(err.to_string());
    }
    println!("{reply}");
    Ok(())
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
        // Keep routing details in the daemon log, not in the chat bubble.
        eprintln!("[crosstalk] codex event {}", json!(meta));
        let _ = tx.send(codex::format_event(&content, &meta));
    })
}

async fn run_channel(daemon: bool, codex_thread: Option<String>) -> Result<(), String> {
    if daemon && std::env::var("CROSSTALK_SESSION").is_err() {
        return Err("daemon needs CROSSTALK_SESSION".into());
    }
    if let Some(t) = &codex_thread {
        if t.len() != 36 || !t.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            return Err("--codex-thread needs a thread uuid".into());
        }
    }
    let home = default_home();
    let session = resolve_session_id(&home).await;
    let store = Store::open(home, &session).map_err(|e| e.to_string())?;
    let identity = store.load_or_create_identity().map_err(|e| e.to_string())?;
    eprintln!("[crosstalk] session {session} is {}{}", identity.id, if daemon { " (daemon)" } else { "" });

    let writer = mcp::stdout_writer();
    let (sink, pump): (Sink, Option<_>) = if daemon {
        (codex_thread.clone().map(codex_waker).unwrap_or_else(|| Box::new(|_, _| {})), None)
    } else {
        let (tx, pump) = mcp::channel_sink(writer.clone());
        (Box::new(move |content, meta| { let _ = tx.send((content, meta)); }), Some(pump))
    };
    if let Some(pump) = pump {
        tokio::spawn(pump);
    }

    let (app, mut delivered) = App::new(store, identity, sink);
    let session_dir = app.store.dir.clone();
    let app = Arc::new(Mutex::new(app));

    // Inbound envelopes from every hub.
    tokio::spawn({
        let app = app.clone();
        async move {
            while let Some((env, hub)) = delivered.recv().await {
                let out = app.lock().await.on_deliver(env, hub);
                if let Some(out) = out {
                    App::send_outgoing(&app, out).await;
                }
            }
        }
    });

    let stop = Arc::new(tokio::sync::Notify::new());
    let handler = {
        let app = app.clone();
        let stop = stop.clone();
        let session = session.clone();
        move |req: ControlRequest| {
            let app = app.clone();
            let stop = stop.clone();
            let session = session.clone();
            let codex_thread = codex_thread.clone();
            async move { handle(&app, &stop, &session, daemon, codex_thread, req).await }
        }
    };
    let control = ControlServer::start(&session_dir, if daemon { Ownership::Exclusive } else { Ownership::Shared }, handler).await?;
    app.lock().await.start();

    let ctrl_handler = {
        let app = app.clone();
        let stop = stop.clone();
        move |cmd: String, args: Vec<String>| {
            let app = app.clone();
            let stop = stop.clone();
            let session = session.clone();
            async move { handle(&app, &stop, &session, daemon, None, ControlRequest { cmd, args }).await }
        }
    };
    let stop_signal = stop_wait(stop.clone());
    if daemon {
        stop_signal.await;
    } else {
        // Claude Code ends the server by closing stdin; without this the hub sockets keep the process alive.
        tokio::select! {
            _ = mcp::serve(writer, ctrl_handler) => {}
            _ = stop_signal => {}
        }
    }
    if let Some(c) = &control {
        c.stop();
    }
    app.lock().await.stop();
    Ok(())
}

async fn stop_wait(stop: Arc<tokio::sync::Notify>) {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
        _ = stop.notified() => {}
    }
}

async fn handle(app: &Arc<Mutex<App>>, stop: &Arc<tokio::sync::Notify>, session: &str, daemon: bool, thread: Option<String>, req: ControlRequest) -> Result<String, String> {
    let args = req.args;
    let need = |n: usize| if args.len() < n { Err(format!("{} needs {n} argument{}", req.cmd, if n > 1 { "s" } else { "" })) } else { Ok(()) };
    let rest = |from: usize, default: &str| { let t = args[from..].join(" "); if t.is_empty() { default.to_string() } else { t } };
    let out: Outgoing = match req.cmd.as_str() {
        "ping" => return Ok("pong".into()),
        "whoami" => return Ok(app.lock().await.whoami()),
        "peers" => return Ok(app.lock().await.peers()),
        "status" => {
            let guard = app.lock().await;
            return Ok(json!({ "pid": std::process::id(), "session": session, "id": guard.identity.id, "daemon": daemon, "threadId": thread, "hubs": guard.hub_status() }).to_string());
        }
        "stop" => {
            if !daemon {
                return Err("stop only applies to a daemon".into());
            }
            let stop = stop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                stop.notify_one();
            });
            return Ok("stopping".into());
        }
        "add-hub" => {
            need(1)?;
            return app.lock().await.add_hub(&args[0]);
        }
        "connect" => {
            need(1)?;
            app.lock().await.connect(&args[0], &rest(1, "a Claude Code session"))?
        }
        "accept" => {
            need(1)?;
            app.lock().await.accept(&args[0], &rest(1, "a Claude Code session"))?
        }
        "send" => {
            need(2)?;
            app.lock().await.send(&args[0], &rest(1, ""))?
        }
        "inbox" => {
            let wait = args.first().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0).min(55);
            let notify = app.lock().await.inbox_notify();
            if wait > 0 {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(text) = app.lock().await.drain_inbox() {
                    return Ok(text);
                }
                let _ = tokio::time::timeout(Duration::from_secs(wait), notified).await;
            }
            return Ok(app.lock().await.drain_inbox().unwrap_or_else(|| "inbox empty".into()));
        }
        "forget" => app.lock().await.forget(args.first().map(String::as_str))?,
        other => return Err(format!("unknown command {other}")),
    };
    Ok(App::send_outgoing(app, out).await)
}
