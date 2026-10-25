// The channel process: one identity, its hub connections, and the control socket.
// As `Role::Mcp` it also serves Claude Code over stdio; as `Role::Daemon` it runs alone.
use crate::app::{App, Sink};
use crate::control::{ControlRequest, ControlServer, Ownership};
use crate::store::{default_home, resolve_session_id, Store};
use crate::{codex, mcp, DEFAULT_DESCRIPTION, INBOX_WAIT_MAX};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

pub enum Role {
    Mcp,
    Daemon { codex_thread: Option<String> },
}

/// What a control command needs besides its arguments.
struct Channel {
    app: Mutex<App>,
    stop: Notify,
    session: String,
    daemon: bool,
    codex_thread: Option<String>,
}

pub async fn run(role: Role) -> Result<(), String> {
    let (daemon, codex_thread) = match role {
        Role::Mcp => (false, None),
        Role::Daemon { codex_thread } => (true, codex_thread),
    };
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
    let tag = if daemon { " (daemon)" } else { "" };
    eprintln!("[crosstalk] session {session} is {}{tag}", identity.id);

    let writer = mcp::stdout_writer();
    let sink: Sink = match (&codex_thread, daemon) {
        (Some(thread), true) => codex::waker(thread.clone()),
        (None, true) => Box::new(|_, _| {}),
        (_, false) => mcp::channel_sink(writer.clone()),
    };

    let (app, mut delivered) = App::new(store, identity, sink);
    let session_dir = app.store.dir.clone();
    let channel =
        Arc::new(Channel { app: Mutex::new(app), stop: Notify::new(), session, daemon, codex_thread });

    tokio::spawn({
        let channel = channel.clone();
        async move {
            while let Some((env, hub)) = delivered.recv().await {
                let reply = channel.app.lock().await.on_deliver(env, hub);
                if let Some(reply) = reply {
                    App::send_outgoing(&channel.app, reply).await;
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
        // Claude Code ends the server by closing stdin; without this the hub sockets keep the
        // process alive.
        let serve = mcp::serve(writer, mcp::Mode::Channel, {
            let channel = channel.clone();
            move |_thread, cmd, args| {
                let channel = channel.clone();
                async move { handle(&channel, ControlRequest { cmd, args }).await }
            }
        });
        tokio::select! {
            _ = serve => {}
            _ = stop_wait(&channel.stop) => {}
        }
    }
    if let Some(control) = &control {
        control.stop();
    }
    channel.app.lock().await.stop();
    Ok(())
}

async fn stop_wait(stop: &Notify) {
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
        _ = stop.notified() => {}
    }
}

fn text_after(args: &[String], from: usize, default: &str) -> String {
    let text = args[from..].join(" ");
    if text.is_empty() {
        default.to_string()
    } else {
        text
    }
}

async fn handle(channel: &Arc<Channel>, req: ControlRequest) -> Result<String, String> {
    let app = &channel.app;
    let args = req.args;
    let need = |n: usize| {
        if args.len() < n {
            let plural = if n > 1 { "s" } else { "" };
            return Err(format!("{} needs {n} argument{plural}", req.cmd));
        }
        Ok(())
    };
    let outgoing = match req.cmd.as_str() {
        "ping" => return Ok("pong".into()),
        "whoami" => return Ok(app.lock().await.whoami()),
        "peers" => return Ok(app.lock().await.peers()),
        "status" => return Ok(status(channel).await),
        "stop" => return schedule_stop(channel),
        "add-hub" => {
            need(1)?;
            return app.lock().await.add_hub(&args[0]);
        }
        "inbox" => {
            let wait = args.first().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0).min(INBOX_WAIT_MAX);
            let events = App::inbox(app, Duration::from_secs(wait)).await;
            return Ok(events.unwrap_or_else(|| "inbox empty".into()));
        }
        "connect" => {
            need(1)?;
            app.lock().await.connect(&args[0], &text_after(&args, 1, DEFAULT_DESCRIPTION))?
        }
        "accept" => {
            need(1)?;
            app.lock().await.accept(&args[0], &text_after(&args, 1, DEFAULT_DESCRIPTION))?
        }
        "send" => {
            need(2)?;
            app.lock().await.send(&args[0], &text_after(&args, 1, ""))?
        }
        "forget" => app.lock().await.forget(args.first().map(String::as_str))?,
        other => return Err(format!("unknown command {other}")),
    };
    Ok(App::send_outgoing(app, outgoing).await)
}

async fn status(channel: &Channel) -> String {
    let app = channel.app.lock().await;
    json!({
        "pid": std::process::id(),
        "session": channel.session,
        "id": app.identity.id,
        "daemon": channel.daemon,
        "threadId": channel.codex_thread,
        "hubs": app.hub_status(),
    })
    .to_string()
}

fn schedule_stop(channel: &Arc<Channel>) -> Result<String, String> {
    if !channel.daemon {
        return Err("stop only applies to a daemon".into());
    }
    // The reply must reach the client before the socket goes away.
    let channel = channel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        channel.stop.notify_one();
    });
    Ok("stopping".into())
}
