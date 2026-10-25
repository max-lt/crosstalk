// The command line client: finds the session's channel process and talks to it over
// its control socket. Also the attached MCP server, which is the same client behind MCP.
use crate::control::{self, ControlRequest};
use crate::store::{default_home, session_dir, session_from_claude};
use crate::{codex, mcp, INBOX_WAIT_MAX};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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

async fn live_socket() -> Result<PathBuf, String> {
    let path = socket_path(&default_home()).await?;
    if !path.exists() {
        return Err(format!("no channel process for this session ({})", path.display()));
    }
    Ok(path)
}

async fn request_at(path: &Path, cmd: &str, args: Vec<String>) -> Result<String, String> {
    let timeout = Duration::from_secs(INBOX_WAIT_MAX + 5);
    let reply = control::request(path, &ControlRequest { cmd: cmd.into(), args }, timeout).await?;
    match reply.strip_prefix("error: ") {
        Some(err) => Err(err.to_string()),
        None => Ok(reply),
    }
}

async fn request(cmd: &str, args: Vec<String>) -> Result<String, String> {
    request_at(&live_socket().await?, cmd, args).await
}

pub async fn run(cmd: &str, args: Vec<String>) -> Result<(), String> {
    println!("{}", request(cmd, args).await?);
    Ok(())
}

// Each batch of events is one stdout write, so a line-oriented reader (Monitor, tail) gets
// them as they land.
pub async fn follow_inbox() -> Result<(), String> {
    loop {
        let events = request("inbox", vec![INBOX_WAIT_MAX.to_string()]).await?;
        if events != "inbox empty" {
            println!("{events}\n");
        }
    }
}

// For hosts that spawn MCP servers freely (Codex): the tools of a chat's daemon, forwarded
// over its control socket, with no identity or hub connection of their own. One attached
// server serves every chat: each call names its thread, and the daemon must confirm it.
// No fallback to "the only live daemon": a call without a thread is refused.
pub async fn attach_mcp(config: Option<PathBuf>) -> Result<(), String> {
    let config = Arc::new(config);
    mcp::serve(mcp::stdout_writer(), mcp::Mode::Attached, move |thread, cmd, args| {
        let config = config.clone();
        async move {
            if cmd == "inbox" {
                return Err("crosstalk_inbox is not available here: events arrive as queued turns".into());
            }
            let thread =
                thread.ok_or("thread_id is required: this chat's id, from the session start hook")?;
            let path = daemon_for_thread(&thread, config.as_deref()).await?;
            request_at(&path, &cmd, args).await
        }
    })
    .await;
    Ok(())
}

// Fails closed: a daemon that serves another chat, or none, is an error, never a fallback.
async fn daemon_for_thread(thread: &str, config: Option<&Path>) -> Result<PathBuf, String> {
    let session = codex::session_for_thread(thread, config)?;
    let path = control::control_socket_path(&session_dir(&default_home(), &session));
    if !path.exists() || !control::is_alive(&path).await {
        return Err(format!(
            "no crosstalk daemon for this chat (session {session}); the Codex SessionStart hook starts it"
        ));
    }
    let status = request_at(&path, "status", vec![]).await?;
    let status: serde_json::Value = serde_json::from_str(&status).map_err(|e| e.to_string())?;
    let owner_matches =
        status["daemon"] == true && status["session"] == session.as_str() && status["threadId"] == thread;
    if !owner_matches {
        return Err(format!("the process on session {session} is not the daemon of this chat"));
    }
    Ok(path)
}
