mod app;
mod channel;
mod claude_hook;
mod cli;
mod codex;
mod control;
mod crypto;
mod hub;
mod mcp;
mod protocol;
mod store;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub const DEFAULT_DESCRIPTION: &str = "a crosstalk session";
/// One control request blocks at most this long; `inbox --follow` chains requests.
pub const INBOX_WAIT_MAX: u64 = 300;

/// Message agent sessions across machines, accounts and tools.
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
    Mcp {
        /// Expose the tools of existing daemons instead of running a channel process
        #[arg(long)]
        attach: bool,
        /// Codex hook config, for the chat-to-session mapping (with --attach)
        #[arg(long, requires = "attach")]
        config: Option<PathBuf>,
    },
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
    let args = Cli::parse();
    rustls::crypto::ring::default_provider().install_default().expect("first and only provider install");
    let result = match args.command {
        Command::Whoami => cli::run("whoami", vec![]).await,
        Command::Peers => cli::run("peers", vec![]).await,
        Command::Connect { target, description } => {
            cli::run("connect", with_text(target, description, DEFAULT_DESCRIPTION)).await
        }
        Command::Accept { id, description } => {
            cli::run("accept", with_text(id, description, DEFAULT_DESCRIPTION)).await
        }
        Command::Send { id, text } => cli::run("send", with_text(id, text, "")).await,
        Command::Inbox { follow: true, .. } => cli::follow_inbox().await,
        Command::Inbox { wait, .. } => cli::run("inbox", vec![wait.min(INBOX_WAIT_MAX).to_string()]).await,
        Command::Forget { id, all: _ } => cli::run("forget", id.into_iter().collect()).await,
        Command::AddHub { url } => cli::run("add-hub", vec![url]).await,
        Command::Status => cli::run("status", vec![]).await,
        Command::Stop => cli::run("stop", vec![]).await,
        Command::Mcp { attach: false, .. } => channel::run(channel::Role::Mcp).await,
        Command::Mcp { attach: true, config } => cli::attach_mcp(config).await,
        Command::Daemon { codex_thread } => channel::run(channel::Role::Daemon { codex_thread }).await,
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
