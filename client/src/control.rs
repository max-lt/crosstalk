// Control socket: one Unix socket per session, so the CLI drives the channel process
// without a model turn. One JSON request per connection, one text reply, then close.
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub const CONTROL_SOCKET: &str = "ctl.sock";

#[derive(Serialize, Deserialize)]
pub struct ControlRequest {
    pub cmd: String,
    #[serde(default)]
    pub args: Vec<String>,
}

pub fn control_socket_path(session_dir: &Path) -> PathBuf {
    session_dir.join(CONTROL_SOCKET)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// Several processes may share one session (Codex respawns its MCP servers without
    /// killing the old ones). The first live owner keeps the socket; the others skip it.
    Shared,
    /// A daemon must be the only owner.
    Exclusive,
}

pub async fn is_alive(path: &Path) -> bool {
    matches!(request(path, &ControlRequest { cmd: "ping".into(), args: vec![] }, Duration::from_secs(1)).await.as_deref(), Ok("pong"))
}

pub struct ControlServer {
    path: PathBuf,
}

impl ControlServer {
    /// Binds the socket and serves requests in a background task. Returns None when another live process owns it.
    pub async fn start<F, Fut>(session_dir: &Path, ownership: Ownership, handle: F) -> Result<Option<ControlServer>, String>
    where
        F: Fn(ControlRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, String>> + Send + 'static,
    {
        let path = control_socket_path(session_dir);
        if path.exists() {
            if is_alive(&path).await {
                if ownership == Ownership::Exclusive {
                    return Err(format!("another process owns {}", path.display()));
                }
                eprintln!("[crosstalk] control socket owned by another process, skipping");
                return Ok(None);
            }
            let _ = std::fs::remove_file(&path);
        }
        let listener = UnixListener::bind(&path).map_err(|e| format!("cannot bind {}: {e}", path.display()))?;
        let handle = Arc::new(handle);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                let handle = handle.clone();
                tokio::spawn(async move {
                    let _ = serve_one(stream, &*handle).await;
                });
            }
        });
        Ok(Some(ControlServer { path }))
    }

    pub fn stop(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn serve_one<F, Fut>(stream: UnixStream, handle: &F) -> std::io::Result<()>
where
    F: Fn(ControlRequest) -> Fut,
    Fut: Future<Output = Result<String, String>>,
{
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let reply = match serde_json::from_str::<ControlRequest>(line.trim_end()) {
        Ok(req) => match handle(req).await {
            Ok(text) => text,
            Err(err) => format!("error: {err}"),
        },
        Err(_) => "error: bad request".into(),
    };
    writer.write_all(reply.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.shutdown().await
}

/// Client side: send one request, return the reply without its trailing newline.
pub async fn request(path: &Path, req: &ControlRequest, timeout: Duration) -> Result<String, String> {
    let io = async {
        let mut stream = UnixStream::connect(path).await.map_err(|e| e.to_string())?;
        let mut line = serde_json::to_string(req).map_err(|e| e.to_string())?;
        line.push('\n');
        stream.write_all(line.as_bytes()).await.map_err(|e| e.to_string())?;
        let mut reply = String::new();
        BufReader::new(stream).read_to_string(&mut reply).await.map_err(|e| e.to_string())?;
        Ok(reply.trim_end_matches('\n').to_string())
    };
    tokio::time::timeout(timeout, io).await.map_err(|_| "control request timed out".to_string())?
}
