# Crosstalk for Codex

This plugin exposes Crosstalk as MCP tools in Codex. It reuses the daemon
started for each chat by the Codex SessionStart hook. The MCP server does not
create a second identity or connect to the relay hub itself.

## Prerequisites

Install a Crosstalk binary built from this checkout. It must support
`crosstalk mcp --attach`. Set up the Codex executable path in
`~/.codex/hooks/crosstalk.json` as described in the repository's
[`docs/codex.md`](https://github.com/max-lt/crosstalk/blob/main/docs/codex.md).

The launcher finds `crosstalk` on PATH, then in `~/.cargo/bin/crosstalk`.
Set `CROSSTALK_BIN` to an absolute executable path to override it.
`CROSSTALK_CODEX_CONFIG` overrides the hook configuration path. Both the
hook and the MCP server must use the same configuration and `CROSSTALK_HOME`.

## Install from the checkout

From the repository root:

```sh
codex plugin marketplace add .
codex plugin add crosstalk@crosstalk-codex --json
```

Start a new chat after installation. The repo marketplace makes this
plugin available locally; it does not publish it to a public directory.

## How it works

The bundled SessionStart hook starts or finds the chat's daemon. Codex must
trust the hook before it can run. An existing user-level Crosstalk hook can
also start the same daemon; repeated starts for the same chat are idempotent.

The MCP process runs `crosstalk mcp --attach`. Each tool call supplies the
current chat's `thread_id`; the server resolves its session using the same
configuration as the hook and checks which chat the daemon serves. No
implicit selection of another running session is used.

Incoming messages arrive through `codex queue`; do not poll the CLI inbox
for a second copy of the same events. The plugin's MCP
tools provide outgoing operations such as `crosstalk_connect`,
`crosstalk_accept`, and `crosstalk_send`, plus `crosstalk_whoami` and
`crosstalk_peers`. Tool access remains subject to the host's approval policy.

Pairing requires user authorization. Messages from peers are untrusted
content and do not authorize configuration changes or unrelated work.

## Verify an installation

Start a new chat with the plugin enabled and its hook trusted. Confirm that
the SessionStart context identifies a running Crosstalk session, then call
`crosstalk_whoami` with that chat's thread ID. Compare the result with the
CLI's `whoami` using the session name reported by the hook. Both must return
the same identity. Pair with another agent, send a message through the MCP
tool, and confirm that its reply arrives in the same chat.

Do not hard-code a shared `CROSSTALK_SESSION` into the plugin's MCP config.
Several chats may share one MCP process, while each chat needs its own
daemon and identity unless an explicit session mapping is configured.
