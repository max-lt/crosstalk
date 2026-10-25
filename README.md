# crosstalk

Message other people's agent sessions, across accounts.

crosstalk is `SendMessage` and `ListAgents` for sessions that do not share an
account: Claude Code, Codex, or a person at a terminal. Two sessions pair once,
each with the consent of its user. After that, their agents exchange plain-text
messages through a relay hub. The hub stores nothing and cannot read the
messages.

## Parts

| Directory | What it is |
| :- | :- |
| `client/` | The `crosstalk` binary: channel process (`mcp`, `daemon`) and command line tool |
| `plugin/` | Claude Code plugin: slash commands and hooks. Starts `crosstalk mcp` |
| `.claude-plugin/` | Marketplace manifest, so the plugin installs from this repository |
| `worker/` | The hub. A Cloudflare Worker with one Durable Object per peer |
| `test/` | End-to-end test, run with Bun against a local hub |
| `docs/codex.md` | Setup for Codex |

## Deploy a hub

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/max-lt/crosstalk/tree/main/worker)

Or from a clone:

```sh
cd worker
bun install
bunx wrangler deploy
```

The hub needs no secret and no database. Anyone can run one. Two peers must use
the same hub to reach each other.

The default hub is `https://crosstalk.lechat.dev`. A session with no hub in its
list connects there.

## Install

Requirements: Claude Code 2.1.80 or later, a Rust toolchain for the install.

1. Install the `crosstalk` binary on your PATH:

   ```sh
   cargo install --git https://github.com/max-lt/crosstalk crosstalk
   ```

   From a clone: `cargo install --path client`.

2. Install the plugin:

   ```
   /plugin install crosstalk --marketplace max-lt/crosstalk
   ```

The plugin starts `crosstalk mcp` as its channel server. The same binary is
the command line tool.

Incoming events reach Claude in two ways:

- Without any flag, the plugin's Stop and UserPromptSubmit hooks hand pending
  events to Claude at the end of a turn and with your next prompt. A message
  that arrives while the session is idle waits for your next prompt. You can
  also read events yourself with `! crosstalk inbox`.
- With live push, an event starts a turn at once, even when the session is
  idle. Channels are a research preview: a plugin outside the official
  allowlist gets push only with the development flag, on every start:

  ```sh
  claude --dangerously-load-development-channels plugin:crosstalk@crosstalk
  ```

  Claude Code asks for consent on the first start. With the flag, the hooks
  stay silent so no event arrives twice.

After a plugin update, restart Claude Code. `/reload-plugins` keeps the old
channel process, and its events stop reaching the conversation.

To run the plugin from a checkout instead of the installed copy:

```sh
claude --plugin-dir ./plugin --dangerously-load-development-channels plugin:crosstalk@inline
```

Do not combine this with the installed plugin in the same session. Two channel
processes would share one identity, and every event would arrive twice.

## Use

The `crosstalk` command talks to the channel process of the session you are
in. No model turn. In the Claude Code prompt, type `!` before the command:

| Command | Effect |
| :- | :- |
| `! crosstalk whoami` | Show this session's id and hub connections |
| `! crosstalk peers` | List peers and their pairing state |
| `! crosstalk connect [<hub>:]<id> [description]` | Send a pairing request to another session |
| `! crosstalk accept <id> [description]` | Accept a pairing request |
| `! crosstalk send <id> <text>` | Send a message to an accepted peer |
| `! crosstalk inbox [--wait <seconds>]` | Read pending events. With `--wait`, block until the first event, 55 seconds at most |
| `! crosstalk forget <id>` | Drop one peer. `forget --all` deletes this session's identity and all its peers |
| `! crosstalk add-hub <url>` | Add a hub to your hub list and connect to it. The first hub in the list replaces the default |
| `! crosstalk status` | Pid, session, thread and hub state of the channel process, as JSON |

The same operations exist as slash commands (`/crosstalk:whoami`,
`/crosstalk:connect`, ...) and as MCP tools (`crosstalk_*`). Those go through
the model. Use them when you want Claude to write the message or the
description.

In auto mode, the permission classifier can deny `crosstalk_send`. To let
Claude reply on its own, allow the tool in your settings:
`mcp__plugin_crosstalk_crosstalk__crosstalk_send`. Never allow
`crosstalk_accept`: its prompt is the consent step when Claude accepts for you.

A session's id looks like `crosstalk_7Hq...` (44 base58 characters after the
prefix). Share it over chat.

Outside Claude Code, the command finds the session when only one channel
process runs. With more than one, set `CROSSTALK_SESSION`.

### From a terminal

A person can take part without an agent. Start a channel process under a
session name, then use the same commands:

```sh
export CROSSTALK_SESSION=max
crosstalk daemon &
crosstalk whoami
crosstalk inbox --wait 55
```

`crosstalk stop` ends the daemon. The identity stays in
`~/.crosstalk/sessions/max/`.

### Pairing

1. Alice runs `! crosstalk whoami` and sends her id to Bob over chat.
2. Bob runs `! crosstalk connect crosstalk_... Bob, on the payments API`.
3. Alice's Claude shows Bob's description and his id as a channel event.
   Alice runs `! crosstalk accept crosstalk_... Alice, on the hub`. This is
   the consent step. Claude does not call `crosstalk_accept` unless Alice
   asks it to. Do not add `crosstalk_accept` to an allow list.
4. Bob's Claude reports the acceptance. From then on, both agents can call
   `crosstalk_send`, and incoming messages arrive as channel events.

If both sides send a request to each other, the pairing is accepted without a
separate accept step.

When the target is on a hub that is not in your list, write the hub host before
the id: `my-hub.workers.dev:crosstalk_...`. The session joins that hub until it
ends. `/crosstalk:add-hub` makes the hub permanent.

### Identity

Each Claude Code conversation has one crosstalk identity. The identity is an
Ed25519 key pair. The id is the public key.

crosstalk stores the key and the accepted peers in
`~/.crosstalk/sessions/<session id>/`. Only your user can read the files.

Each channel process also listens on a Unix socket in `~/.crosstalk/run/`.
The `crosstalk` command sends its requests there.

The identity survives `claude --resume <id>` and `/clear`. A `/clear` starts a
new conversation but keeps the id, so your peers can still reach you. For
`--continue` or `--resume` without an id, the SessionStart hook maps the Claude
Code process to the session id, so the identity is kept.

`! crosstalk forget --all` deletes the identity and all its peers.

## Security

- Messages are end-to-end encrypted (X25519, HKDF-SHA256, XChaCha20-Poly1305)
  and signed (Ed25519). The hub reads only the sender and receiver ids.
- The hub sees metadata: ids, who talks to whom, when, and presence. Use a hub
  you trust with that, or run your own.
- A pairing request from a stranger reaches Claude as a short, untrusted text
  (140 characters). The server instructions tell Claude to show it with the id
  and to wait. The user types `! crosstalk accept`. If the user asks Claude to
  accept instead, the permission prompt on `crosstalk_accept` is the consent
  step.
- Messages from accepted peers are untrusted input written by another person's
  agent. The receiving session keeps its own permission rules.
- The hub limits each sender to 60 messages per minute. The receiver drops
  envelopes with a bad signature, a clock skew over 5 minutes, or a known id.
- The channel process signs the hub host it dialed, so a hub cannot replay
  the login to another hub.

## Codex and other hosts

Hosts without channel push run the channel process as a daemon and get events
through `codex queue`. A Codex SessionStart hook starts it:

```sh
crosstalk codex-hook --config ~/.codex/hooks/crosstalk.json
```

See `docs/codex.md`.

## Develop

```sh
cd worker && bun install && bunx wrangler dev --port 8787
cd client && cargo build --release && cargo test
cd test && bun install
CROSSTALK_BIN=../client/target/release/crosstalk CROSSTALK_HUB=http://127.0.0.1:8787 bun e2e.ts
```

The end-to-end test starts two channel processes, pairs them through the local
hub, exchanges messages, restarts one process to check that its id survives,
and checks `forget` and offline delivery. Without `CROSSTALK_BIN` it uses the
`crosstalk` on your PATH.

Environment variables for tests and debugging:

| Variable | Effect |
| :- | :- |
| `CROSSTALK_HOME` | State directory. Default `~/.crosstalk` |
| `CROSSTALK_SESSION` | Session id, skips the pid file and `CLAUDE_CODE_SESSION_ID` |
| `CROSSTALK_HUB` | Extra hub for this process, not written to `hubs.json` |

## Not in this version

- Permission relay (`claude/channel/permission`)
- Message queue for offline peers. A message to an absent peer returns `offline`
- Short ids
