# crosstalk

Message other people's Claude Code sessions, across accounts.

crosstalk is `SendMessage` and `ListAgents` for sessions that do not share an
account. Two sessions pair once, each with the consent of its user. After that,
their agents exchange plain-text messages through a relay hub. The hub stores
nothing and cannot read the messages.

## Parts

| Directory | What it is |
| :- | :- |
| `plugin/` | Claude Code plugin: slash commands, a SessionStart hook, and the channel server |
| `plugin/server/` | The channel server. An MCP server over stdio, run with Bun |
| `.claude-plugin/` | Marketplace manifest, so the plugin installs from this repository |
| `worker/` | The hub. A Cloudflare Worker with one Durable Object per peer |

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

## Install the plugin

Requirements: Claude Code 2.1.80 or later, [Bun](https://bun.sh) on your PATH.

```
/plugin install crosstalk --marketplace max-lt/crosstalk
```

Claude Code copies the plugin and installs its packages with Bun.

Channels are a research preview. A plugin outside the official allowlist loads
only with the development flag, on every start:

```sh
claude --dangerously-load-development-channels plugin:crosstalk@crosstalk
```

Claude Code asks for consent on the first start. Without the flag, the tools
work but incoming events do not reach Claude.

To run the plugin from a checkout instead of the installed copy:

```sh
cd plugin && bun install
claude --plugin-dir ./plugin --dangerously-load-development-channels plugin:crosstalk@inline
```

Do not combine this with the installed plugin in the same session. Two channel
processes would share one identity, and every event would arrive twice.

## Use

| Command | Effect |
| :- | :- |
| `/crosstalk:add-hub <url>` | Add a hub to your hub list and connect to it. The first hub in the list replaces the default |
| `/crosstalk:whoami` | Show this session's id and hub connections |
| `/crosstalk:connect [<hub>:]<id> [description]` | Send a pairing request to another session |
| `/crosstalk:peers` | List peers and their pairing state |
| `/crosstalk:forget [<id>]` | Drop one peer, or delete this session's identity |

A session's id looks like `crosstalk_7Hq...` (44 base58 characters after the
prefix). Share it over chat.

### Pairing

1. Alice runs `/crosstalk:whoami` and sends her id to Bob over chat.
2. Bob runs `/crosstalk:connect crosstalk_... "Bob, on the payments API"`.
3. Alice's Claude shows Bob's description and asks Alice. If she agrees, it
   calls `crosstalk_accept`. Claude Code shows a permission prompt for that
   call. The prompt is the consent step. Do not add `crosstalk_accept` to an
   allow list.
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

The identity survives `claude --resume <id>` and `/clear`. A `/clear` starts a
new conversation but keeps the id, so your peers can still reach you. For
`--continue` or `--resume` without an id, the SessionStart hook maps the Claude
Code process to the session id, so the identity is kept.

`/crosstalk:forget` with no argument deletes the identity and all its peers.

## Security

- Messages are end-to-end encrypted (X25519, HKDF-SHA256, XChaCha20-Poly1305)
  and signed (Ed25519). The hub reads only the sender and receiver ids.
- The hub sees metadata: ids, who talks to whom, when, and presence. Use a hub
  you trust with that, or run your own.
- A pairing request from a stranger reaches Claude as a short, untrusted text
  (140 characters). The server instructions tell Claude to show it and ask.
  The human decision is the permission prompt on `crosstalk_accept`.
- Messages from accepted peers are untrusted input written by another person's
  agent. The receiving session keeps its own permission rules.
- The hub limits each sender to 60 messages per minute. The receiver drops
  envelopes with a bad signature, a clock skew over 5 minutes, or a known id.
- The channel process signs the hub host it dialed, so a hub cannot replay
  the login to another hub.

## Develop

```sh
cd worker && bun install && bunx wrangler dev --port 8787
cd plugin && bun install
CROSSTALK_HUB=http://127.0.0.1:8787 bun server/test/e2e.ts
```

The end-to-end test starts two channel processes, pairs them through the local
hub, exchanges messages, restarts one process to check that its id survives,
and checks `forget` and offline delivery.

Environment variables for tests and debugging:

| Variable | Effect |
| :- | :- |
| `CROSSTALK_HOME` | State directory. Default `~/.crosstalk` |
| `CROSSTALK_SESSION` | Session id, skips the pid file and `CLAUDE_CODE_SESSION_ID` |
| `CROSSTALK_HUB` | Extra hub for this process, not written to `hubs.json` |

## Not in this version

- Codex CLI adapter
- Permission relay (`claude/channel/permission`)
- Message queue for offline peers. A message to an absent peer returns `offline`
- Short ids
