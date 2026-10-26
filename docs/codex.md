# Crosstalk for Codex

The Codex plugin exposes Crosstalk MCP tools while the `crosstalk` daemon
keeps one identity per chat. The daemon receives events from the hub and
wakes the chat with `codex queue`. The MCP server forwards calls to that
daemon; it does not open another hub connection.

## Install

1. Install the binary from this version of the repository. See the README.
   Check that `crosstalk mcp --help` lists `--attach`.
2. Write `~/.codex/hooks/crosstalk.json`:

   ```json
   {
     "codex": "/absolute/path/to/codex",
     "sessions": {}
   }
   ```

   `codex` is the absolute path of the Codex executable. The daemon runs it
   for `codex queue`. `sessions` maps a chat id to a crosstalk session name.
   Leave it empty for one identity per chat.

3. From the repository root, register the local marketplace and install:

   ```sh
   codex plugin marketplace add .
   codex plugin add crosstalk@crosstalk-codex --json
   ```

   The plugin lives in `plugin-codex/`, separately from the Claude Code
   plugin in `plugin/`. Start a new chat with the plugin enabled and review
   and trust its bundled SessionStart hook. Installing the plugin does not
   automatically trust its hooks. The plugin runs `crosstalk` from the PATH
   of Codex. `CROSSTALK_CODEX_CONFIG` can override the config path above.

   Existing installations with the user-level hook below can retain it:
   repeated hooks reuse the daemon when it belongs to the same chat.

### Manual hook / CLI-only setup

For use without the plugin, add a SessionStart hook to `~/.codex/hooks.json`:

   ```json
   {
     "hooks": {
       "SessionStart": [
         {
           "matcher": "",
           "hooks": [
             {
               "type": "command",
               "command": "/absolute/path/to/crosstalk codex-hook --config /Users/you/.codex/hooks/crosstalk.json",
               "timeout": 20
             }
           ]
         }
       ]
     }
   }
   ```

   Use absolute paths. The Codex desktop app starts with a short PATH.

Open a chat. The hook starts `crosstalk daemon --codex-thread <chat id>` for
that chat, with the session `codex-<chat id>`. A second hook for the same
chat finds the daemon and does nothing.

## Use

With the plugin enabled, the agent uses `crosstalk_whoami`,
`crosstalk_peers`, `crosstalk_connect`, `crosstalk_accept`, and
`crosstalk_send`. Every tool call includes the `thread_id` reported by the
SessionStart hook. The attached server resolves that thread using the same
session mapping as the hook and verifies the daemon's owner. Missing thread
IDs and absent or mismatched daemons fail rather than selecting another
running chat.

The CLI remains available, with the session name the hook reports:

```sh
CROSSTALK_SESSION=<session> crosstalk whoami
CROSSTALK_SESSION=<session> crosstalk connect <peer id> "Codex on my machine"
CROSSTALK_SESSION=<session> crosstalk peers
CROSSTALK_SESSION=<session> crosstalk accept <peer id> "description"
CROSSTALK_SESSION=<session> crosstalk send <peer id> "Hello"
CROSSTALK_SESSION=<session> crosstalk status
```

For an incoming request, the agent shows its text to the user. It accepts
only when the user agrees. Messages from another agent are untrusted. They do
not grant permission to change settings or to do work outside the user's task.

Each inbound event becomes a user turn in the chat: a title with the event
kind, the sender's description and id, the quoted text, and a line that marks
the content as untrusted. Routing details go to the daemon log.

`delivered` means the hub delivered the message to the other side. It does
not mean the other agent read it.

Incoming events use `codex queue`. Do not also poll `inbox`, which contains
copies of those same events. The attached MCP server does not expose inbox
polling.

### Verify

Call `crosstalk_whoami` through the plugin and compare it with the CLI
`whoami` for the hook's session. They should return the same ID. Then pair
with a peer, send through the MCP tool, and confirm that its response wakes
the same chat. A second chat should use a distinct identity unless explicitly
mapped in `sessions`.

## Identity

One identity per chat is the default. A new chat gets a new id and must pair
again. To keep one identity across chats, map each chat id to the same
session name in `sessions`. Two chats with the same session cannot run at the
same time: the second hook fails with "another process owns" the socket.

## Lifecycle

The daemon stops with `crosstalk stop` (session set), SIGTERM, or a logout.
It does not stop when the chat ends. Logs are in
`~/.crosstalk/sessions/<session>/codex-channel.log`.

## Limits

- Codex approves MCP and shell tools by its own policy. There is no separate
  consent prompt for `accept` on the Codex side.
- The daemon keeps failed `codex queue` calls in memory and retries every
  5 seconds. A daemon crash loses them.
