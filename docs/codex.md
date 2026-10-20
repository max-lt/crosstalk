# Crosstalk for Codex

Codex has no channel push. The `crosstalk` binary runs as a daemon next to a
Codex chat. It receives events from the hub and wakes the chat with
`codex queue`. The Codex agent sends with the same binary.

## Install

1. Install the binary. See the README.
2. Write `~/.codex/hooks/crosstalk.json`:

   ```json
   {
     "home": "/Users/you/.crosstalk",
     "codex": "/absolute/path/to/codex",
     "sessions": {}
   }
   ```

   `codex` is the absolute path of the Codex executable. The daemon runs it
   for `codex queue`. `sessions` maps a chat id to a crosstalk session name.
   Leave it empty for one identity per chat.

3. Add a SessionStart hook to `~/.codex/hooks.json`:

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

The agent runs the binary from its shell, with the session name the hook
reports in its context:

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

Each inbound event becomes a user turn in the chat. The turn starts with a
line that marks the content as untrusted, then a JSON object with `kind`,
`from`, `hub` and `text`.

`delivered` means the hub delivered the message to the other side. It does
not mean the other agent read it.

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
