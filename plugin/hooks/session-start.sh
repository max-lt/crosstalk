#!/bin/sh
# Maps this Claude Code process to its session id, so the channel server finds
# the right identity even when --continue hands it a fresh CLAUDE_CODE_SESSION_ID.
input=$(cat)
sid=$(printf '%s' "$input" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
[ -n "$sid" ] && [ -n "$CLAUDE_PID" ] || exit 0
home="${CROSSTALK_HOME:-$HOME/.crosstalk}"
mkdir -p "$home/pid" "$home/bin" || exit 0
printf '%s' "$sid" > "$home/pid/$CLAUDE_PID"
# Keep ~/.crosstalk/bin/crosstalk pointing at the current plugin copy.
[ -n "$CLAUDE_PLUGIN_ROOT" ] && ln -sfn "$CLAUDE_PLUGIN_ROOT/bin/crosstalk" "$home/bin/crosstalk"
find "$home/pid" -type f -mtime +1 -delete 2>/dev/null
exit 0
