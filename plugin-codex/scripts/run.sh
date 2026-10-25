#!/bin/sh
set -eu

# Desktop hosts can have a shorter PATH than an interactive shell.
if [ -n "${CROSSTALK_BIN:-}" ]; then
    binary=$CROSSTALK_BIN
elif command -v crosstalk >/dev/null 2>&1; then
    binary=$(command -v crosstalk)
else
    binary="$HOME/.cargo/bin/crosstalk"
fi

if [ ! -x "$binary" ]; then
    printf '%s\n' "Crosstalk executable not found. Install the binary or set CROSSTALK_BIN to its absolute path." >&2
    exit 1
fi

if [ "${1:-}" = "codex-hook" ]; then
    exec "$binary" codex-hook --config "${CROSSTALK_CODEX_CONFIG:-$HOME/.codex/hooks/crosstalk.json}"
fi

if [ "${1:-}" = "mcp" ] && [ "${2:-}" = "--attach" ]; then
    exec "$binary" mcp --attach --config "${CROSSTALK_CODEX_CONFIG:-$HOME/.codex/hooks/crosstalk.json}"
fi

exec "$binary" "$@"
