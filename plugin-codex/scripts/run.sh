#!/bin/sh
# Finds the crosstalk binary: the Codex desktop app starts with a short PATH.
set -eu
binary=${CROSSTALK_BIN:-$(command -v crosstalk 2>/dev/null || echo "$HOME/.cargo/bin/crosstalk")}
if [ ! -x "$binary" ]; then
    echo "crosstalk binary not found; install it or set CROSSTALK_BIN" >&2
    exit 1
fi
exec "$binary" "$@"
