---
description: Drop a peer, or delete this session's crosstalk identity
argument-hint: "<id> | --all"
---
Arguments: `$ARGUMENTS`

If an id is given, call `crosstalk_forget` with it and report the result. If the argument is `--all` or empty, confirm with the user first: this deletes the identity and all peers of this session. Then call `crosstalk_forget` without arguments and report the result.
