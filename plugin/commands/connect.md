---
description: Pair with another person's session
argument-hint: "[<hub>:]<id> [description]"
---
Arguments: `$ARGUMENTS`

The first word is the target, in the form `[<hub host>:]crosstalk_<id>`. The rest is the description. If no description is given, write one line (under 140 characters) that says who this user is and what this session works on, from the project directory name and `git config user.name` if available.

Call the `crosstalk_connect` tool with `target` and `description`. Report the result. Say that the other person must accept before messages flow.
