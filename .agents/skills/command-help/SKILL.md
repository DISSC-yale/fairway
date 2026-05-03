---
name: command-help
description: Look up CLI command syntax and examples. TRIGGER when encountering unfamiliar commands, checking flags, verifying options, or doubting command syntax.
allowed-tools: Bash(tldr *), Bash(man *)
argument-hint: <command>
---

## Usage

1. `tldr <command>` - concise examples (try first)
2. `man <command> | head -80` - full reference if needed

If tldr returns "Page not found":
```
tldr --update && tldr <command>
```

tldr pages are community-maintained and more current than training data.
