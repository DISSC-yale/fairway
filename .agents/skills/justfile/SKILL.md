---
name: justfile
description: Provides Justfile syntax reference for task automation. TRIGGER when writing, editing, debugging Justfiles or migrating from Makefiles.
---

## Boilerplate

```just
# Every Justfile should start with this
set dotenv-load

default:
    @just --list
```

## Variables

```just
var := "value"
dir := justfile_directory()           # directory containing Justfile
home := env_var("HOME")               # required env var (fails if missing)
opt := env_var_or_default("VAR", "")  # optional with default
```

## Recipes

```just
# Comment becomes help text for `just --list`
recipe-name:
    command here

# Parameters (positional)
greet NAME:
    echo "Hello, {{NAME}}"

# Default parameter value
build TARGET="debug":
    cargo build --{{TARGET}}

# Variadic parameters
test +FILES:                          # + requires one or more args
    pytest {{FILES}}

run *ARGS:                            # * allows zero or more args
    python main.py {{ARGS}}
```

## Settings

```just
set positional-arguments              # pass args as $1, $2 to shell
set shell := ["bash", "-euo", "pipefail", "-c"]
```

## Shebang recipes (multi-line scripts)

```just
deploy ENV:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{ENV}}" = "prod" ]; then
        echo "Deploying to production"
    fi
    ./scripts/deploy.sh "{{ENV}}"
```

## Common patterns

```just
# @ suppresses command echo
clean:
    @rm -rf ./tmp

# Recipe dependencies
build: clean
    cargo build

# Private recipe (hidden from --list)
_helper:
    echo "internal"
```

## Reference

- Manual: https://just.systems/man/en/
- README: https://raw.githubusercontent.com/casey/just/master/README.md
- Grammar: https://raw.githubusercontent.com/casey/just/master/GRAMMAR.md
