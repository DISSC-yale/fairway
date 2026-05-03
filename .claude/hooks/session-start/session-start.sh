#!/bin/bash
# session-start.sh — SessionStart hook
#
# Injects a reminder into Claude's context at the start of every session.
# For SessionStart hooks, stdout text is added to Claude's context.

cat <<'EOF'
REQUIRED: Before doing anything else, read the Justfile and the README.md in the repo root. The Justfile is the authoritative reference for all project commands. Do not skip this step.
EOF
