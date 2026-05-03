#!/bin/bash
# block-github-webfetch.sh — PreToolUse hook for WebFetch
#
# Blocks WebFetch calls to github.com URLs and directs Claude
# to use the gh CLI instead.

set -euo pipefail

INPUT=$(cat)
URL=$(echo "$INPUT" | jq -r '.tool_input.url // empty')

[ -z "$URL" ] && exit 0

if echo "$URL" | grep -qE '^https?://(www\.)?github\.com/'; then
  echo "Do not use WebFetch for GitHub URLs. Use the gh CLI instead (e.g., gh repo view, gh pr view, gh issue view, gh api)." >&2
  exit 2
fi

exit 0
