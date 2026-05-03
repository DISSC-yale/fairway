---
name: forgejo
description: Manages repositories, issues, PRs, releases, and CI on Forgejo via `fj`. TRIGGER when needing to create/edit issues, open PRs, view CI status, manage repos, or check releases. 
allowed-tools: Bash(fj *), Bash(curl *code.tailcddce6.ts.net*), Bash(*| jq*)
---

## Instance

The user's Forgejo instance is `code.tailcddce6.ts.net` (Tailscale tailnet).
Auth is already configured via `fj auth login`.

## STOP — `fj` is NOT `gh`. Read before running any command.

`fj` is a Rust CLI (`forgejo-cli`). It looks similar to `gh` but has **different syntax** for almost everything. Do NOT guess flags by analogy with `gh`. The errors below happen repeatedly — check this list before every `fj` invocation.

### Flags/commands that DO NOT EXIST

| What you might try (WRONG)           | What to do instead (RIGHT)                        |
|--------------------------------------|---------------------------------------------------|
| `fj issue create --title "X"`        | `fj issue create "X"` — title is positional       |
| `fj pr create --title "X"`           | `fj pr create "X"` — title is positional           |
| `fj issue list`                      | `fj issue search` — there is no `list` subcommand |
| `fj pr list`                         | `fj pr search` — there is no `list` subcommand    |
| `fj repo list`                       | No CLI command — use API (see below)               |
| `fj repo search`                     | No CLI command — use API (see below)               |
| `fj auth status`                     | `fj auth list`                                     |
| `fj issue search --limit N`          | No `--limit` flag — results are not paginated in CLI |
| `fj issue view 42 --comments`        | `fj issue view 42 comments` — subcommand, not flag |
| `fj issue comment 42 -b "text"`      | `fj issue comment 42 "text"` — body is positional  |
| `fj issue comment 42 --body "text"`  | `fj issue comment 42 "text"` — body is positional  |
| `fj issue comment list 5`            | `fj issue view 5 comments` — comments are under `view` |
| `fj issue edit 42 --state closed`    | `fj issue close 42` — no `--state` flag on edit   |
| `fj issue view 42 --json`            | `fj issue view 42 --style minimal`                |
| `fj api repos/...`                   | No `fj api` — use `curl` with `$FJ_TOKEN` (see below) |

### `-r` (repo) vs `-R` (remote) — they are different flags!

`-R/--remote` means a **local git remote name** (e.g. `origin`). `-r/--repo` means a **Forgejo repo slug** (e.g. `kljensen/my-repo`). Not all subcommands accept both:

| Subcommand        | `-r/--repo` | `-R/--remote` |
|-------------------|:-----------:|:-------------:|
| `issue create`    | yes         | yes           |
| `issue search`    | yes         | yes           |
| `issue view`      | **NO**      | yes           |
| `issue edit`      | **NO**      | yes           |
| `issue close`     | **NO**      | yes           |
| `issue comment`   | **NO**      | yes           |
| `pr create`       | yes         | **NO**        |
| `pr search`       | yes         | yes           |
| `pr view`         | **NO**      | yes           |
| `repo view`       | **NO**      | `-R` only     |
| `release *`       | yes         | yes           |
| `actions *`       | yes         | yes           |

**Inside a repo with a Forgejo remote:** Use `-R origin` (most common case).
**Outside a repo or targeting a different repo:** Use `-r owner/repo` on commands that support it, or `-H code.tailcddce6.ts.net` as a global flag.

### Flag ordering matters — global flags go BEFORE the subcommand

`-H/--host` and `--style` are **global** flags (they belong to `fj` itself).
`-r/--repo` and `-R/--remote` are **subcommand** flags (they belong to `issue`, `release`, etc).

```bash
# WRONG — -r is not a global flag, so fj rejects it before reaching `release`
fj -H code.tailcddce6.ts.net -r kljensen/repo release list

# RIGHT — global flags before subcommand, subcommand flags after
fj -H code.tailcddce6.ts.net release list -r kljensen/repo
fj --host code.tailcddce6.ts.net release list --repo kljensen/repo
```

General pattern: `fj [GLOBAL_FLAGS] <command> <subcommand> [SUBCOMMAND_FLAGS] [ARGS]`

### Issue body: use `--body` flag or `--body-file`, never `-b`

```bash
# Inline body
fj issue create "Title" --body "Description text"

# Long body from a file (preferred for multi-line)
fj issue create "Title" --body-file ./description.md

# For comments: body is POSITIONAL (second arg), not a flag
fj issue comment 42 "Comment text"
fj issue comment 42 --body-file ./comment.md
```

### Issue edit: subcommand-based, not flag-based

```bash
# Edit title
fj issue edit 42 title "New title"

# Edit body (pass new body as arg, or omit to open editor)
fj issue edit 42 body "New body text"

# Edit a specific comment on an issue (IDX is the comment index, 1-based)
fj issue edit 42 comment 3 "Updated comment text"

# To change state: use close, not edit
fj issue close 42
```

Note: `fj issue edit <ISSUE> comment <IDX>` uses the comment's **index**
(position in the comment list), not an API comment ID. To get comment IDs
and metadata (timestamps, authors), use the API:

```bash
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/issues/42/comments" | \
  jq '[.[] | {id, body: (.body[:80]), created_at, user: .user.login}]'
```

## Quick self-help

`fj` has NO tldr pages and NO man pages. To look up flags:

- `fj <command> -h` for short help
- `fj <command> --help` for detailed help with descriptions
- `fj <parent> help` to list subcommands (e.g. `fj repo help`)

Always run `-h` before guessing at flags you're unsure about.

## Host detection

`fj` resolves the host in this priority order:
1. `-H code.tailcddce6.ts.net` flag (always wins)
2. Git remote of current repo (if inside a git repo)
3. `FJ_FALLBACK_HOST` env var (last resort, only when no git remote)

**Important:** When working inside a repo whose git remote points to GitHub (or another non-Forgejo host), `fj` will try to use that host and fail. In that case, always pass `-H code.tailcddce6.ts.net` or use `-r owner/repo` to target the Forgejo instance explicitly.

## Output style

Use `--style minimal` when piping or parsing output programmatically.
There is no `--json` flag.

## Git remote URLs

**Always use SSH remotes.** HTTPS remotes will fail in non-interactive terminals
(no credential helper configured).

```
git@code.tailcddce6.ts.net:kljensen/repo.git
```

The full `ssh://` form also works but the SCP-like shorthand above is preferred:
```
ssh://git@code.tailcddce6.ts.net/kljensen/repo.git
```

## Common workflow: New local repo → Forgejo

When creating a brand-new repo and pushing it to Forgejo:

```bash
# 1. Initialize repo locally
mkdir -p ~/src/code.tailcddce6.ts.net/kljensen/my-repo
cd ~/src/code.tailcddce6.ts.net/kljensen/my-repo
git init && git add . && git commit -m "Initial commit"

# 2. Create Forgejo repo (pass -H since no Forgejo remote exists yet)
fj -H code.tailcddce6.ts.net repo create my-repo -d "Description"

# 3. Add SSH remote and push (do NOT use --push/--remote, see caveat below)
git remote add origin git@code.tailcddce6.ts.net:kljensen/my-repo.git
git push -u origin main
```

## Common workflow: GitHub clone → Forgejo repo

When cloning a GitHub repo to re-host on Forgejo:

```bash
# 1. Clone from GitHub
git clone https://github.com/user/repo /path/to/local-name

# 2. Rename GitHub remote
cd /path/to/local-name
git remote rename origin upstream

# 3. Create Forgejo repo (must pass -H since no Forgejo remote yet)
fj -H code.tailcddce6.ts.net repo create local-name --description "..." --private

# 4. Add Forgejo remote and push
git remote add origin git@code.tailcddce6.ts.net:kljensen/local-name.git
git push -u origin main
```

**Do NOT use `fj repo create --push`** in this workflow — it will fail because `origin`
already exists (pointing to GitHub). Rename first, then add the Forgejo remote manually.

## Caveat: `fj repo create --push` / `--remote` uses HTTPS

`fj repo create --remote origin --push` adds an HTTPS remote URL and tries to push over
HTTPS. This **will fail** in non-interactive terminals (like Claude Code) because there is
no credential helper configured for HTTPS — you get `could not read Username` errors.

**Always add the remote manually with an SSH URL and push separately:**
```bash
fj -H code.tailcddce6.ts.net repo create my-repo -d "Description"
git remote add origin git@code.tailcddce6.ts.net:kljensen/my-repo.git
git push -u origin main
```

If `fj` already added an HTTPS remote (the repo gets created server-side even when push
fails), fix it:
```bash
git remote set-url origin git@code.tailcddce6.ts.net:kljensen/my-repo.git
git push -u origin main
```

## Core commands (80/20)

### Repos

```bash
# Create repo (in current directory context)
fj repo create my-repo --description "Description" --private
# Do NOT use --push or --remote flags (they use HTTPS, which fails non-interactively)
# Instead, add SSH remote and push manually (see workflows above)

# View repo info
fj repo view owner/repo

# Clone
fj repo clone owner/repo [local-path]

# Fork
fj repo fork owner/repo --name my-fork

# Delete (destructive — confirm with user first)
fj repo delete owner/repo

# Migrate from GitHub/GitLab
fj repo migrate https://github.com/user/repo new-name --service github --include all

# NOTE: There is NO `fj repo list` or `fj repo search`.
# To list repos, use the API:
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/search?q=QUERY&limit=50" | jq .
```

### Issues

```bash
# Create issue — title is POSITIONAL, not --title
fj issue create "Title" --body "Description"
fj issue create "Title" --body-file ./description.md
fj issue create "Title" --body "Description" -r owner/repo
fj issue create "Title" --body "Description" -R origin
# Use --no-template if the repo requires templates but you want a blank issue

# Search issues (there is NO `fj issue list`)
fj issue search                          # list open issues
fj issue search -R origin               # in repo pointed to by origin remote
fj issue search "query" --state all      # search all states
fj issue search --labels "bug"           # filter by label
fj issue search --assignee kljensen

# View issue
fj issue view 42                   # title + body (default)
fj issue view 42 comments          # all comments (subcommand, NOT --comments)
fj issue view 42 comment 3         # view specific comment

# Comment on issue — body is POSITIONAL (second arg)
fj issue comment 42 "Comment text"
fj issue comment 42 --body-file ./comment.md

# Edit issue — uses subcommands (title, body), NOT flags
fj issue edit 42 title "New title"
fj issue edit 42 body "New body text"

# Close issue (there is NO `fj issue edit --state closed`)
fj issue close 42
fj issue close 42 --with-msg "Fixed in commit abc"
```

### Pull Requests

```bash
# Create PR (push branch first!) — title is POSITIONAL, not --title
fj pr create "Title" --body "Description" --base main
# --autofill (-A): populate title/body from commits
# --agit (-a): AGit workflow (no fork needed, pushes directly)
# -aA: autofill + agit combined

# Search PRs (there is NO `fj pr list`)
fj pr search                       # list open PRs
fj pr search --state all

# View PR
fj pr view 42                      # summary
fj pr view 42 diff                 # the diff
fj pr view 42 comments             # comments
fj pr view 42 files                # changed files
fj pr view 42 commits              # commits in PR

# Check CI/merge status
fj pr status 42
fj pr status 42 --wait             # block until checks finish

# Checkout PR locally
fj pr checkout 42

# Comment on PR — body is POSITIONAL
fj pr comment 42 "Comment text"

# Edit PR
fj pr edit 42 title "New title"
fj pr edit 42 labels --add "review" --remove "wip"

# Merge (confirm with user first)
fj pr merge 42 --method rebase --delete
# methods: merge, rebase, rebase-merge, squash, manual

# Close without merging
fj pr close 42 --with-msg "Superseded by #43"
```

### Releases & Tags

```bash
# Create release
fj release create v1.0.0 --body "Release notes" --create-tag
# --attach file.tar.gz: include asset
# --draft, --prerelease

# List releases
fj release list

# View release details
fj release view v1.0.0

# Download release asset (use fj, not curl)
fj release asset download v1.0.0 asset-name -r owner/repo -o /tmp/output

# Download via API (when fj isn't available, e.g. in Ansible)
# 1. Get asset ID first:
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/releases/tags/v1.0.0" | \
  jq '.assets[] | {id, name, browser_download_url}'
# 2. Download by asset ID:
curl -s -H "Authorization: token $FJ_TOKEN" -o output-file \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/releases/assets/{asset_id}"

# Create tag
fj tag create v1.0.0 --body "Tag message"

# List tags
fj tag list
```

### Organizations

```bash
fj org list --only-member-of
fj org create my-org --description "Org description" --visibility private
```

### Auth

```bash
# Check auth — there is NO `fj auth status`
fj auth list          # list all instances you're logged into
fj whoami             # show current authenticated user

# Log in (opens browser)
fj auth login

# Add token manually (useful in containers/CI)
fj auth add-key <USER> [KEY]
```

### Actions (CI)

```bash
# Check CI status (best starting point)
fj actions tasks
# Output: #TASK_ID (COMMIT) STATUS JOB_NAME DURATION (EVENT): COMMIT_MESSAGE
# Example: #11 (76f4cf54cf) success check 47s (push): Fix unit tests
# Status values: success, failure, running, waiting, cancelled

# List runs via API (has run IDs, workflow file names)
curl -sk -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/actions/runs" | \
  jq '[.workflow_runs[] | {id, title, status, event, workflow_id}]'

# Trigger workflow manually
fj actions dispatch workflow.yaml main --inputs key=value

# Manage secrets and variables
fj actions variables list
fj actions secrets create MY_SECRET

# NOTE: Forgejo v13 has NO REST API for job logs. Use the forgejo-actions
# skill for getting log output and debugging failed CI runs.
```

## REST API fallback (when `fj` is not sufficient)

`fj` has no `fj api` equivalent. When you need something `fj` doesn't cover, use the
Forgejo REST API directly with `curl`. Auth token is in `$FJ_TOKEN`.

**`$FJ_TOKEN` is valid and configured.** Do not assume it is stale or wrong. If an API
call returns 404, the URL is wrong — not the token. Verify the endpoint path first.

**Auth header format:** `Authorization: token $FJ_TOKEN` — NOT `Bearer`. Forgejo uses
its own `token` scheme. `Bearer` will return 401/405.

**Pagination:** Most list endpoints return 20 items by default. Use `?page=1&limit=50` query params. Check the `x-total-count` response header for total items.

```bash
# Base pattern — note "token", NOT "Bearer"
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/ENDPOINT" | jq .

# POST/PATCH/DELETE — add Content-Type and method
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}' \
  "https://code.tailcddce6.ts.net/api/v1/ENDPOINT"
```

### Common API operations not in `fj`

```bash
# List repos (NO `fj repo list` exists)
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/search?limit=50" | jq '.[].full_name'

# Labels — create, list, add to issue
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/labels" | jq .
curl -s -X POST -H "Authorization: token $FJ_TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"bug","color":"#ee0701"}' \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/labels"
curl -s -X POST -H "Authorization: token $FJ_TOKEN" -H "Content-Type: application/json" \
  -d '{"labels":[1,2]}' \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/issues/{index}/labels"

# Milestones
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/milestones" | jq .

# File contents (read a file from a repo)
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/contents/{filepath}" | jq .

# Branches
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/branches" | jq .

# Collaborators
curl -s -X PUT -H "Authorization: token $FJ_TOKEN" -H "Content-Type: application/json" \
  -d '{"permission":"write"}' \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/collaborators/{username}"

# Webhooks
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/hooks" | jq .

# Topics
curl -s -X PUT -H "Authorization: token $FJ_TOKEN" -H "Content-Type: application/json" \
  -d '{"topics":["go","cli"]}' \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/topics"

# Notifications
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/notifications" | jq .
```

### Discovering API endpoints

Swagger UI (browse in browser): `https://code.tailcddce6.ts.net/api/swagger`

Note: The Swagger JSON spec is not easily accessible via curl (the UI endpoint returns HTML). Use the Swagger UI in a browser to explore endpoints, or check the Forgejo API docs at `https://forgejo.org/docs/latest/developer/api-usage/`.

## IMPORTANT: Do not use `gh` for Forgejo

`gh` (GitHub CLI) does NOT work with Forgejo. Always use `fj` for Forgejo operations. If inside a repo with a GitHub remote, `fj` needs `-H code.tailcddce6.ts.net` or `-r owner/repo` to target Forgejo.
