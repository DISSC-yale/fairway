# Fairway Rewrite — Session Handoff

**Date:** 2026-05-01
**Source repo:** `/Users/mad265/git-pub/DISSC/fairway/`
**Branch:** `rewrite/v0.3`
**Plan:** `PLAN.md` v3.3 (Python-plugin transforms; ~2,500 LOC ceiling; 4 quality gates; 4-stage post-completion review)

This document is for resuming the rewrite in a new session (likely inside a container). Read this first; then read `PLAN.md`. Everything you need is here or referenced.

---

## 1 — Build state (as of handoff)

### Completed and committed (verified)

| Step | Commit | What landed | Cost | Wall time |
|---|---|---|---|---|
| 0 | `d137c46` | Branch + baseline metrics + ruff cleanup. `BASELINE.txt` written with named sections. ruff/mypy installed in `.venv`. mypy ceiling = 23 errors. | $2.54 | 6:13 |
| 0.5 | `7b15dce` | `--cov-fail-under=67` token-stripped from `pyproject.toml` via tomllib. | ~$0.50 | ~2:00 |
| 1 | `5dc39b1` | Deleted `pyspark_engine.py` + `slurm_cluster.py` + `engines/base.py`. Removed pyspark/delta-spark from optional-deps. Pruned ~30 import + call sites across 15 files. Test imports patched with `pytest.skip(...)`. mypy went 23 → 20 (good direction). | $10.34 | 15:00 |

**Cumulative:** ~$13.40, ~23 minutes dispatch time. Branch `rewrite/v0.3` has 3 step commits on top of `afc6545` (the `fix/full-remediation` baseline).

### Dispatched but unverified

| Step | Workdir | Status | Action needed |
|---|---|---|---|
| 2 | `/tmp/forge/fairway-build-step2-<ts>/` | Completed (exit 0 per Slurm-style notification) but **NOT verified by me before interrupt**. | **First action in new session: verify Step 2 outcome** (see §4 below). |

### Working tree state at handoff

Uncommitted (pre-existing rewrite-prep edits, expected — touched throughout the build via stash/restore):
- `M rules.md`
- `M src/fairway/hpc.py`
- `M src/fairway/slurm_templates/submit_with_spark.sh`

Untracked (all expected):
- `.agent-sync/`, `.agents/`, `.pi/`, `.venv/`, `.github/workflows/roborev.yml`
- `PLAN.md` (the rewrite plan — git-add this when committing the v0.3 work)
- `BASELINE.txt` (Step 0 metrics — already committed in step 0)
- `HANDOFF.md` (this file)
- `tests/test_cli_generate_schema.py`
- Conversation export: `2026-05-01-172624-git-pubmauriceagent-infrastructurerepos-to.txt`

---

## 2 — Artifacts to bring into the container

### Critical (the build cannot resume without these)

| Path on host | Purpose | Container destination |
|---|---|---|
| `/Users/mad265/git-pub/DISSC/fairway/` | Fairway repo (the work tree) | mount or clone to `${FAIRWAY_REPO}` |
| `/Users/mad265/git-pub/DISSC/fairway/PLAN.md` | The rewrite plan (v3.3) — source of truth for design decisions | inside repo at `${FAIRWAY_REPO}/PLAN.md` |
| `/Users/mad265/git-pub/DISSC/fairway/BASELINE.txt` | Step 0 metrics, mypy/ruff baselines, original addopts | inside repo at `${FAIRWAY_REPO}/BASELINE.txt` (committed in `d137c46`) |
| `/Users/mad265/git-pub/DISSC/fairway/HANDOFF.md` | This file | `${FAIRWAY_REPO}/HANDOFF.md` |
| `/Users/mad265/git-pub/maurice/agent-infrastructure/agent-config/` | Forge tooling (cli.ts, dispatch, debate primitives) | mount or clone to `${AGENT_CONFIG_REPO}` |

### Important (preserves design context across sessions)

| Path on host | Purpose | Container destination |
|---|---|---|
| `/Users/mad265/.claude/projects/-Users-mad265-git-pub-DISSC-fairway/memory/MEMORY.md` | Memory index | replicate at `~/.claude/projects/<container-project-key>/memory/MEMORY.md` |
| `/Users/mad265/.claude/projects/-Users-mad265-git-pub-DISSC-fairway/memory/rewrite_v3_plan.md` | Locked decisions, re-entry triggers, trajectory disclosure | replicate alongside MEMORY.md |
| `/Users/mad265/git-pub/DISSC/fairway/2026-05-01-172624-git-pubmauriceagent-infrastructurerepos-to.txt` | Full grilling-and-build conversation export — for human reference, not agent input | optional; useful if you need to reconstruct intent |

### Nice-to-have (per-step audit trail)

| Path on host | Purpose |
|---|---|
| `/tmp/forge/fairway-build-step0-*/run.log` | Step 0 dispatch transcript |
| `/tmp/forge/fairway-build-step0.5-*/run.log` | Step 0.5 dispatch transcript |
| `/tmp/forge/fairway-build-step1-*/run.log` | Step 1 dispatch transcript |
| `/tmp/forge/fairway-build-step2-*/run.log` | Step 2 dispatch transcript (the unverified one — read this first in the new session) |
| `/tmp/forge/fairway-plan-review-*/` | Three forge debate workdirs (v1, v2, v3 plan reviews — historical) |

These are ephemeral on `/tmp` — copy them to a stable location before container handoff if you want to keep them.

---

## 3 — Container prerequisites

### Tools required on PATH

```
bun         (>= 1.3) — runs forge cli.ts
claude      (Claude Code CLI; auth required)
pi          (>= 0.65) — though only used as runtime detection, dispatches go through bun directly
git
python      (>= 3.10) — for .venv and tooling
```

Codex and Gemini CLIs were skipped throughout (codex timed out repeatedly; gemini-pro hit 429s). **Don't bother installing them unless you want their reviews.**

### Authentication

- `claude` requires a `claude login` (or env-based auth, depending on container setup).
- Forge dispatch shells to `claude -p` for the coder agent (`anthropic/claude-opus-4-7`); auth must be ready before first dispatch.

### Python venv

The `.venv/` in the repo is host-specific (Mac paths). **Recreate inside the container:**

```bash
cd ${FAIRWAY_REPO}
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev,duckdb]
pip install ruff mypy            # Step 0 added these; the dependencies aren't pinned in pyproject yet
```

Verify mypy returns ~20 errors (Step 1 ceiling) and ruff exits 0 — that's the baseline subsequent steps inherit.

### Path substitutions

The forge dispatch command in §4 uses an absolute path for `cli.ts`. **Edit before first dispatch in container** to point at your container path:

```
${AGENT_CONFIG_REPO}/pi/extensions/forge/cli.ts
```

---

## 4 — How to resume execution

### Step A — Verify Step 2 outcome (FIRST THING)

Step 2 was dispatched at the time of handoff and completed with exit 0, but I did not verify the result before being interrupted. Run:

```bash
cd ${FAIRWAY_REPO}
git log --oneline -5
# Expect to see: rewrite/v0.3 step2: delete apptainer + container artifacts
# If absent, Step 2 did not commit cleanly — re-dispatch (see Step C).

# Verify the deletions:
ls src/fairway/apptainer.py src/fairway/data/Apptainer.def src/fairway/data/Dockerfile \
   src/fairway/data/spark.yaml Dockerfile.dev fairway-dev.def entrypoint.sh 2>&1 | head
# All seven should report "No such file or directory"

# Verify gates:
.venv/bin/python -c "import fairway, fairway.cli, fairway.pipeline" && echo "import: OK"
.venv/bin/pytest --collect-only -q 2>&1 | tail -1
.venv/bin/ruff check src/fairway/ --no-fix 2>&1 | tail -1   # should be "All checks passed!"
.venv/bin/mypy --ignore-missing-imports src/fairway/ 2>&1 | tail -1
# mypy should be ≤ 20 errors (Step 1 ceiling, since Step 2 is just deletions)

# Verify no leftover apptainer references:
grep -rn 'apptainer\|Apptainer\|singularity' src/ tests/ 2>/dev/null | grep -v ".egg-info"
# Expect 0 lines
```

If all these pass, Step 2 is verified — proceed to Step 3.

If any fail:
- Read the dispatch transcript: `/tmp/forge/fairway-build-step2-*/run.log` (if preserved) for the agent's report.
- Investigate what broke.
- Either fix manually + commit, or re-dispatch Step 2 with the same command template.

### Step B — Dispatch the next step

Use this exact pattern (substitute the step number, persona stays constant):

```bash
WORKDIR=/tmp/forge/fairway-build-step${STEP}-$(date +%s)
mkdir -p "$WORKDIR"

PERSONA='You are a senior Python engineer executing a bounded rewrite step from PLAN.md. You follow the plan exactly and do not improvise scope. If a precondition or predicate fails, you stop and report rather than working around it. You never push to remote, never modify main branch, never use destructive git commands (reset --hard, clean -fd, branch -D, checkout --), and never skip pre-commit hooks. You match commit message style from existing git log.'

bun ${AGENT_CONFIG_REPO}/pi/extensions/forge/cli.ts dispatch \
  anthropic/claude-opus-4-7 \
  "<TASK PROMPT — see queue below>" \
  --persona "$PERSONA" \
  --tools read,edit,write,bash \
  --workdir "$WORKDIR" 2>&1 | tee "$WORKDIR/run.log"
```

Each step's task prompt should:
1. Reference the section in PLAN.md.
2. State pre-conditions (branch, prior step committed, working tree state).
3. Stash pre-existing edits (`rules.md`, `hpc.py`, `submit_with_spark.sh`) at start, restore at end.
4. List the action sequence from PLAN.md.
5. Specify the implicit Done-when gates (importability + collect + ruff + mypy ≤ N).
6. Specify the commit message: `rewrite/v0.3 step${N}: <summary>`.
7. Constraints (no push, no `git add -A`, no `--no-verify`, etc.).
8. Output: STATUS line at end.

Reference templates: see the `run.log` files of completed steps for working examples of task prompts.

### Step C — Verify after each dispatch

After every step:

```bash
git log --oneline -1                                                     # confirm new commit
.venv/bin/python -c "import fairway, fairway.cli, fairway.pipeline"     # importability gate
.venv/bin/pytest --collect-only -q 2>&1 | tail -1                       # collect gate
.venv/bin/ruff check src/fairway/ --no-fix 2>&1 | tail -1               # ruff gate
.venv/bin/mypy --ignore-missing-imports src/fairway/ 2>&1 | tail -1     # mypy gate (must be ≤ ceiling)
```

The mypy ceiling tightens as the rewrite progresses. Track it in `BASELINE.txt`.

---

## 5 — Build queue (Steps 3 → 14)

| Step | Summary | Key concerns |
|---|---|---|
| 3 | Delete `schema_pipeline.py`, `validations/`, `enrichments/`, `transformations/` directories. | Largest deletion; many call sites in `pipeline.py` to prune. ~$10–15. |
| 4 | Cull tests for deleted modules + consolidate `test_cli_*` into one. | Grep-driven deletion list per PLAN.md (deterministic). |
| 5 | Rewrite `config_loader.py` (832 LOC) → `config.py` (~250 LOC). New fields: `storage_root`, `layer`, `scratch_dir`, `unzip`, `sub_partition_by`, `validations`, `slurm_*`, `apply_type_a`, `python` path, etc. Add `IngestCtx` + `TransformCtx` dataclasses. | Most architecturally consequential step. Plan it carefully. |
| 6 | Manifest fragment + finalize redesign. Per-shard `_fragments/<task_id>.json`, atomic write. `task_id = sha256(sorted partition kv)[:16]`. | Critical for Slurm-array correctness. Tests must include `test_manifest_corruption.py` (concurrency safety). |
| 7.1 | Skeleton: dataclasses + plugin loader (`transform_loader.py`) + empty `defaults` module + `validations.py` stub + `ShardResult`. | Don't delete `engines/duckdb_engine.py` yet (cutover in 7.4). |
| 7.2 | Build `fairway.defaults` (`default_ingest`, `apply_type_a`, `unzip_inputs`) + `fairway.validations.apply_validations` with full check types (min_rows, max_rows, check_nulls, expected_columns, check_range, check_values, check_pattern). | Tests for each check type. |
| 7.3 | `duckdb_runner.run_shard(con, ctx)` full pipeline integration: load plugin → call user `transform()` → apply Type A → inject partition cols → validate → sort → write parquet (with `partition_by` from filename + `sub_partition_by` from data) → fingerprint → fragment. Always exit 0; errors recorded in fragment. | Heart of the runner. Take care with the order. |
| 7.4 | Cutover: retarget `pipeline.py` to `duckdb_runner`, delete `engines/`. | LOC budget enforcement. |
| 8.1–8.3 | Trim `pipeline.py` (1419 → ~300 LOC) into orchestrator + manifest fragment writes. | Three substeps in PLAN.md. |
| 9.1 | CLI skeleton + simple subcommands. **`fairway init <project> --dataset <name>`** scaffolds `<project>/datasets/<name>.{yaml,py}` with the default-ingest one-liner. | The init scaffold is the entry point for new datasets. Get it right. |
| 9.2 | `fairway submit` core: render sbatch, invoke via `subprocess.run(["sbatch", ...])`, write submission record. `--dry-run` flag. | |
| 9.3 | `fairway submit` pre-scan validation (`naming_pattern` mismatch, `--allow-skip`). Modifies `batcher.py` to expose `expand_and_validate`. | |
| 9.4 | `fairway submit` idempotent resume + `--force`. Source hash = sha256(size + first 4 KB + last 4 KB). | |
| 9.5 | `fairway submit` chunking for large arrays (> `slurm_chunk_size`, default 4000). | Multiple sbatch scripts emitted, all submitted. |
| 9.6 | Trim `summarize.py` (103 → ≤60 LOC), DuckDB SQL only. | |
| 10 | Slurm template `slurm_templates/job_array.sh` (≤60 LOC). | `--array=0-{N-1}%{CONCURRENCY}` consistent syntax. |
| 11 | Delete `rules.md`, prune `docs/`. Update `README.md`. | |
| 11.5 | Re-anchor coverage gate to `max(60, M-5)` where M = post-rewrite measured coverage. | |
| 12 | Smoke test on synthetic fixture (<100 MB). Real-data validation is out-of-band human task. | |
| 13 | Final tidy. LOC ≤ 2500, tests ≤ 20 files, coverage ≥ Step 11.5 threshold. `BASELINE.txt` updated with `## After` section. | |
| 14 | When all gates pass, print `WIGGUM_DONE`. | Don't print it earlier. |

### Post-`WIGGUM_DONE` (4 quality gates before merge to `main`)

1. Wiggum predicates (covered automatically by `WIGGUM_DONE`).
2. **Human diff review** — read `git diff main..rewrite/v0.3 --stat`, then read every survivor file end-to-end.
3. **Multi-model code review debate** — see PLAN.md "Post-WIGGUM_DONE workflow" Gate 3 for the exact `bun cli.ts debate --review` command.
4. **Real-data scale validation** — ingest the 50 GB TSV via the new tool on Grace/Bouchet; confirm parity with the throwaway script.

---

## 6 — Locked design decisions (do not relitigate)

These are settled. A fresh agent reading PLAN.md will see them in the changelog/locked sections, but for fast context:

### Architecture
- **Single engine: DuckDB.** No PySpark. No Polars. No Dagster (Phase 2 trigger reserved). No Snakemake.
- **Cross-node parallelism: Slurm job arrays.** One DuckDB process per shard.
- **Storage: plain partitioned parquet.** No Delta Lake. No Iceberg.
- **Medallion architecture enforced** in path layout: `<storage_root>/{raw,processed,curated}/<dataset>/__state=X/__date=Y/...`.
- **Schema drift handled losslessly** via `all_varchar=true` reads + per-shard schema fingerprints + `union_by_name=true` at consumer read time.

### Transform contract (v3.3 Python plugins)
- **Per-dataset = `<dataset>.yaml + <dataset>.py`.** Both required.
- **`transform(con, ctx) -> DuckDBPyRelation`** signature. Runner owns the parquet write.
- **`IngestCtx`** = frozen dataclass with `config: Config` + shard-specific fields.
- **`fairway.defaults`** module exposes `default_ingest`, `apply_type_a`, `unzip_inputs` helpers.
- **No plugin registry, no base class, no discovery** — just `importlib.util.spec_from_file_location`.

### Quality gates
- **Type A column rename: fail-loud on collision.** No silent rename, no opt-in override.
- **Encoding: fail-by-default**, opt-in `Config.allow_encoding_fallback` with `latin-1` fallback.
- **Validations are first-class.** `fairway validate` is a real subcommand. Runner applies validations every shard before write — user can't skip.
- **Naming-pattern mismatch: fail-loud at submit time** with `--allow-skip` opt-in.

### Operational
- **`fairway submit` actually runs `sbatch`** by default; `--dry-run` to preview.
- **Idempotent resume by default** (skip shards with status=ok and matching source-hash). `--force` overrides.
- **Source hash** = `sha256(size_le(8) || first 4 KB || last 4 KB)` — best-effort, documented as such.
- **Manifest: per-shard fragments + finalize merge.** Atomic writes (`.tmp` + `os.rename`). No JSON write race under arrays.

### Out-of-scope (deferred until concrete trigger)
- Dagster / Snakemake orchestration (Phase 2 trigger: ≥7 datasets active OR first cross-dataset transform OR ≥3 researchers want UI)
- `fairway enrich` real impl (still a stub)
- `fairway schema infer` (deferred — scaffold .yaml works without it for v1)
- Multi-output transforms from one shard (use multiple datasets instead)
- Custom file-naming inside partitions (DuckDB default `data_*.parquet`)
- Fully-custom user-owned-write mode (`raw_python:` reserved field, not built)
- Stage 2 transforms (`transforms/` dir) — included in plan but lowest-priority; can be deferred if ceiling tight

### Trajectory (documented for context)
- 2 datasets today (50 GB TSV + 10 TB CSV)
- 5–10 datasets in 6 months
- ~20 datasets over 2-3 years
- Cross-dataset transforms hypothetical today
- DE team: small (1–3 engineers), Python-leaning, no Spark background
- Researchers: many; consume parquet via DuckDB / R / Stata / Quarto

---

## 7 — Cost / time tracker

| Step | Status | Cost | Wall time |
|---|---|---|---|
| 0 | ✅ | $2.54 | 6:13 |
| 0.5 | ✅ | ~$0.50 | ~2:00 |
| 1 | ✅ | $10.34 | 15:00 |
| 2 | ⚠️ unverified | TBD | TBD |
| 3–14 | queued | TBD | TBD |
| **Total so far** | | **~$13.40** | **~23 min** |

**Projection (revised after observing Step 1):** $80–150 + ~2.5 hours dispatch time for the full build. Higher than the initial $60 estimate.

---

## 8 — Recovery / rollback

If a step's dispatch fails mid-way and leaves the working tree dirty:

1. **Read the dispatch log:** `/tmp/forge/fairway-build-step${STEP}-*/run.log` — the agent's final report explains what got done and what blocked.
2. **Inspect git state:** `git status --short` — see what's modified or staged.
3. **Decide:**
   - **Partial work landed in a commit** → great, the commit is your save point. Investigate and either fix-forward in a new commit or revert.
   - **Partial work staged but not committed** → unstage with `git restore --staged <files>`. Either redo from clean or commit the partial work explicitly.
   - **Partial work in working tree, unstaged** → typically safe to leave; the next dispatch should stash + restore as designed.
   - **Stash from previous step's stash-restore wasn't restored** → `git stash list` and `git stash pop` to recover.

**Never use `git reset --hard` or `git clean -fd`** unless you're certain — they discard work that may be the only record of partial progress.

If a step's gates fail (mypy regression, ruff fail, importability broken):
- **Do not commit.** Mypy must monotonically decrease; ruff must stay clean.
- **Investigate** what the dispatch did. Often the `NotImplementedError` replacements during deletion steps cause `unused variable` ruff errors or unreachable mypy errors. The agent should have inline-suppressed with `# noqa` or `# type: ignore` with justification — verify.
- **Re-dispatch** the same step with a sharper prompt if needed.

If completely lost:
- **Worst case:** `git reset --hard 7b15dce` (Step 0.5, before any deletion) restores a known-good state. **Confirm with the user before doing this.**
- All step commits can be cherry-picked back if you want to restore a partial state.

---

## 9 — Known issues / things to watch

- **`src/fairway.egg-info/requires.txt` and `SOURCES.txt`** still mention pyspark/delta-spark as of Step 1. These are auto-generated by setuptools when `pip install -e .` was run earlier. They regenerate on next install. Not real source files; ignore.
- **Pre-existing edits in `rules.md`, `hpc.py`, `submit_with_spark.sh`** are kept across steps via stash/restore. They get deleted as a group in **Step 11** (rules.md) and **Step 7.4** cutover (hpc.py absorbed into cli.submit) and **Step 1 already handled** submit_with_spark.sh references. Verify.
- **Container path adjustments:** the dispatch command hard-codes `/Users/mad265/git-pub/...`; rewrite for container before first dispatch.
- **mypy gate must monotonically decrease.** If a step adds new mypy errors, the gate should block. Some agents have inline-suppressed with `# type: ignore` to satisfy the gate — verify each suppression has a justification comment.
- **Auth refresh:** Anthropic API auth via `claude` CLI typically lasts ~30 days. If dispatches start failing with auth errors, re-run `claude login` in the container.

---

## 10 — Quick reference: critical files

| File | Purpose | Source of truth for |
|---|---|---|
| `PLAN.md` | The rewrite plan | Architecture, design decisions, step-by-step actions, gates, re-entry triggers, post-completion workflow |
| `BASELINE.txt` | Step 0 metrics | Original LOC, mypy/ruff baseline, original `addopts` |
| `HANDOFF.md` (this file) | Resume instructions | Build state, dispatch templates, recovery |
| `~/.claude/.../memory/rewrite_v3_plan.md` | Locked decisions memory | Trajectory, re-entry triggers, dataset trajectory |
| `pyproject.toml` | Project metadata | Dependencies, pytest config, package-data |

When in doubt about design: **read PLAN.md.** When in doubt about resumption: **read this file.** When in doubt about both: **read the conversation export** (`2026-05-01-172624-...txt`) for full intent reconstruction.

---

**End of handoff. Good luck.**
