# Fairway Rewrite Plan v3.3 (Python-plugin transforms; post-grilling-on-2B)

## Objective

Reduce Fairway from ~12,500 LOC to ~1,200–1,500 LOC by deleting features added speculatively and rewriting the survivors around one clear job: **convert per-(state, date) CSV/TSV/fixed-width files to partitioned parquet on Yale HPC (Grace/Bouchet) using DuckDB driven by Slurm job arrays**, with manifest tracking and lossless schema-drift handling.

Executed by `wiggum` (fresh-context loop runner). Each iteration reads this file, surveys repo state, picks the lowest-numbered step whose `Done when` check fails, performs only that step, commits, and either signals `WIGGUM_DONE` or loops.

## v3.3 changelog (Python-plugin transform contract)

After grilling on 2B (SQL files vs Python plugins) with the framing locked as **internal DE tool servicing social science research** (DE team configures, researchers consume parquet), the transform contract is now **Python plugins** instead of SQL files. User explicitly preferred Python over SQL.

**Locked design decisions from this grilling round:**

- **Per-dataset = `datasets/<dataset>.yaml` + `datasets/<dataset>.py`.** YAML for structured config; Python for the data flow definition. Both files required (Q1 → B) — `fairway init` scaffolds the .py with a default-ingest one-liner.
- **`ctx` is a typed dataclass `IngestCtx`** (Q2 → C) carrying frozen `Config` plus shard-specific fields (`input_paths`, `output_path`, `partition_values`, `shard_id`, `scratch_dir`).
- **Runner owns the parquet write** (Q3 → B). User's `transform(con, ctx) -> DuckDBPyRelation` returns a relation; the runner applies Type A cleanup, injects partition columns from filename, applies validations, sorts, writes parquet, computes fingerprint, writes manifest fragment. Validations are guaranteed; user can't skip them.
- **Partitioning flexibility:** `Config.partition_by` (filename-derived, runner-injected) + `Config.sub_partition_by` (data-column-derived, user adds to rel). Multi-output and fully-custom-write deferred as escape hatches (`raw_python:` mode reserved, not built in v1).
- **Zip extraction is runner-pre-extracted** (Q5 → A). `Config.unzip: bool`, `Config.zip_inner_pattern`, optional `Config.zip_password_file`. User's transform sees extracted files in `ctx.input_paths`. Runner cleans up scratch after.
- **Per-shard heterogeneity is handled by user's `transform()` branching** (e.g., `if file.suffix == ".csv": ... elif ...:`). Cross-format heterogeneity splits into multiple datasets with separate Configs.
- **`fairway.defaults` module** exposes `default_ingest`, `default_ingest.read_only`, `apply_type_a`, `apply_validations`, `unzip_inputs` helpers. ~50 LOC.
- **Plugin loader is ~25 LOC.** No registry, no base class, no discovery. `importlib.util.spec_from_file_location` + lookup `transform` attribute.
- **Stage 2 transforms** use the same `transform(con, ctx) -> DuckDBPyRelation` shape with `TransformCtx` (different fields). Lives in `transforms/<source_layer>_to_<target_layer>/<dataset>.{py,yaml}`.

**LOC budget impact:** ~+75 LOC vs the 2B SQL plan (defaults module + plugin loader). Total ceiling ~2,500 LOC. From 7,898 starting point, that's a 3.2× source reduction. Honest accounting in the audit section below.

**LOC accounting honest audit (asked by user, answered by user "ok lets move forward"):**

User-driven additions to the plan beyond minimal v1:
- Validations module real (not stub): +150 (user explicit ask)
- Medallion enforcement: +20 (user explicit ask)
- Scratch dir resolution: +10 (user explicit ask)
- Zip extraction + password: +60 (user explicit ask)
- Sort + row_group_size: +10 (user explicit ask)
- Schema in fragment + summary: +35 (user confirmed)
- Python plugin loader + IngestCtx + defaults: +75 (user "I dislike SQL")
- sub_partition_by: +5 (user "what if we partition differently")
- Stage 2 transforms (transforms/ + fairway transform real): +100 (user "want a transform process similar to what we have")
- Auto-submit: +30 (user "fairway should submit jobs for you")
- Idempotent resume + source-hash: +20 (mine, defensible)

Net additions ~+515 LOC vs the bare 1,800 LOC ceiling. v0.3 final ceiling: **~2,500 LOC** ≈ 3.2× reduction from v0.2's 7,898 LOC. Real simplification: PySpark, Delta, Apptainer, plugin registry, engine abstraction, schema enforcement (~5,400 LOC) all gone.

## v3.2 changelog (post-third-debate fixes)

Third forge debate converged 2/3 LGTM:True after 3 rounds (Anthropic dissented with concrete blockers; Google flash and pro both LGTM:True). v3.2 incorporates the substantive Anthropic findings + agent-1's mypy-ordering note + agent-3's template-rendering clarification.

**P1 fixes:**
- **Step 13 ↔ Step 11.5 contradiction** — Step 13 now references Step 11.5's recorded threshold rather than literal 67%.
- **`task_id` formula pinned** — `sha256("/".join(sorted partition kv)).hexdigest()[:16]`. `task_id` and `shard_id` are the same value (resolves the dual-name confusion).
- **`fairway run` contract pinned** — takes `--shards-file <path> --shard-index <N>`, reads pre-rendered shards manifest, does NOT re-run `batcher.expand`. Always exits 0 on shard error (manifest is truth).
- **DuckDB encoding** — default fallback changed from `cp1252` to `latin-1` (DuckDB 1.1+ supports natively; cp1252 would require Python decode fallback).
- **DuckDB version pinned** — `duckdb >= 1.1.0`. PyArrow pinned for fixed-width path.

**P2 fixes:**
- **LOC ceiling** — raised from 1500 to 1800 (the under-budget claim was demonstrably wrong; 1659 before counting `paths.py`/`batcher.py`/`__init__.py`/`logging`).
- **Array syntax pinned** to `--array=0-{N-1}%{CONCURRENCY}` consistently. `Config.slurm_concurrency = 64` default.
- **Coverage floor** — `max(60, M-5)` instead of `max(0, M-5)` to prevent silent ratchet-down across rewrites.
- **`batcher.expand_and_validate` scope** — Step 9.3 now explicitly modifies `batcher.py` (no more dangling symbol).
- **Step 0 mypy baseline** — captured AFTER `ruff --fix` (was BEFORE; ruff fixes might introduce/expose type errors).
- **Step 0.5 brittle substring replace** — replaced with token-aware tomllib edit.
- **`naming_pattern` ↔ `partition_by` contract** — group names MUST equal `partition_by` set; checked at config parse.
- **Step 10 template rendering** — pinned to `string.Template` / `.format()`, no Jinja.

## v3.1 changelog (gate-tightening)

Beyond the v3 forge-debate fixes, two additions per user direction:

- **Ruff lint gate** added as implicit Done-when on every step from Step 1 onward. Step 0 now clears any pre-existing ruff errors so subsequent steps inherit a clean baseline.
- **Mypy typecheck gate** added — error count must monotonically decrease or stay flat across the rewrite vs. baseline captured in `BASELINE.txt`. New mypy errors block the step.
- **Post-`WIGGUM_DONE` workflow** codified explicitly: 4 gates (wiggum predicates → human diff review → forge code-review debate → real-data scale validation) before merge. Includes the exact `bun cli.ts debate --review` command for Gate 3.

## v3 changelog (post-second-debate fixes)

Second forge debate on v2 (3 rounds, agents=anthropic+google). Anthropic reviewer returned `LGTM:False` consistently with concrete blockers; Google flash gave shallow `LGTM:True` without engaging the blockers; Google pro 429-errored every round. The Anthropic findings drove this v3:

- **P0:** Stripped stale "HUMAN GATE" Out-of-band bullet (categorization table now has definitive dispositions; line was leftover from v2 draft).
- **P0:** Fixed `ingest_shard` signature ambiguity — now takes `output_root: Path` and constructs the full partitioned path internally (`<output_root>/<dataset>/__state=X/__date=Y/part-0000.parquet`). Each shard writes exactly one parquet file.
- **P1:** Split monolithic Step 9 into substeps 9.1–9.6 (CLI skeleton, submit-core, pre-scan, resume, chunking, summarize trim) — same anti-monolith principle v2 applied to Step 7.
- **P1:** Reworded source-hash claim from "stable enough for content-change detection" to "best-effort — will not detect mid-file edits that preserve size." Documented `--force` as the bit-exact escape hatch.
- **P1:** Re-anchored Step 11.5 coverage gate from hardcoded 67% to `(measured post-rewrite coverage) - 5pp`.
- **P1:** Added explicit `fairway init` behavior contract in Step 9.1 (resolves "named but unspecified").
- **P1:** Step 4 test-deletion is now deterministic — grep-driven auto-discover + explicit keep/delete lists; no "survey for redundancy" judgment calls.
- **P2:** Anti-thrash rule strengthened: requires non-whitespace LOC change (was "any git diff").
- **P2:** Documented `min_row_count = 0` as the legitimate-empty-partition escape hatch.
- **P2:** Added Step 12 note that real-data scale validation is an out-of-band human task (after `WIGGUM_DONE`).
- **P2:** Step 7.1 no longer deletes `engines/` or retargets `pipeline.py` — the cutover happens in Step 7.6 once the new runner is fully functional. Avoids the `NotImplementedError` window across substeps.

## v2 changelog (post first forge debate)

Multi-model debate (Anthropic + Google reviewers, both `LGTM: False` on v1; OpenAI Codex timed out) flagged 4 P0 hazards, 1 P1 hard deadlock, plus 17 lower-severity issues. v2 applies all 22 findings:

- Strip + restore coverage gate (`--cov-fail-under=67`) bracketing the rewrite (Steps 0.5 + 11.5).
- Reorder: test cull before config rewrite before pipeline rewrite.
- Split formerly-monolithic steps 4 (duckdb_runner) and 5 (pipeline) into bounded substeps.
- **Importability gate** added implicitly to every step from Step 1 onward.
- Manifest concurrency: per-shard fragment files + finalize merge — replaces the single-JSON write race.
- Type A cleanup: use `all_varchar=true` at landing, detect column-rename collisions, append `_2`/`_3` suffix on collision and record warning in manifest.
- Pin schema fingerprint algorithm: `sha256(json.dumps(sorted([(name, dtype)])))`.
- Resolve Step 1 `__init__.py` contradiction.
- Encoding + date-column config fields explicit; default `utf-8` with `cp1252` fallback recorded in fingerprint.
- Slurm chunking for arrays exceeding `MaxArraySize` (typically 4096 on Grace).
- Categorize previously-unaddressed files: `exporters/`, `summarize.py`, `templates.py`, `generate_test_data.py`, `data/spark.yaml`, `data/Apptainer.def`, `data/Dockerfile`, `data/Makefile`.
- Stubs exit 2 (not 0) so chained pipelines fail loud.
- Anti-thrash + shell-error-stop safety rules added.
- Re-entry triggers reworked: Delta is OR (not AND); PySpark adds "single shard exceeds node memory."
- Throwaway 50 GB script must produce manifest fingerprint compatible with Step 6's contract.

## How to run

```bash
cd /Users/mad265/git-pub/DISSC/fairway
git checkout -b rewrite/v0.3   # if not already on it
wiggum -n 40 @PLAN.md
```

Iteration cap raised to 40 because step splits add ~10 substeps.

## Trajectory and orchestrator boundary (Path A)

This rewrite targets **2 datasets now → 5-10 datasets in 6 months → ~20 over 2+ years**. Cross-dataset transforms and lineage are **hypothetical** today. Decision: **build fairway as a focused ingestion utility now; layer an orchestrator (Dagster, Snakemake, or alternative) on top in Phase 2 when concrete triggers fire.**

**Phase 2 trigger** (any one fires): dataset count ≥ 7 active datasets in flight, **OR** first cross-dataset transform/lineage requirement lands as a researcher request, **OR** ≥ 3 researchers request a shared status/lineage UI. When triggered, evaluate Dagster vs Snakemake vs alternatives at that point — do not pre-commit now.

**Why the manifest format is Dagster-compatible at zero cost now:**

| Fairway concept | Dagster equivalent (Phase 2) |
|---|---|
| Manifest fragment per shard | `AssetMaterialization` |
| `schema_fingerprint` | `data_version` |
| Partition values `{state, date}` | `MultiPartitionKey` |
| `source_hashes` | input asset versions |
| `min_row_count` / `required_columns` | `@asset_check` |
| `fairway submit` array | Backfill via custom executor |

Phase 2 integration estimated at ~100 LOC because of this mapping. If the Phase 2 trigger never fires, no integration code is wasted.

## Pinned dependency versions

To make encoding behavior, parquet writes, and DuckDB feature assumptions deterministic across hosts:

- **DuckDB**: pin `duckdb >= 1.1.0` in `pyproject.toml` `[project.optional-dependencies] duckdb` (and `all`). DuckDB 1.1+ supports `read_csv(encoding=...)` with `utf-8`, `latin-1`, `utf-16`. (Forge v3 P1#5 fix.)
- **PyArrow**: pin `pyarrow >= 14.0` (used by `fixed_width.py` ingest path → arrow → parquet — forge v3 agent-1 P1 fix).
- **Python**: `requires-python = ">=3.10"` (already in `pyproject.toml`).

If a future trigger forces a DuckDB upgrade past a major version, re-validate the encoding handling and `read_csv` flags before bumping.

## Locked architectural decisions (do not relitigate)

1. **Single engine: DuckDB.** No PySpark. No Polars. No engine abstraction layer. One module `duckdb_runner.py`.
2. **Cross-node parallelism: Slurm job arrays.** One DuckDB process per shard. No Spark. With chunking when shard count > `MaxArraySize`.
3. **Storage: plain partitioned parquet.** No Delta Lake. No `delta-spark`.
4. **Schema drift: lossless landing** via `all_varchar=true` at ingest. Per-shard schema fingerprint = `sha256(json.dumps(sorted([(name, dtype) for name, dtype in schema])))`. `union_by_name=true` at read time.
5. **Type A cleanup at ingest:** lowercase column names → snake_case via `LOWER(REGEXP_REPLACE(col, '[^a-zA-Z0-9]+', '_'))`. **On collision (two or more original columns map to the same cleaned name): fail loud.** Raise `ColumnRenameCollision`; fragment manifest gets `status="error", error="rename collision: [originals] all map to '<name>'"`; no parquet written; pipeline continues to next shard. No silent rename, no opt-in override. `TRIM` strings; `STRPTIME` dates only for columns explicitly listed in `Config.date_columns: dict[str, str]` (column → format). All other columns stay VARCHAR.

6. **Encoding handling: fail-by-default + opt-in fallback.** Default `encoding = "utf-8"`. On `UnicodeDecodeError` (or DuckDB-equivalent `Conversion Error: Invalid Input`), behavior depends on `Config.allow_encoding_fallback`:
   - `allow_encoding_fallback = False` (**default**): raise `EncodingError`, fragment manifest `status="error"`, no parquet written. Researcher must explicitly set `encoding` in config or flip the flag.
   - `allow_encoding_fallback = True` (opt-in): retry read with `Config.encoding_fallback` (default `cp1252`). On success, record `encoding_used` in fragment so post-hoc query of mojibake-suspect shards is one DuckDB SELECT. On second failure, raise `EncodingError`.
7. **Reshapes via `fairway transform <in> --sql <f> --out <p>`.** DuckDB SQL is the transform DSL.
8. **`enrich` / `validate` are deferred stub subcommands** that **exit 2** and print a deferred-message to stderr. Not exit 0.
9. **No Apptainer, no Dockerfile.** `pip install` works on Grace.
10. **Path / manifest conventions enforced** (not configurable):
   - Output: `<output_root>/<dataset>/__state=<X>/__date=<Y>/part-NNNN.parquet`
   - **`task_id` (and `shard_id`) formula:** `task_id == shard_id == sha256("/".join(f"__{k}={v}" for k, v in sorted(partition_values.items()))).hexdigest()[:16]`. Deterministic, content-addressed, 16-char hex. Same value used for fragment filename (`_fragments/<task_id>.json`) AND the `shard_id` field inside the fragment — they are the same identifier (forge v3 P1#2 fix; the two names referred to one concept).
   - Partition column names are **prefixed with `__`** (double underscore) to avoid collision with source columns of the same logical name. `Config.partition_by = ["state", "date"]` is logical; the tool prefixes internally when injecting partition values and writing the Hive directory tree.
   - Source columns are preserved as-is (under whatever they're called after Type A normalization). Researchers querying see **both** `state` (from source) and `__state` (from filename) — they can compare to detect upstream inconsistency between filename-encoded metadata and in-file metadata.
   - Manifest fragments: `<output_root>/<dataset>/manifest/_fragments/<task_id>.json` (per-shard, write-once)
   - Merged manifest: `<output_root>/<dataset>/manifest/manifest.json` (produced by `fairway manifest finalize`)
   - Submission records: `<output_root>/<dataset>/manifest/_submissions/<ts>.json` (one per `fairway submit` invocation; carries array job IDs for `fairway status`)
   - Skipped-file log: `<output_root>/<dataset>/manifest/_skipped/<ts>.json` (only written when `fairway submit --allow-skip` is used; lists files matched by `source_glob` but not by `naming_pattern`)

## Anti-scope — do NOT rebuild

- ❌ `pyspark_engine`, `slurm_cluster`, engine abstraction layer
- ❌ `apptainer` build pipeline, `Apptainer.def` data file
- ❌ `delta-spark` dependency, Delta Lake support
- ❌ `schema_pipeline.py`, "schema enforcement" rules, Type C harmonization at ingest
- ❌ `validations/` framework (replaced by stub subcommand exiting 2)
- ❌ `enrichments/` framework (replaced by stub subcommand exiting 2)
- ❌ `transformations/` plugin registry (replaced by SQL subcommand)
- ❌ `rules.md` (13 KB ruleset)
- ❌ `data/spark.yaml`, `data/Apptainer.def`, `data/Dockerfile`
- ❌ `Dockerfile.dev`, `fairway-dev.def`, `entrypoint.sh`

## Survivors (in addition to the trimmed core)

- ✅ `src/fairway/fixed_width.py` (~221 LOC) — preserved; `duckdb_runner` dispatches: delimited → DuckDB `read_csv`; fixed-width → `fixed_width.py` parser → arrow → parquet write.
- ✅ `src/fairway/generate_test_data.py` (~58 LOC) — preserved; CLI utility used by tests.
- ✅ `src/fairway/summarize.py` — preserved, **trimmed to ~50 LOC** (delete Spark path, rewrite pandas path as DuckDB SQL). Wired into CLI as `summarize` subcommand. See Step 9.6.
- ✅ `data/Makefile` and `data/spark.yaml` — `Makefile` preserved (ingest helper); `spark.yaml` deleted.

## Categorization of files previously unaddressed (resolved per forge review item #11)

| File / dir | Disposition | Notes |
|---|---|---|
| `src/fairway/exporters/` | **Delete** | Verified empty package (no `.py` files, no importers). Stale `__pycache__/` only. Delete the directory. |
| `src/fairway/summarize.py` | **Keep + trim** | Wired into CLI as `summarize` subcommand, used by `test_summarize.py` and `test_summarize_separation.py`. Currently 103 LOC with pandas + Spark paths. Trim to ~50 LOC: delete Spark path, rewrite pandas path as DuckDB SQL (`SELECT column_name, COUNT(*), COUNT(DISTINCT ...) FROM read_parquet(...)`). Add to Survivors. |
| `src/fairway/templates.py` | Delete (fold loader logic into `paths.py` if needed) |
| `src/fairway/generate_test_data.py` | **Keep** |
| `src/fairway/data/spark.yaml` | Delete |
| `src/fairway/data/Apptainer.def` | Delete |
| `src/fairway/data/Dockerfile` | Delete |
| `src/fairway/data/Makefile` | Keep, update if it references deleted commands |
| Top-level `Dockerfile.dev`, `fairway-dev.def`, `entrypoint.sh` | Delete |

**If wiggum hits a HUMAN GATE row, stop and report; do not proceed.** No HUMAN GATEs remain in this plan.

## Re-entry triggers (reworked per forge review item #16)

| Deletion | Re-entry trigger |
|---|---|
| PySpark | Single source file > 500 GB unsplittable, **OR** cross-shard JOIN at ingest, **OR** single shard exceeds the largest available node's memory and cannot be sub-partitioned. |
| Delta Lake | Onboard a row-level-update source (sparse changes within a partition), **OR** time-travel queries become a regular analysis pattern, **OR** concurrent writers compete for the same logical table. (OR, not AND.) |
| Apptainer | Deploy to a non-Yale HPC cluster. |
| Validation framework | > 5 distinct check types per dataset, **OR** check authoring becomes a routine researcher activity. |
| Plugin transformations | Multiple researchers requesting distinct non-SQL transform code. |

## Per-iteration workflow

1. Read this file (`PLAN.md`).
2. `git status && git log --oneline -5`.
3. Confirm branch is `rewrite/v0.3`. If not, **stop and report**.
4. Find the lowest-numbered step in the **Steps** section whose `Done when` check fails.
5. Perform **only that one step**. Bounded scope per iteration. Do not bundle.
6. Run the verification commands in that step's `Verify` block.
7. Commit specific files with message prefix `rewrite/v0.3 stepN: <summary>`.
8. Print a one-paragraph report and a final-line `STATUS: step <N> <in progress|complete|blocked>`.
9. If **every** step's `Done when` passes after this commit, print `WIGGUM_DONE` on the final line.

### Implicit Done-when on every step from Step 1 onward (quality gates)

Every step's `Done when` block, in addition to its specific predicates, **must** also satisfy ALL of:

- **Importability:** `python -c "import fairway, fairway.cli, fairway.pipeline"` exits 0.
- **Test collect:** `pytest --collect-only -q` exits 0.
- **Lint:** `ruff check src/fairway/ --no-fix` exits 0 (zero ruff errors). Pre-existing errors must be fixed in Step 0 (see Step 0 actions) so the rest of the rewrite operates on a clean baseline.
- **Typecheck:** `mypy --ignore-missing-imports src/fairway/` produces no MORE errors than the count recorded in `BASELINE.txt` under `## Mypy baseline`. The error count must monotonically decrease (or stay flat) across the rewrite, never increase. Pre-existing mypy errors are tolerated; new mypy errors introduced by a step block that step.

If any of these four gates fails, the step is not done — even if all explicit predicates pass. Wiggum must not commit if any gate fails.

### Safety rules per iteration

- **Never push.** No `git push`.
- **Never modify `main`.** All work on `rewrite/v0.3`.
- **No destructive git** (`reset --hard`, `clean -fd`, `branch -D`, `checkout --`).
- **Never delete files outside the explicit deletion list or the categorization table.** If a HUMAN GATE row applies, stop.
- **Never skip pre-commit hooks** (`--no-verify` forbidden).
- **Never delete** `.venv/`, `data/`, `notes/`, `.agents/`, `.claude/`, `.codex/`, `.pi/`, or anything user-owned outside `src/`, `tests/`, top-level config.
- **Shell-error-stop:** if any shell command exits non-zero (other than `grep` returning 1 for no-match, which is fine), stop the iteration and report.
- **Anti-thrash:** if the same step's `Done when` fails for **3 consecutive iterations** AND `git diff --stat HEAD~3..HEAD` shows zero net non-whitespace LOC change in the step's target files (computed via `git diff -w --stat HEAD~3..HEAD -- <files>`), stop and report. Wiggum is going in circles. Whitespace-only edits do not count as progress.
- **No autonomous restructuring of locked decisions.** If you think a locked decision needs revisiting, log in iteration report and stop.

## Steps

### Step 0 — Branch + baseline snapshot + lint cleanup

**Goal:** confirm branch, capture LOC/test/coverage/mypy baseline, and clear any pre-existing ruff errors so the lint gate is enforceable from Step 1 onward.

**Done when:**
- Current git branch is `rewrite/v0.3`.
- `BASELINE.txt` exists in repo root with sections `## Source LOC`, `## Test count`, `## Pytest collect`, `## Mypy baseline`, `## Ruff baseline`, `## Original addopts`.
- `ruff check src/fairway/ --no-fix` exits 0 (any pre-existing errors fixed via `ruff check --fix` and reviewed; non-auto-fixable issues handled manually or suppressed via inline `# noqa: <code>` with justification comment).

**Actions (ruff fixes BEFORE mypy baseline — forge v3 P0 fix):**
1. `git checkout rewrite/v0.3 || git checkout -b rewrite/v0.3`.
2. Capture pre-fix LOC and test count:
   ```bash
   find src/fairway -name '*.py' -print0 | xargs -0 wc -l | tail -1
   find tests -name 'test_*.py' | wc -l
   pytest --collect-only -q 2>&1 | tail -3
   ruff check src/fairway/ 2>&1 | tail -5
   ```
   (Avoid `wc -l src/fairway/**/*.py` — globstar is not enabled in default non-interactive bash.)
3. Run `ruff check src/fairway/ --fix` to auto-fix what can be auto-fixed. Handle the remainder: inline-suppress with justification, or fix manually.
4. Verify `ruff check src/fairway/ --no-fix` exits 0.
5. **Now** capture mypy baseline (after ruff fixes have potentially introduced/exposed new type errors):
   ```bash
   mypy --ignore-missing-imports src/fairway/ 2>&1 | tail -1   # count of errors
   ```
6. Write all captured values to `BASELINE.txt` with named sections (`## Source LOC`, `## Test count`, `## Pytest collect`, `## Mypy baseline`, `## Ruff baseline`, `## Original addopts`).
7. Commit `rewrite/v0.3 step0: capture baseline metrics + clear ruff baseline`.

### Step 0.5 — Disable coverage fail-under during rewrite

**Goal:** prevent every `pytest` invocation between Steps 1–11 from failing on coverage drop as deletions remove tested LOC before tests are culled.

**Done when:**
- `pyproject.toml` no longer contains `--cov-fail-under=` in `[tool.pytest.ini_options].addopts`.
- Original `addopts` value preserved in `BASELINE.txt` under a `## Original addopts` section.
- `pytest --collect-only -q` exits 0.

**Actions (token-aware edit, not literal substring replace — forge v3 P2 fix):**
1. Append the original `addopts` line to `BASELINE.txt`.
2. Use Python tomllib + tomli_w (or `pyproject.toml` aware editing) to load `[tool.pytest.ini_options].addopts`, parse the value as a shell-tokenized arg list (`shlex.split`), filter out any token starting with `--cov-fail-under`, re-join with spaces, and write back. Do NOT use `sed`/`grep` literal-substring substitution — brittle to TOML reformatting and quoting variants.
3. Verify the resulting `addopts` no longer contains `--cov-fail-under` via `python -c "import tomllib; print(tomllib.loads(open('pyproject.toml','rb').read().decode())['tool']['pytest']['ini_options']['addopts'])"` and grep.
4. Commit.

### Step 1 — Delete PySpark engine + abstraction (excluding `duckdb_engine.py`)

**Goal:** remove all Spark code and the engine-abstraction interface — but keep `duckdb_engine.py` and `engines/__init__.py` as a transitional vestige until Step 7. Resolves the v1 Step 1 contradiction.

**Done when:**
- `src/fairway/engines/pyspark_engine.py` does not exist.
- `src/fairway/engines/slurm_cluster.py` does not exist.
- `src/fairway/engines/base.py` does not exist.
- `src/fairway/engines/__init__.py` exists but is **empty** (or contains only a `# transitional, removed in Step 7` comment). It must remain a valid package so `duckdb_engine` stays importable until Step 7.
- `src/fairway/engines/duckdb_engine.py` still exists (untouched).
- `pyproject.toml` no longer lists `pyspark` or `delta-spark` in any `[project.optional-dependencies]` block.
- `grep -r 'pyspark\|delta_spark\|delta-spark' src/` returns no matches.
- `grep -r 'from fairway.engines.pyspark_engine\|from fairway.engines.slurm_cluster\|from fairway.engines.base' src/ tests/` returns no matches.
- All call sites of removed engines are either (a) deleted, or (b) replaced with `raise NotImplementedError("PySpark removed in v0.3 rewrite — see PLAN.md re-entry triggers")`.
- **Importability gate** passes (see Per-iteration workflow).
- `mypy --ignore-missing-imports src/fairway/` and `ruff check src/fairway/` exit 0 (or pre-existing failures unchanged — capture diff in iteration report).

**Actions:** delete files, prune optional-deps, prune call sites, commit.

### Step 2 — Delete Apptainer + container artifacts

**Done when:**
- `src/fairway/apptainer.py` does not exist.
- `src/fairway/data/Apptainer.def`, `src/fairway/data/Dockerfile`, `src/fairway/data/spark.yaml` do not exist.
- `Dockerfile.dev`, `fairway-dev.def`, `entrypoint.sh` (top-level) do not exist.
- `pyproject.toml`'s `[tool.setuptools.package-data]` no longer references `*.def` or `Dockerfile`.
- `grep -r 'apptainer\|Apptainer\|singularity' src/ tests/` returns no matches.
- Importability gate passes.

### Step 3 — Delete schema-evolution / validation / enrichment / transformation modules

**Done when:**
- `src/fairway/schema_pipeline.py` does not exist.
- `src/fairway/validations/`, `src/fairway/enrichments/`, `src/fairway/transformations/` do not exist.
- `grep -r 'from fairway.validations\|from fairway.enrichments\|from fairway.transformations\|schema_pipeline' src/` returns no matches.
- `config_loader.py` no longer parses `validations:`, `enrichments:`, `transformations:`, or `schema:` blocks. Field removal is explicit (not silent accept-and-ignore).
- Importability gate passes.

### Step 4 — Cull tests for deleted modules (was Step 8 in v1)

**Goal:** remove tests that import deleted modules so `pytest --collect-only` passes cleanly. **Moved before pipeline rewrite** to break the Step 5↔Step 8 deadlock from v1.

**Done when:**
- No test file imports a deleted module (no `from fairway.engines.pyspark_engine`, `from fairway.validations`, `from fairway.enrichments`, `from fairway.transformations`, `from fairway.schema_pipeline`, `from fairway.apptainer`).
- `pytest --collect-only -q` exits 0.
- `pytest tests/ -m "not spark and not hpc" -x` runs (collected tests pass — at this point the pipeline tests will skip or fail on missing duckdb_runner; that is expected and addressed in Step 8). For now: count of collected-but-failing pipeline integration tests is recorded in iteration report.
- Importability gate passes.

**Actions (deterministic — no judgment calls per forge review P1#7):**

1. **Auto-discover tests importing deleted modules** via grep. Delete any test file matching:
   ```bash
   grep -rl 'from fairway\.engines\.pyspark_engine\|from fairway\.engines\.slurm_cluster\|from fairway\.engines\.base\|from fairway\.validations\|from fairway\.enrichments\|from fairway\.transformations\|from fairway\.schema_pipeline\|from fairway\.apptainer\|from fairway\.hpc' tests/
   ```
   Delete every file that command returns. No discretion.

2. **Explicit keep-list** (these survive regardless of any other consideration):
   - `tests/conftest.py` (trim to fixtures still used)
   - `tests/test_manifest.py`
   - `tests/test_manifest_corruption.py` (**critical** — guards Step 6 fragment design)
   - `tests/test_batcher.py`
   - `tests/test_paths.py`
   - `tests/test_config.py` (rename from `test_config_validation.py` if needed)
   - `tests/test_e2e_pipeline.py` → renamed to `tests/test_pipeline.py`
   - `tests/test_duckdb_engine.py` → renamed to `tests/test_duckdb_runner.py` in Step 7.6
   - `tests/test_fixed_width.py` (`fixed_width.py` is in Survivors)
   - `tests/test_summarize.py` (or consolidated `test_summarize.py`; see Step 9.6)
   - `tests/test_cli.py` (consolidated from all `test_cli_*.py`)

3. **Explicit delete-list** (in addition to grep results):
   - All `tests/test_cli_*.py` files (consolidated into `test_cli.py` in Step 9.1).
   - `tests/test_round2_fixes.py` (legacy v0.2 fix-tracker; not a real test surface).
   - `tests/test_pipeline_reliability.py` (overlaps with renamed `test_pipeline.py`; reliability test cases survive only if not already covered there — verified by name-based test-id grep before deletion).
   - `tests/test_pyspark_salting.py` (Spark-specific).
   - `tests/test_cli_spark_lifecycle.py` (Spark-specific).

4. **Anything not in the keep-list and not matching the grep delete-pattern: leave alone for this step.** Subsequent steps may delete more, with explicit predicates.

5. Commit with message `rewrite/v0.3 step4: cull tests for deleted modules + consolidate cli tests`.

### Step 5 — Rewrite config (`config_loader.py` → `config.py`, was Step 6 in v1)

**Goal:** small dataclass with explicit fields. **Moved before pipeline rewrite** so Step 8 can target the final shape without rework.

**Done when:**
- `src/fairway/config.py` exists, ≤ 220 LOC.
- `src/fairway/config_loader.py` does not exist.
- `Config` dataclass (frozen) exposes the fields below. **Required: `dataset_name`, `source_glob`, `naming_pattern`, `partition_by`, `python`, `storage_root`.** All others have defaults.

  **Identity & paths:**
  - `dataset_name: str`
  - `python: Path` — path to the Python transform file (required; `fairway init` scaffolds a default).
  - `storage_root: Path` — root for medallion layout. Derives `<storage_root>/raw`, `<storage_root>/processed`, `<storage_root>/curated` unless overridden.
  - `storage_raw: Path | None = None` — override for raw layer (default `<storage_root>/raw`).
  - `storage_processed: Path | None = None` — override for processed layer.
  - `storage_curated: Path | None = None` — override for curated layer.
  - `layer: Literal["raw","processed","curated"] = "raw"` — which layer this Config writes to.

  **Source & partitioning:**
  - `source_glob: str` — file glob, possibly `*.zip` or `*.tsv` etc.
  - `source_format: Literal["delimited","fixed_width"] = "delimited"`
  - `naming_pattern: str` — regex with named groups; group names MUST equal `partition_by` set.
  - `partition_by: list[str]` — filename-derived partition keys. Runner injects as `__<key>` columns.
  - `sub_partition_by: list[str] = field(default_factory=list)` — data-derived partition keys. User adds these columns inside `transform()`; runner uses both lists in DuckDB's `PARTITION_BY`.

  **Reading (delimited):**
  - `delimiter: str = "\t"`
  - `has_header: bool = True`
  - `encoding: str = "utf-8"`
  - `encoding_fallback: str = "latin-1"`
  - `allow_encoding_fallback: bool = False`
  - `date_columns: dict[str, str] = field(default_factory=dict)` — column → STRPTIME format. Empty dict = all columns stay VARCHAR.

  **Type A cleanup:**
  - `apply_type_a: bool = True` — runner applies lowercase/snake_case rename + fail-loud collision detection. Set to False for datasets whose column names are already canonical and where renaming would break things.

  **Zip handling:**
  - `unzip: bool = False`
  - `zip_inner_pattern: str = "*.csv|*.tsv|*.txt"`
  - `zip_password_file: Path | None = None`

  **Validations (runner-applied after transform returns rel):**
  - `validations: Validations = field(default_factory=Validations)` — sub-dataclass with: `min_rows: int = 1`, `max_rows: int | None = None`, `check_nulls: list[str] = []`, `expected_columns: ExpectedColumns | None = None` (with `columns: list[str]` and `strict: bool = False`), `check_range: dict[str, RangeSpec] = {}`, `check_values: dict[str, list] = {}`, `check_pattern: dict[str, str] = {}`.

  **Performance / output tuning:**
  - `sort_by: list[str] | None = None` — sort within each shard before writing. Improves compression + downstream query speed.
  - `row_group_size: int = 122880` — DuckDB default; tunable per dataset.

  **Slurm:**
  - `slurm_account: str | None = None`
  - `slurm_partition: str | None = None`
  - `slurm_chunk_size: int = 4000` — max array size before chunking.
  - `slurm_concurrency: int = 64` — per-array `%CONCURRENCY` cap.
  - `slurm_mem: str = "64G"` — `--mem` per array task.
  - `slurm_cpus_per_task: int = 8` — `--cpus-per-task` per array task. DuckDB `PRAGMA threads` matches this.
  - `slurm_time: str = "2:00:00"` — `--time` per array task.

  **Runtime resolution:**
  - `scratch_dir: Path | None = None` — runner resolves at startup: `Config.scratch_dir` → `$SLURM_TMPDIR` → `$TMPDIR` → `/tmp`.

- `IngestCtx` dataclass (frozen): `config: Config`, `input_paths: list[Path]`, `output_path: Path`, `partition_values: dict[str, str]`, `shard_id: str`, `scratch_dir: Path`. Passed to user's `transform(con, ctx) -> DuckDBPyRelation`.

- `TransformCtx` dataclass (frozen, for Stage 2): `transform_config: TransformConfig`, `input_paths: list[Path]` (parquet files in source layer), `output_path: Path`, `partition_values: dict[str, str]`, `shard_id: str`, `scratch_dir: Path`. Same shape as IngestCtx but distinguishes Stage 2 from Stage 1.
- No engine-selection field.
- All importers use `from fairway.config import Config`.
- Importability gate passes.

**Actions:** read existing 832-LOC `config_loader.py`, write minimal `config.py`, update imports, delete old, commit.

### Step 6 — Manifest fragment + finalize redesign

**Goal:** eliminate the JSON write race that Slurm job arrays would cause. **New step explicitly addressing forge review P0 #2.**

**Done when:**
- `src/fairway/manifest.py` ≤ 220 LOC.
- Manifest has two physical forms:
  - **Fragments**: `<output_root>/<dataset>/manifest/_fragments/<task_id>.json` — written once per shard, idempotent, atomically (`.tmp` + `os.rename`).
  - **Merged**: `<output_root>/<dataset>/manifest/manifest.json` — produced by `fairway manifest finalize`, never written by ingest workers.
- Fragment schema: `{shard_id, partition_values: dict, source_files: list, source_hashes: list[str], schema_fingerprint: str, row_count: int, ingest_ts: str, status: Literal["ok","error"], error: str | None, encoding_used: str}`.
- **Source hash algorithm (pinned):** `sha256(file_size_bytes_le(8) || first_4096_bytes || last_4096_bytes)`. Cheap on 50 GB files (no full read). **Best-effort content-change detection only — will NOT detect mid-file edits that preserve file size.** This is an explicit performance/correctness tradeoff: full-content hashing of 50 GB files would dominate ingest cost. Researchers who need bit-exact change detection should pass `--force` to bypass the hash check. For files smaller than 8 KB, hash the entire file. Documented in module docstring with this tradeoff stated.
- Schema fingerprint = `sha256(json.dumps(sorted([(name, dtype) for name, dtype in schema_pairs])).encode()).hexdigest()`.
- `fairway manifest finalize <output_root>/<dataset>` reads all fragments, validates no duplicate `shard_id`, and writes the merged file.
- `test_manifest.py` and `test_manifest_corruption.py` cover: fragment round-trip, finalize merge, duplicate-shard-id detection, partial-write recovery (simulated crash mid-write).
- All tests in those two files pass.
- Importability gate passes.

### Step 7 — Build runner with Python plugin contract (split into 4 substeps)

**Goal (v3.3 — Python plugin contract):** runner loads a user-supplied `transform(con, ctx) -> DuckDBPyRelation` from `<dataset>.py`, applies Type A + partition cols + validations + sort, writes parquet, computes fingerprint, writes manifest fragment. Helpers in `fairway.defaults` give the user a one-liner default-ingest path.

#### Step 7.1 — Skeleton: dataclasses + plugin loader + empty `defaults` module

**Done when:**
- `src/fairway/duckdb_runner.py` exists with skeleton `run_shard(con, ctx) -> ShardResult` (body raises `NotImplementedError`).
- `src/fairway/transform_loader.py` exists (~25 LOC):
  ```python
  def load_transform(path: Path) -> Callable[[DuckDBPyConnection, "IngestCtx"], DuckDBPyRelation]:
      """Import the user's <dataset>.py and return its `transform` function. No registry, no class."""
  ```
- `src/fairway/defaults.py` exists with stub functions `default_ingest`, `default_ingest_read_only`, `apply_type_a`, `apply_validations`, `unzip_inputs` (each raises `NotImplementedError`; bodies filled in 7.2).
- `src/fairway/validations.py` exists with stub `apply_validations(rel, validations: Validations) -> None`.
- `IngestCtx` and `TransformCtx` frozen dataclasses defined (in `config.py` or a new `ctx.py`).
- `ShardResult` dataclass: `row_count`, `schema_fingerprint`, `schema: list[ColumnSpec]`, `output_paths: list[Path]`, `duration_seconds`, `encoding_used`.
- **`src/fairway/engines/duckdb_engine.py` is NOT deleted yet** — cutover deferred to Step 7.4. Pipeline.py imports stay pointed at the old engine.
- Importability gate passes; pipeline tests still run via old engine.

#### Step 7.2 — Build out `fairway.defaults` + `fairway.validations` helpers

**Goal:** all the runner-side logic lives as small testable helpers. Validations is its own module (~120 LOC). Defaults wraps the read + helpers (~50 LOC). Type A and unzip are utilities in defaults.

**Done when:**
- `src/fairway/defaults.py` ≤ 80 LOC implements:
  - `default_ingest_read_only(con, ctx) -> DuckDBPyRelation` — applies the read step only (delimited via `con.read_csv` with config-driven options; fixed_width via `fixed_width.py` parser → `con.from_arrow`). Handles encoding fallback per `config.allow_encoding_fallback`.
  - `default_ingest(con, ctx) -> DuckDBPyRelation` — calls `default_ingest_read_only`, returns the rel as-is. Used by the scaffold transform.
  - `apply_type_a(rel) -> DuckDBPyRelation` — lowercase/snake_case rename via `LOWER(REGEXP_REPLACE(...))`. **Fail-loud on collision** (raises `ColumnRenameCollision`).
  - `unzip_inputs(zip_paths, scratch_dir, inner_pattern, password_file) -> list[Path]` — extract via stdlib `zipfile`, returns extracted paths.
- `src/fairway/validations.py` ≤ 150 LOC implements:
  - `Validations` dataclass parsed from YAML block (`min_rows`, `max_rows`, `check_nulls`, `expected_columns`, `check_range`, `check_values`, `check_pattern`).
  - `apply_validations(rel, v: Validations) -> None` — runs each check via DuckDB SQL aggregates. Raises `ShardValidationError(check_name, detail)` on first failure.
- Tests:
  - `test_defaults.py` covers `default_ingest_read_only` round-trip (UTF-8, cp1252-fallback success, cp1252-fallback-disabled failure), `apply_type_a` round-trip + collision raise, `unzip_inputs` for plain + password-protected.
  - `test_validations.py` covers each check type (empty, range violation, null violation, pattern mismatch, missing required column, etc.).
- Importability + collect gates pass.

#### Step 7.3 — `duckdb_runner.run_shard` full pipeline integration

**Goal:** glue everything together — connection setup, plugin load, user transform, runner pipeline, write, fragment.

**Done when:**
- `src/fairway/duckdb_runner.py` ≤ 200 LOC implements `run_shard(con, ctx) -> ShardResult` with this exact pipeline:

  ```
  1. Configure con: PRAGMA threads={cpus_per_task}, memory_limit, temp_directory={ctx.scratch_dir}.
  2. If config.unzip: ctx.input_paths = unzip_inputs(...), record original_zips for cleanup.
  3. user_transform = transform_loader.load_transform(config.python)
  4. rel = user_transform(con, ctx)                      # USER CODE
  5. if config.apply_type_a: rel = apply_type_a(rel)
  6. for k, v in ctx.partition_values.items(): rel = rel.project(f"*, '{v}' AS __{k}")
  7. validations.apply_validations(rel, config.validations)   # raises on failure
  8. if config.sort_by: rel = rel.order(", ".join(config.sort_by))
  9. ctx.output_path.parent.mkdir(parents=True, exist_ok=True)
  10. partition_keys = [f"__{k}" for k in config.partition_by] + config.sub_partition_by
  11. rel.write_parquet(str(ctx.output_path), compression="zstd",
                        row_group_size=config.row_group_size,
                        partition_by=partition_keys)
  12. schema = describe_parquet(con, ctx.output_path)
  13. fp = schema_fingerprint(schema)
  14. fragment = build_fragment(ctx, schema, fp, row_count=rel.count(), status="ok")
  15. write_fragment_atomically(ctx, fragment)
  16. cleanup_scratch(ctx.scratch_dir)
  return ShardResult(...)
  ```
- On any exception (encoding, validation, collision, write failure): catch, build fragment with `status="error", error=str(exc)`, write atomically, **return ShardResult with status=error** (don't re-raise — Slurm task exits 0; manifest is truth).
- Test `test_duckdb_runner.py` integration test: synthetic 1 MB fixture, state×date partitioning, verifies (a) output partition tree, (b) fragment exists with correct schema/fingerprint/row_count, (c) error fixture produces error fragment but exit 0, (d) sub_partition_by works (data column becomes second-level partition).
- Importability + collect gates pass; all pipeline tests pass.

#### Step 7.4 — Cutover + LOC budget + delete `engines/`

**Done when:**
- `pipeline.py` imports retargeted from `fairway.engines.duckdb_engine` to `fairway.duckdb_runner`. Call sites updated to pass `IngestCtx`.
- `src/fairway/engines/duckdb_engine.py` does not exist.
- `src/fairway/engines/__init__.py` does not exist.
- `src/fairway/engines/` directory does not exist.
- LOC budgets enforced:
  - `duckdb_runner.py` ≤ 200
  - `defaults.py` ≤ 80
  - `validations.py` ≤ 150
  - `transform_loader.py` ≤ 30
- All pipeline tests in `test_pipeline.py` pass.
- Importability + collect gates pass.

### Step 8 — Trim `pipeline.py` (split substeps; was Step 5 in v1)

#### Step 8.1 — Skeleton orchestrator

**Done when:**
- `src/fairway/pipeline.py` body replaced with a `run_pipeline(config: Config, shard_index: int | None) -> PipelineResult` function that loops shards from `batcher` and calls `duckdb_runner.ingest_shard`.
- `pipeline.py` ≤ 400 LOC at this point (cleaning continues in 8.3).
- No `engine` parameter, no `if engine == ...` branches, no batch-strategy enum dispatch.
- Importability + collect gates pass.

#### Step 8.2 — Wire to `manifest` fragment writes

**Done when:**
- After each successful `ingest_shard`, pipeline writes a fragment via `manifest.write_fragment(...)`.
- On error, fragment is written with `status="error"`, `error=str(exc)`. Pipeline does **not** re-raise during array execution — recorded for retry on next run.
- Test: `test_pipeline.py` covers happy path and error-recorded-but-not-fatal path.
- Importability + collect gates pass.

#### Step 8.3 — LOC budget

**Done when:**
- `src/fairway/pipeline.py` ≤ 300 LOC.
- All `test_pipeline.py` tests pass.
- Importability + collect gates pass.

### Step 9 — CLI (split into substeps; v2 fix per forge review item P1#3)

Step 9 was originally a 9-feature monolith. Split into 6 bounded substeps to match the same anti-monolith principle applied to Step 7.

#### Step 9.1 — CLI skeleton + simple subcommands

**Done when:**
- `src/fairway/cli.py` exists with `click` (or `argparse`) command group and these subcommands wired:
  - `init <dir>` — see explicit behavior below.
  - `run --shards-file <path> --shard-index <N>` — reads the JSON shards manifest at `<path>` (written by `fairway submit`), looks up the entry at index `<N>`, builds `IngestCtx`, calls `duckdb_runner.run_shard(con, ctx)`. **Does not re-run `batcher.expand` on `Config`** (avoids glob-ordering drift across resubmits). Always exits **0** even on shard error — manifest fragment is the source of truth for shard status. Pipeline-level exceptions (config parse error, plugin load error) DO exit non-zero.
  - `status --dataset <name>` — reads most recent submission record, runs `squeue -j <array_job_id>`, prints aggregate summary including manifest fragment counts (ok / error / pending).
  - `transform <transform.yaml>` — **REAL subcommand (v3.3 — was deferred stub)**. Reads transform YAML, loads `transforms/<stage>/<dataset>.py`, runs `transform(con, ctx)` against landed parquet from `input_layer`, writes to `output_layer`. Same plugin contract as ingest. See Step 9.6.
  - `transform-submit <transform.yaml>` — Slurm-array variant of `transform`, for transforms over very large datasets. Mirrors `submit` but for Stage 2.
  - `validate <dataset_path> --rules <yaml>` — **REAL subcommand (v3.3 — was deferred stub).** Loads checks YAML, runs `apply_validations()` against landed parquet (one shard at a time, or aggregate). Reports per-check pass/fail. Exit 0 on all-pass; exit 2 on any failure. ~30 LOC subcommand wrapping `fairway.validations.apply_validations`.
  - `enrich` — still a stub; exits 2 with deferred message.
  - `summarize` — wires to `summarize.py` (filled in Step 9.7).
  - `manifest finalize <output_root>/<dataset>` — runs the Step 6 merge + writes `schema_summary.json`.
  - `submit` — declared but body is `raise NotImplementedError` until Step 9.2.
- **`fairway init <project_dir> --dataset <name>` behavior contract (v3.3 — scaffolds .yaml + .py per dataset):**
  - Refuses if `<project_dir>` exists and is non-empty unless `--force` passed (only for first invocation; subsequent `init --dataset` calls add to an existing project).
  - On first invocation (no `<project_dir>` yet, or with `--init-project`): creates `<project_dir>/datasets/`, `<project_dir>/transforms/raw_to_processed/`, `<project_dir>/transforms/processed_to_curated/`, `<project_dir>/data/`, `<project_dir>/build/`, `<project_dir>/.gitignore` (excluding `data/`, `build/`, `.venv/`), and a project-level `README.md`.
  - With `--dataset <name>`: writes `<project_dir>/datasets/<name>.yaml` (fully-commented example config, all Config fields with defaults shown) and `<project_dir>/datasets/<name>.py`:
    ```python
    """Ingest transform for <name>.

    Edit `transform` to customize. Default behavior is `default_ingest(con, ctx)`.
    See fairway.defaults for `default_ingest_read_only`, `apply_type_a`, etc.
    """
    from fairway.defaults import default_ingest

    def transform(con, ctx):
        return default_ingest(con, ctx)
    ```
  - Total scaffolded LOC per dataset < 100.
- `fairway --help` exits 0; `python -m fairway --help` exits 0.
- Test: `test_cli.py` covers `init` round-trip (creates expected tree, refuses non-empty without `--force`), `transform` with a trivial SQL file, `enrich`/`validate` exit 2.
- Importability + collect gates pass.

#### Step 9.2 — `fairway submit` core (sbatch invocation, no resume/pre-scan/chunking yet)

**Done when:**
- `submit <config>` reads `Config`, calls `batcher.expand(config)` to enumerate shards, **writes the shards manifest** to `build/sbatch/<dataset>.shards.json` as a JSON list of `{shard_id, input_paths, partition_values}` entries (one per shard, in deterministic order — sort by `shard_id`). Renders ONE sbatch script `build/sbatch/<dataset>.sh` from `slurm_templates/job_array.sh` (Step 10) with `#SBATCH --array=0-{N-1}%{CONCURRENCY}` (default `CONCURRENCY=64`, settable via `Config.slurm_concurrency`).
- Invokes `subprocess.run(["sbatch", script], check=True, capture_output=True, text=True)`, parses array job ID from `Submitted batch job <id>` stdout.
- Appends a record to `<output_root>/<dataset>/manifest/_submissions/<ts>.json`: `{submitted_at, chunk_index: 0, sbatch_script, array_job_id, n_shards}`.
- `--dry-run` flag: renders script, prints intended `sbatch` command, does **not** invoke `sbatch`, does **not** write submission record.
- If `sbatch` not on PATH and not `--dry-run`: refuse with `Error: sbatch not found on PATH. Run from a Slurm-enabled host or pass --dry-run to render scripts only.` Exit 2.
- Test: `test_cli.py` covers `submit --dry-run` (renders script, no submission), `submit` with mocked `sbatch` (writes submission record).
- Importability + collect gates pass.

#### Step 9.3 — `fairway submit` pre-scan validation (`naming_pattern` + `--allow-skip`)

**Goal:** add the pre-scan gate. **This step also modifies `batcher.py`** to expose `expand_and_validate` (forge v3 P2 — Step 9.3 introduced the symbol but no step actually wrote it; now explicit).

**Done when:**
- `src/fairway/batcher.py` exports `expand_and_validate(config: Config) -> tuple[list[ShardSpec], list[Path]]`. `ShardSpec` is a dataclass `{shard_id: str, input_paths: list[Path], partition_values: dict[str, str]}` where `shard_id` is computed via the formula in locked decisions.
- Implementation expands `config.source_glob`, attempts `re.fullmatch(config.naming_pattern, file.name)`. If match → group dict → partition_values; multiple files with same partition_values cluster into one ShardSpec. If no match → file added to unmatched list.
- Before rendering any sbatch script, `submit` calls `batcher.expand_and_validate(config)`.
- If `unmatched_files` is non-empty AND `--allow-skip` is not passed: print `Error: <N> file(s) in source_glob did not match naming_pattern: <list>` to stderr and exit 2.
- With `--allow-skip`: write `<output_root>/<dataset>/manifest/_skipped/<ts>.json` containing `[{file, reason: "naming_pattern mismatch"}, ...]`. Proceed with `matched_shards`.
- `--dry-run` prints what would be skipped without writing the skipped log.
- Test: `test_batcher.py` covers `expand_and_validate` with both clean and dirty globs (forge v3 P2 fix), and naming_pattern-vs-partition_by mismatch fails fast at config parse time (Step 5 covers this). `test_cli.py` covers submit-rejection and submit-with-allow-skip.
- Importability + collect gates pass.

#### Step 9.4 — `fairway submit` idempotent resume + `--force`

**Done when:**
- After Step 9.3 produces `matched_shards`, `submit` filters them: for each candidate shard, compute `source_hash` per file using the algorithm pinned in locked decisions (sha256 of `size || first 4 KB || last 4 KB`). If a fragment exists at `manifest/_fragments/<task_id>.json` with `status="ok"` AND every file's recorded `source_hash` matches the current hash → skip. Else → include.
- `--force` flag: bypass all skip logic; re-ingest every shard.
- `--dry-run` prints `would-submit`, `would-skip-existing-ok`, and `would-reject-regex-mismatch` counts and lists.
- Test: `test_cli.py` covers `submit` after a partial prior run (some shards `status="ok"`, some `status="error"`) — only error-or-missing shards re-included. `--force` re-includes everything.
- Importability + collect gates pass.

#### Step 9.5 — `fairway submit` chunking for large arrays

**Done when:**
- If `len(included_shards) > Config.slurm_chunk_size` (default 4000), `submit` emits multiple sbatch scripts: `build/sbatch/<dataset>-chunk-0.sh`, `<dataset>-chunk-1.sh`, ... each covering a contiguous slice of `included_shards`.
- `--chunk-size N` overrides `Config.slurm_chunk_size`.
- All chunks submitted independently — no Slurm dependencies between them; chunks run in parallel.
- Submission record per chunk: `{chunk_index, sbatch_script, array_job_id, n_shards}`.
- `fairway status` aggregates across all chunks of the most recent submission.
- Test: `test_cli.py` covers `submit` with `n > chunk_size` producing N chunks; `status` aggregating across chunks.
- `src/fairway/cli.py` ≤ 280 LOC after all of 9.1–9.5 land.
- `src/fairway/hpc.py` does not exist (folded into cli + slurm_templates).
- Importability + collect gates pass.

#### Step 9.6 — Trim `summarize.py`

**Goal:** keep `summarize` subcommand working, on DuckDB only.

**Done when:**
- `src/fairway/summarize.py` ≤ 60 LOC.
- No `pyspark` import anywhere in the file.
- Body uses DuckDB SQL against landed parquet: per-column `COUNT`, `COUNT(DISTINCT)`, null count, and (for numeric columns) `MIN/MAX/AVG/MEDIAN`. Writes a CSV summary.
- `cli.py`'s `summarize` subcommand wires through to it.
- `test_summarize.py` and `test_summarize_separation.py` (or a single consolidated `test_summarize.py`) pass.
- Importability + collect gates pass.

### Step 10 — Slurm template file

**Goal:** the `job_array.sh` script that Slurm runs per array task. Submit logic lives in Steps 9.2–9.5; Step 10 is just the .sh content.

**Done when:**
- `src/fairway/slurm_templates/job_array.sh` exists, ≤ 60 LOC.
- Template uses `#SBATCH --array=0-{N-1}%{CONCURRENCY}` (consistent syntax — forge v3 P2 fix) with `$SLURM_ARRAY_TASK_ID` selecting shard from `build/sbatch/<dataset>.shards.json` (single chunk) or `build/sbatch/<dataset>-chunk-<i>.shards.json` (when chunked).
- Body: activate venv (or `module load`), invoke `python -m fairway run --shards-file <path> --shard-index $SLURM_ARRAY_TASK_ID`. Exit code 0 always (manifest is truth).
- Variable substitution: simple Python `.format()` or `string.Template` — no Jinja, no DSL.
- Importability + collect gates pass.

### Step 11 — Delete `rules.md` and prune docs

**Done when:**
- `rules.md` does not exist.
- `docs/architecture.md`, `docs/schema_evolution.md`, `docs/validations.md` either updated to reflect the new design or deleted.
- `README.md` updated: removes Nextflow references, removes Apptainer references, removes engine-selection flag examples; adds the new CLI surface.
- Importability + collect gates pass.

### Step 11.5 — Restore coverage gate (re-anchored to post-rewrite measurement)

**Goal:** the v1 67% coverage gate reflected the pre-rewrite codebase. After deleting ~11k LOC and dozens of tests, the appropriate threshold is whatever the post-rewrite codebase actually achieves, **minus a small tolerance**. Forge review (P1#5) flagged the v1 hardcoded 67% as either vacuous or spuriously failing.

**Done when:**
- Run `pytest tests/ -m "not spark and not hpc"` once with `--cov` reporting only (no fail-under). Read the resulting total coverage percentage `M`.
- Set `--cov-fail-under = max(60, M - 5)` — 5pp slack for organic test churn, **floored at 60%** to prevent silent ratchet-down across future rewrites (per forge v3 P2 — `max(0, M-5)` allowed gate decay). Round down to integer percent.
- Append to `BASELINE.txt` under `## After`: `original_addopts: <value>; measured_coverage_pct: M; new_threshold: max(60, M - 5)`.
- `pyproject.toml`'s `addopts` updated with the new threshold.
- `pytest tests/ -m "not spark and not hpc" -x` exits 0.

### Step 12 — Smoke test on synthetic fixture

**Done when:**
- `tests/test_pipeline.py` includes an end-to-end smoke test ingesting a < 100 MB synthetic delimited fixture with state×date partitioning, asserting: output partition tree shape, manifest fragment exists per shard, finalize merges them, fingerprints match across shards with identical schemas, fingerprints differ across shards with different schemas.
- The smoke test passes.
- Importability + collect gates pass.

**Note (per forge review P2#10):** the in-loop smoke test caps at < 100 MB. **Real-data scale validation is an out-of-band human task** — after `WIGGUM_DONE` and before declaring the rewrite done in production, ingest the 50 GB TSV via the new tool end-to-end on Grace and confirm parity with the throwaway-script output. Wiggum is not asked to validate at scale because real-data fixtures are too large for CI.

### Step 13 — Final tidy

**Done when:**
- `find src/fairway -name '*.py' -print0 | xargs -0 wc -l` total ≤ **2500** (v3.3 — Python-plugin transforms add `defaults.py` + `transform_loader.py` + `validations.py` + IngestCtx/TransformCtx; medallion + zip + sort + sub_partition_by + auto-submit user-driven additions; honest LOC accounting in v3.3 changelog).
- `find tests -name 'test_*.py' | wc -l` ≤ 20.
- `pytest tests/ -m "not spark and not hpc" -x` passes with coverage **≥ Step 11.5's recorded threshold** (i.e., the value written to `pyproject.toml` in Step 11.5 — NOT a literal 67%).
- `BASELINE.txt` updated with `## After` section: final LOC, final test count, final coverage %. (Removed the v1 "≥ 8000 lines deleted" target — adversarial; LOC budget is the constraint, not a goal in itself.)
- A final commit summarizes the rewrite with a before/after LOC table per module.
- Importability + collect gates pass.

### Step 14 — Done

When **all** of Step 0 through Step 13's `Done when` criteria pass, print on the final line of the iteration's output:

```
WIGGUM_DONE
```

Do not print this signal at any earlier point.

## Post-WIGGUM_DONE workflow (out-of-band, human-driven)

`WIGGUM_DONE` means **the plan's predicates all pass** (code compiles, imports work, tests pass, LOC under budget, lint+typecheck clean). It does NOT mean the rewrite is fit to ship. Three independent quality gates must clear before merging `rewrite/v0.3` to `main`:

### Gate 1 — Wiggum predicates (automated)
Already covered by `WIGGUM_DONE`. Necessary, not sufficient.

### Gate 2 — Human diff review
- Run `git diff main..rewrite/v0.3 --stat` to see what changed at file granularity.
- Read every non-deletion file end-to-end: `cli.py`, `config.py`, `pipeline.py`, `manifest.py`, `paths.py`, `batcher.py`, `duckdb_runner.py`, `summarize.py`, `slurm_templates/job_array.sh`.
- Spot-check 2–3 of the deletion-heavy diffs (e.g. `git diff main..rewrite/v0.3 -- src/fairway/pyspark_engine.py`) to confirm what was removed matches the plan.
- If anything looks structurally awkward or hard to read, file an issue and have wiggum (or a human) iterate. Do not ship code you can't read.

### Gate 3 — Multi-model code review debate
- Run forge debate against the actual code diff (not the plan):
  ```bash
  WORKDIR=/tmp/forge/fairway-code-review-$(date +%s)
  mkdir -p "$WORKDIR"
  git diff main..rewrite/v0.3 > "$WORKDIR/diff.patch"
  git log --oneline main..rewrite/v0.3 > "$WORKDIR/commits.txt"
  FORGE_SKIP_PROVIDERS=openai-codex bun \
    /Users/mad265/git-pub/maurice/agent-infrastructure/agent-config/pi/extensions/forge/cli.ts debate \
    "Review the Fairway rewrite diff for production safety. Find P0/P1 bugs, incorrect refactors, dropped behaviors, untested code paths." \
    --review \
    --context "$WORKDIR/diff.patch" \
    --context "$WORKDIR/commits.txt" \
    --rounds 2 \
    --agents 3 \
    --workdir "$WORKDIR"
  ```
- The `--review` flag injects the project's `code-review` skill so reviewers apply a consistent severity rubric.
- Block merge until P0/P1 findings are addressed.

### Gate 4 — Real-data scale validation
- Ingest the 50 GB TSV via `fairway submit --dry-run` first, then `fairway submit` for real, on Grace.
- Confirm parity with the throwaway-script output (row counts, schema fingerprints, partition tree).
- Confirm `fairway manifest finalize` produces a clean merged manifest.
- Confirm `fairway summarize` and `fairway transform` work against the landed parquet.

Only when **all four gates** pass does the rewrite ship. The first three usually take 1–3 hours combined; the fourth depends on Grace queue time.

## Out-of-band tasks (human, not wiggum)

- **Ingest the overdue 50 GB TSV** with the throwaway DuckDB script BEFORE relying on the rewrite. **The throwaway must produce manifest fragments compatible with the Step 6 contract** (or be re-ingestible by `fairway run` afterward) — otherwise the throwaway parquet is opaque to the new tool. Minimum compatibility: write per-shard schema fingerprints with the pinned algorithm, and use the same path convention.
- **Choose the dataset slug** for the `<dataset>` token in path conventions (one slug per dataset config file).

## Notes for the executing agent

- This plan is a contract. Do not improvise structure. Do not introduce abstractions for hypothetical flexibility.
- If a step looks already done, verify with the listed `Done when` check before declaring done. File existence alone is insufficient — LOC budgets and importability gate matter.
- If a step's actions reveal a load-bearing dependency this plan missed, **stop, report, and end the iteration** without `WIGGUM_DONE`. The human will update this plan.
- If you find yourself wanting to "just add a small abstraction" — **stop**. The whole point of the rewrite is to refuse that instinct.
- Iteration reports must end with: `STATUS: step <N> <in progress|complete|blocked: <reason>>`.
