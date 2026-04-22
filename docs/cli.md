# CLI Reference

Fairway provides a command-line interface for data ingestion, state
management, and HPC dispatch.

## Installation

```bash
pip install fairway
# Or with all optional dependencies:
pip install fairway[all]
```

## Commands

### `fairway init`

Initialize a new fairway project with configuration templates.

```bash
fairway init [PROJECT_NAME]
```

Creates:
- `fairway.yaml` - Main configuration file
- `Makefile` - Build and run shortcuts
- `scripts/` - HPC and driver scripts

### `fairway run`

Run the ingestion pipeline (Worker Mode).

```bash
fairway run [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--config TEXT` | Auto-discover | Path to config file |
| `--spark-master TEXT` | None | Spark master URL (e.g., `spark://host:port` or `local[*]`) |
| `--dry-run` | False | Show matched files without processing |
| `--skip-summary` | False | Skip end-of-run summary stats |
| `--log-file TEXT` | PathResolver default | Path to JSONL log file (empty string to disable) |
| `--log-level` | `INFO` | Log level: DEBUG, INFO, WARNING, ERROR |

**Examples:**
```bash
fairway run
fairway run --config config/production.yaml
fairway run --dry-run
fairway run --log-level DEBUG
```

### `fairway generate-schema`

Generate schema from data files via two-phase discovery + type inference.

```bash
fairway generate-schema [OPTIONS]
```

### `fairway generate-data`

Generate synthetic test data for development/CI.

```bash
fairway generate-data [OPTIONS]
```

### `fairway build`

Build the container image (Apptainer preferred, Docker fallback).

```bash
fairway build [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--force` | Overwrite existing image |

### `fairway spark`

Manage Spark clusters for distributed processing.

```bash
fairway spark [SUBCOMMAND]
```

Subcommands:
- `start` — Start a Spark cluster (Slurm-dispatched)
- `stop` — Stop the Spark cluster

### `fairway status`

Show status of submitted Slurm jobs. Wrapper around `squeue`.

```bash
fairway status [OPTIONS]
```

### `fairway submit`

Submit the pipeline as a Slurm job, optionally provisioning a Spark cluster.

```bash
fairway submit [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--config TEXT` | Auto-discover | Path to config file |
| `--account TEXT` | From spark.yaml | Slurm account |
| `--partition TEXT` | `day` | Slurm partition |
| `--time TEXT` | `24:00:00` | Time limit (HH:MM:SS) |
| `--mem TEXT` | `16G` | Memory per node |
| `--cpus INTEGER` | `4` | CPUs per task |
| `--with-spark` | False | Start Spark cluster before running |
| `--with-summary` | False | Run `summarize` after ingest |
| `--dry-run` | False | Print job script without submitting |

`--dry-run` renders the exact script that would be submitted, using
`cfg.paths.slurm_log_dir` for the log directory (matches the real
submit path).

The rendered sbatch templates install an `EXIT` trap that calls
`fairway run-abandon` as belt-and-suspenders: if the job is OOM-killed
or hits walltime before the pipeline can finalize its own `run.json`,
the trap marks the run as `unfinished`.

### `fairway summarize`

Generate summary stats and reports for already-ingested data.

```bash
fairway summarize [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--config TEXT` | Auto-discover | Path to config file |
| `--slurm` | False | Submit as a Slurm job |

Slurm resource options (`--account`, `--partition`, `--time`, `--mem`,
`--cpus`) match `submit`.

### `fairway cancel`

Cancel Slurm jobs (wrapper around `scancel`).

```bash
fairway cancel [JOB_ID]
fairway cancel --all
```

### `fairway state`

Inspect and initialize fairway state directories.

```bash
fairway state [SUBCOMMAND]
```

Subcommands:
- `init` — Create the state/scratch dir tree for this project (idempotent).
  Warns if a legacy pre-v4.1 `./manifest/` dir is found in CWD, since
  those manifests would otherwise be orphaned on upgrade.
- `path` — Print every resolved state/scratch path (one per line,
  label TAB path).

### `fairway doctor`

Diagnose the fairway environment: env vars, project paths, last run.

```bash
fairway doctor [--config TEXT]
```

Prints `FAIRWAY_HOME`, `FAIRWAY_SCRATCH`, `FAIRWAY_RUN_ID`, the project
name, every state directory with an `[ok]`/`[miss]` marker, and the
most recent `run.json` under `log_dir` (if any).

### `fairway preflight`

Validate a config before submitting it.

```bash
fairway preflight [--config TEXT] [--require-account]
```

Runs the path-validation checks, confirms each table root and
preprocess action exists, and optionally refuses placeholder Slurm
accounts. Exits non-zero on any problem.

### `fairway cache`

Manage fairway cache (extracted archives).

```bash
fairway cache clean [--config TEXT] [--force]
```

Targets `PathResolver.cache_dir` (under `FAIRWAY_SCRATCH`). Also sweeps
the legacy CWD-local `.fairway_cache` location so operators upgrading
from pre-v4.1 installs can clean up in one step.

### `fairway eject`

Eject bundled scripts and container definitions for customization.

```bash
fairway eject [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--scripts` | Eject only Slurm/HPC scripts |
| `--container` | Eject only container files |
| `-o, --output TEXT` | Output directory (default `.`) |
| `--force` | Overwrite existing files without prompting |

### `fairway shell`

Enter an interactive shell inside the fairway container.

```bash
fairway shell [--image TEXT] [--bind TEXT]... [--dev]
```

### `fairway pull`

Pull the Apptainer container from the registry.

```bash
fairway pull
```

### `fairway logs`

View and filter structured pipeline logs (JSONL).

```bash
fairway logs [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-f, --file TEXT` | Latest `run_*.jsonl` under the sharded log dir | Path to JSONL log file |
| `--config TEXT` | Auto-discover | Config used to resolve the project log dir |
| `--run-id TEXT` | None | Inspect a specific run_id (requires `--config`) |
| `-l, --level` | None | Filter by level: DEBUG, INFO, WARNING, ERROR |
| `-b, --batch TEXT` | None | Filter by batch ID (partial match) |
| `-n, --last INTEGER` | 0 | Show only last N entries |
| `--json` | False | Output raw JSON |
| `--errors` | False | Shortcut for `--level ERROR` |

### `fairway manifest`

Inspect and query the file manifest (tracks processed files).

```bash
fairway manifest [SUBCOMMAND] [OPTIONS]
```

Subcommands:
- `list` — List every table with its file-status counts.
- `query` — Filter files by status, batch_id, or file-path substring.
  Supports `--json` for pipe-friendly output.

### `fairway run-abandon` *(hidden)*

Idempotently mark a run as `unfinished`. Called from the sbatch `EXIT`
trap installed by `submit` so OOM/walltime kills still finalize
`run.json`. Safe to call on already-terminal runs.

```bash
fairway run-abandon --config <path> <run_id>
```

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `FAIRWAY_HOME` | Root of durable state (manifests, logs, run metadata, locks). | platformdirs user state dir |
| `FAIRWAY_SCRATCH` | Root of ephemeral state (cache, temp). | platformdirs user cache dir |
| `FAIRWAY_RUN_ID` | Pin a specific run_id (highest precedence over Slurm/ULID). | Auto-generated per run |
| `FAIRWAY_RUN_MONTH` | Pin the month shard for log dir (`YYYY-MM`). Useful for tests and long-running jobs crossing UTC midnight. | Derived from run_id ULID timestamp |
| `FAIRWAY_TEMP` | User-visible fast scratch for archive extractions and preprocess batches (e.g., node-local SSD). Falls back to `PathResolver.temp_dir` under `FAIRWAY_SCRATCH`. | Unset |
| `FAIRWAY_BINDS` | Additional Apptainer bind paths (comma-separated). | Auto-detected from config |
| `FAIRWAY_SIF` | Path to the Apptainer SIF image. | `fairway.sif` |
| `REDIVIS_API_TOKEN` | API token for Redivis data export. | None |
| `SPARK_LOCAL_IP` | Spark driver bind address. | Auto-detect |
| `PYSPARK_SUBMIT_ARGS` | Additional Spark submit arguments. | Auto-configured |

### `FAIRWAY_TEMP` vs `FAIRWAY_SCRATCH`

Two scratch concepts coexist:

- **`FAIRWAY_TEMP`** (or `storage.temp` in config) — user-pointable fast
  scratch for preprocess work and archive extraction. Set this to a
  node-local SSD or fast burst-buffer to speed up ingest.
- **`FAIRWAY_SCRATCH`** — fairway-managed ephemeral root that holds the
  resolver's `cache_dir` and `temp_dir` when `FAIRWAY_TEMP` is unset.

`FAIRWAY_HOME` is *always* the durable tree (manifests, logs, run
metadata, archive-extraction locks) and is never purged mid-run.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 115 | Data integrity error (RULE-115) |
| 130 | Interrupted (SIGINT); pipeline finalized run.json as `unfinished` |
| 143 | Terminated (SIGTERM); pipeline finalized run.json as `unfinished` |
