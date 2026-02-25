# CLI Reference

Fairway provides a command-line interface for data ingestion and pipeline management.

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
| `--log-file TEXT` | `logs/fairway.jsonl` | Path to JSONL log file (empty string to disable) |
| `--log-level` | `INFO` | Log level: DEBUG, INFO, WARNING, ERROR |

**Examples:**
```bash
# Run with auto-discovered config
fairway run

# Run with specific config
fairway run --config config/production.yaml

# Dry run to see what would be processed
fairway run --dry-run

# Run with debug logging
fairway run --log-level DEBUG

# Run with custom log file
fairway run --log-file /path/to/pipeline.jsonl
```

### `fairway generate-schema`

Generate schema from data files.

```bash
fairway generate-schema [OPTIONS]
```

Scans source files and infers column types using the two-phase approach:
1. **Phase 1**: Discover all columns from all files
2. **Phase 2**: Sample files for type inference

### `fairway build`

Build the container image (Apptainer preferred, Docker fallback).

```bash
fairway build [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--apptainer` | Build Apptainer container (default) |
| `--docker` | Build Docker container |
| `--force` | Overwrite existing image |

### `fairway spark`

Manage Spark clusters for distributed processing.

```bash
fairway spark [SUBCOMMAND]
```

Subcommands:
- `start` - Start a Spark cluster
- `stop` - Stop the Spark cluster
- `status` - Show cluster status

### `fairway status`

Show status of submitted Slurm jobs.

```bash
fairway status
```

Wrapper around `squeue` with fairway-specific formatting.

### `fairway submit`

Submit the pipeline as a Slurm job with optional Spark cluster provisioning.

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
| `--dry-run` | False | Print job script without submitting |

**Examples:**
```bash
# Submit with auto-discovered config
fairway submit

# Submit with Spark cluster
fairway submit --with-spark

# Submit with custom resources
fairway submit --with-spark --mem 64G --cpus 8 --time 48:00:00

# Preview the job script
fairway submit --with-spark --dry-run
```

### `fairway summarize`

Generate summary stats and reports for already-ingested data. Use this after running `fairway run --skip-summary` to generate summaries in a separate step (useful on HPC where ingestion and summarization have different resource needs).

```bash
fairway summarize [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--config TEXT` | Auto-discover | Path to config file |
| `--spark-master TEXT` | None | Spark master URL |
| `--slurm` | False | Submit as a Slurm job (loads Spark/Java modules) |
| `--account TEXT` | From spark.yaml | Slurm account |
| `--partition TEXT` | `day` | Slurm partition |
| `--time TEXT` | `04:00:00` | Slurm time limit |
| `--mem TEXT` | `32G` | Slurm memory |
| `--cpus INTEGER` | `4` | Slurm CPUs per task |
| `--log-file TEXT` | `logs/fairway.jsonl` | Path to JSONL log file |
| `--log-level` | `INFO` | Log level |

**Examples:**
```bash
# Run summarization locally
fairway summarize

# Submit as Slurm job
fairway summarize --slurm

# Submit with custom resources
fairway summarize --slurm --mem 64G --time 08:00:00
```

### `fairway cancel`

Cancel Slurm jobs (wrapper around `scancel`).

```bash
fairway cancel [JOB_ID]
fairway cancel --all
```

| Option | Description |
|--------|-------------|
| `JOB_ID` | Specific job ID to cancel |
| `--all` | Cancel all your running jobs (requires confirmation) |

### `fairway cache`

Manage fairway cache (extracted archives, manifests).

```bash
fairway cache [SUBCOMMAND]
```

Subcommands:
- `clear` - Clear cached data
- `status` - Show cache usage

### `fairway eject`

Eject bundled scripts and container definitions for customization.

```bash
fairway eject [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--scripts` | False | Eject only Slurm/HPC scripts |
| `--container` | False | Eject only container files (Apptainer.def, Dockerfile) |
| `-o, --output TEXT` | `.` | Output directory |
| `--force` | False | Overwrite existing files without prompting |

**Examples:**
```bash
# Eject everything (container files + scripts)
fairway eject

# Eject only scripts to customize Slurm workflows
fairway eject --scripts

# Eject only container definitions
fairway eject --container

# Eject to a custom directory
fairway eject --output custom/

# Force overwrite existing files
fairway eject --force
```

**Ejected Files:**

Container files:
- `Apptainer.def` - Apptainer container definition
- `Dockerfile` - Docker container definition
- `.dockerignore` - Docker ignore patterns
- `Makefile` - Build and run commands

Scripts:
- `scripts/driver.sh` - Slurm driver job script
- `scripts/driver-schema.sh` - Schema generation driver
- `scripts/fairway-spark-start.sh` - Spark cluster startup
- `scripts/fairway-hpc.sh` - HPC utilities

### `fairway shell`

Enter an interactive shell inside the fairway container.

```bash
fairway shell
```

### `fairway pull`

Pull (mirror) the Apptainer container from the registry.

```bash
fairway pull
```

### `fairway logs`

View and filter structured pipeline logs (JSONL format).

```bash
fairway logs [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-f, --file TEXT` | `logs/fairway.jsonl` | Path to JSONL log file |
| `-l, --level` | None | Filter by log level: DEBUG, INFO, WARNING, ERROR |
| `-b, --batch TEXT` | None | Filter by batch ID (supports partial match) |
| `-n, --last INTEGER` | 0 | Show only last N entries |
| `--json` | False | Output raw JSON instead of formatted text |
| `--errors` | False | Shortcut for `--level ERROR` |

**Examples:**
```bash
# Show all logs
fairway logs

# Show last 20 entries
fairway logs --last 20

# Show only errors
fairway logs --errors
fairway logs --level ERROR

# Filter by batch ID (partial match)
fairway logs --batch claims_CT_2023

# Raw JSON output (pipe to jq for advanced queries)
fairway logs --json | jq 'select(.level == "ERROR")'

# Custom log file
fairway logs --file /path/to/other.jsonl
```

**Output Format:**
```
2026-02-06T10:00:00 [INFO] Starting ingestion for dataset: sales
2026-02-06T10:00:01 [INFO] [sales_2023_01_abc123] Processing batch 1/3
2026-02-06T10:00:15 [ERROR] [sales_2023_01_abc123] Batch failed: OOM
```

### `fairway manifest`

Inspect and query the file manifest (tracks processed files).

```bash
fairway manifest [SUBCOMMAND] [OPTIONS]
```

Subcommands:
- `show` - Display manifest entries
- `query` - Query files by status or batch
- `reset` - Reset file status (for reprocessing)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FAIRWAY_BINDS` | Additional Apptainer bind paths (comma-separated) | Auto-detected from config |
| `FAIRWAY_TEMP` | Temporary directory for large operations (archive extraction, scratch) | System temp |
| `REDIVIS_API_TOKEN` | API token for Redivis data export | None (required for export) |
| `SPARK_LOCAL_IP` | Spark driver bind address | Auto-detect |
| `PYSPARK_SUBMIT_ARGS` | Additional Spark submit arguments | Auto-configured |

**Note:** `FAIRWAY_BINDS` is useful when running on HPC clusters with different filesystem paths (e.g., `/scratch`, `/gpfs`, `/project`). Set this to your cluster's shared storage path if auto-detection doesn't find it.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 115 | Data integrity error (RULE-115) |
