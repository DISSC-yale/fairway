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

| Option | Description |
|--------|-------------|
| `--config TEXT` | Path to config file (auto-discovered if not specified) |
| `--spark-master TEXT` | Spark master URL (e.g., `spark://host:port` or `local[*]`) |
| `--dry-run` | Show matched files without processing |

**Examples:**
```bash
# Run with auto-discovered config
fairway run

# Run with specific config
fairway run --config config/production.yaml

# Dry run to see what would be processed
fairway run --dry-run
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

### `fairway cancel`

Cancel a Slurm job.

```bash
fairway cancel [JOB_ID]
```

### `fairway cache`

Manage fairway cache (extracted archives, manifests).

```bash
fairway cache [SUBCOMMAND]
```

Subcommands:
- `clear` - Clear cached data
- `status` - Show cache usage

### `fairway eject`

Eject container definitions for customization.

```bash
fairway eject
```

Exports `Apptainer.def` and `Dockerfile` to project directory.

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

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FAIRWAY_TEMP` | Temporary directory for large operations | System temp |
| `SPARK_LOCAL_IP` | Spark driver bind address | Auto-detect |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 115 | Data integrity error (RULE-115) |
