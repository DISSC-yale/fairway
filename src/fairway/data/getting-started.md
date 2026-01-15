# Getting Started with {name}

This project was initialized with the **{engine_type}** engine.

## Directory Structure

*   `config/`: Configuration files (datasets, checks, spark resources)
*   `data/`: Data storage (raw, intermediate, final)
*   `src/transformations/`: Custom Python transformation logic
*   `logs/`: Execution logs

## Environment Setup

If you installed Fairway in a virtual environment, ensure it is active before running commands:

```bash
# Example
source .venv/bin/activate
```


## Running the Pipeline

Use the Makefile shortcuts:

*   `make run`: Run locally (validation + ingestion)
*   `make run-hpc`: Submit to Slurm (Auto-detects Engine)

## HPC vs Local Execution

- **Local (`make run`)**: Runs everything on the current machine. Great for small data (DuckDB) or testing.
- **HPC (`make run-hpc`)**:
    - **DuckDB**: Submits a single Slurm job.
    - **Spark**: Automatically provisions an ephemeral Spark cluster, submits the job, and tears it down.

## Configuration

Edit `config/fairway.yaml` to change the engine:

```yaml
# Use DuckDB (default, unrelated to Spark)
engine: duckdb

# Use Spark (triggers auto-provisioning on HPC)
engine: spark
```
