# Getting Started with $name

This project was initialized with the **$engine_type** engine.

## Directory Structure

*   `config/`: Configuration files (datasets, checks, spark resources)
*   `data/`: Data storage (raw, intermediate, final)
*   `src/transformations/`: Custom Python transformation logic
*   `scripts/`: Helper scripts for execution
    *   `fairway-hpc.sh`: **User Utility** - Use this to setup your environment (load modules).
    *   `driver.sh`: Slurm batch script for submitting the pipeline.
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
*   `make submit`: Submit pipeline as Slurm job

Or use the CLI directly:

```bash
# Local execution
fairway run

# Submit to Slurm
fairway submit
```

## HPC vs Local Execution

- **Local (`make run`)**: Runs everything on the current machine using DuckDB.
- **HPC (`fairway submit`)**: Submits the pipeline as a Slurm job.

## Configuration

Edit `config/fairway.yaml` to set the engine:

```yaml
# DuckDB (default; only supported engine in v0.3)
engine: duckdb

tables:
  - name: "my_data"
    path: "data/raw/my_data.csv"
    format: "csv"
    transformation: "src/transformations/my_transform.py"
```
