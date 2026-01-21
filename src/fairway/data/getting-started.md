# Getting Started with {name}

This project was initialized with the **{engine_type}** engine.

## Directory Structure

*   `config/`: Configuration files (datasets, checks, spark resources)
*   `data/`: Data storage (raw, intermediate, final)
*   `src/transformations/`: Custom Python transformation logic
*   `scripts/`: Helper scripts for execution
    *   `fairway-hpc.sh`: **User Utility** - Use this to setup your environment (load modules).
    *   `run_pipeline.sh`: **Internal Runner** - Used by Makefile/Driver to launch Nextflow. Do not run directly.
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
*   `make run-hpc`: Run on Login Node (Interactive)
*   `make submit-hpc`: Submit Driver Job (Fire-and-Forget)

## HPC vs Local Execution

- **Local (`make run`)**: Runs everything on the current machine. Great for small data (DuckDB) or testing.
- **HPC Interactive (`make run-hpc`)**:
    - Runs Nextflow on the login node. Output streams to your terminal.
- **HPC Driver Job (`make submit-hpc`)**:
    - Submits Nextflow itself as a job. Logs are written to `logs/driver_*.log`.
    - Best for long-running pipelines.

## Configuration

Edit `config/fairway.yaml` to change the engine:

```yaml
# Use DuckDB (default, unrelated to Spark)
engine: duckdb

# Use Spark (triggers auto-provisioning on HPC)
engine: spark

sources:
  - name: "my_data"
    path: "data/raw/my_data.csv"
    format: "csv"
    transformation: "src/transformations/my_transform.py"
```
