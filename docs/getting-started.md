# Getting Started

## Installation

```bash
pip install git+https://github.com/DISSC-yale/fairway.git
```

## Initializing a Project

```bash
fairway init my_project --engine spark
cd my_project
```

## Running the Pipeline

### Local Development (DuckDB)

Run the pipeline locally on your laptop:

```bash
fairway run
# Or with explicit config:
fairway run --config config/fairway.yaml
```

### HPC Execution (Slurm + Spark)

Submit the pipeline to a Slurm cluster with Spark:

```bash
# Submit with Spark cluster
fairway submit --with-spark

# Submit with custom resources
fairway submit --with-spark --mem 64G --cpus 8 --time 48:00:00

# Preview the job script first
fairway submit --with-spark --dry-run
```

### Check Job Status

```bash
fairway status           # Show your running jobs
fairway cancel <JOB_ID>  # Cancel a specific job
fairway cancel --all     # Cancel all your jobs
```

## Directory Structure
*   `config/`: Configuration files.
*   `data/`: Data storage.
*   `src/`: Custom code.
*   `logs/`: Execution logs.
