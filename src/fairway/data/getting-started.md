# Getting Started with {name}

This project was initialized with the **{engine_type}** engine.

## Directory Structure

*   `config/`: Configuration files (datasets, checks, spark resources)
*   `data/`: Data storage (raw, intermediate, final)
*   `src/transformations/`: Custom Python transformation logic
*   `logs/`: Execution logs

## Running the Pipeline

Use the Makefile shortcuts:

*   `make run`: Run locally (validation + ingestion)
*   `make run-hpc`: Submit to Slurm
