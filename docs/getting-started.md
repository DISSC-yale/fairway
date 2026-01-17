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

Fairway creates a `Makefile` to simplify execution.

### Local Development
To run the pipeline locally (on your laptop):

```bash
make run
# Equivalent to: nextflow run main.nf -profile standard
```

### HPC Execution (Slurm)
To submit the pipeline to a Slurm cluster:

```bash
make run-hpc
# Equivalent to: nextflow run main.nf -profile slurm
```

### Manual Execution (Debugging)
To run the pipeline logic directly (without Nextflow orchestration):

```bash
fairway run --config config/fairway.yaml
```
*Note: This runs the "Worker" logic directly. Use this for fast feedback on Python errors.*

## Directory Structure
*   `config/`: Configuration files.
*   `data/`: Data storage.
*   `src/`: Custom code.
*   `logs/`: Execution logs.
