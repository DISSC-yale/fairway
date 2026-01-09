# Getting Started

This guide will help you set up **fairway** and run your first data ingestion pipeline.

## Installation

### Prerequisites

*   Python 3.9+
*   [Nextflow](https://www.nextflow.io/docs/latest/getstarted.html#installation)
*   [Apptainer](https://apptainer.org/docs/user/main/quick_start.html#installation) (for containerized execution)

### Setup

Clone the repository and install the package in editable mode:

```bash
git clone https://github.com/DISSC/fairway.git
cd fairway
pip install -e .
```

## Initializing a Project

To start a new ingestion project, use the `init` command:

```bash
fairway init my-data-project
cd my-data-project
```

This creates the recommended directory structure:
*   `config/`: Store your YAML configuration files.
*   `data/`: Local storage for raw, intermediate, and final data.
*   `src/transformations/`: Custom Python scripts for data reshaping.
*   `docs/`: Project-specific documentation.
*   `logs/`: Slurm and Nextflow execution logs.

## Running a Pipeline

### Local Execution (DuckDB)

For small datasets or testing, run fairway locally using the default `standard` profile which uses DuckDB:

```bash
fairway run --config config/discovery_test.yaml
```

### Slurm Execution (PySpark)

To run on a Slurm cluster with PySpark support:

```bash
fairway run --config config/discovery_test.yaml --profile slurm --with-spark
```

This command:
1.  Provisions a Spark-on-Slurm cluster.
2.  Submits the Nextflow pipeline to the Slurm controller.
3.  Executes the ingestion using the PySpark engine.
