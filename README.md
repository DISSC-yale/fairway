# Fairway

Fairway is a portable data ingestion framework designed to streamline the processing, transformation, and management of centralized research data. It provides a robust and scalable solution for handling data pipelines, with built-in support for multiple execution environments including local machines and Slurm clusters.

## Installation

To install Fairway, ensure you have Python 3.10+ installed.

1. Install directly from GitHub:
   ```bash
   pip install git+https://github.com/DISSC-yale/fairway.git
   ```

## Quick Start

### 1. Initialize a Project
Create a new Fairway project with the required directory structure:

```bash
fairway init my_new_project
cd my_new_project
```

This will create folders for your data, configuration, logs, and transformations.

### 2. Configure Your Pipeline
Fairway uses YAML configuration files to define data sources and processing steps. A sample configuration is available at `config/example_config.yaml`.

Key configuration elements include:
- **Project Metadata**: Name, description, and owner.
- **Engine**: Execution backend (e.g., `duckdb`, `pyspark`).
- **Data Source**: Path and format of input data.
- **Transformations**: Steps to clean or modify data.
- **Sink**: Output destination and format.

### 3. Run the Pipeline

**Local Execution**
Run the pipeline on your local machine:
```bash
fairway run --config config/my_config.yaml
```

**Slurm Cluster Execution**
Run the pipeline as a job on a Slurm cluster:
```bash
fairway run --config config/my_config.yaml --profile slurm --slurm --account my_account
```
*Note: The `--slurm` flag submits a controller job. The `--profile slurm` tells Nextflow/Fairway to use Slurm executors.*

## Documentation

For comprehensive guides and API details, please refer to the documentation in the `docs/` directory:

- **[Getting Started](docs/getting-started.md)**: Extended guide on setting up and running your first pipeline.
- **[Configuration](docs/configuration.md)**: detailed reference for `config.yaml` options.
- **[Architecture](docs/architecture.md)**: High-level overview of Fairway's design.
- **[Transformations](docs/transformations.md)**: How to write and use data transformations.
- **[Validations](docs/validations.md)**: ensuring data quality.

## Example

Check `config/example_config.yaml` for a working configuration example.
