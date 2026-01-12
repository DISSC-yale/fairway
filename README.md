# Fairway

Fairway is a portable data ingestion framework designed to streamline the processing, transformation, and management of centralized research data. It provides a robust and scalable solution for handling data pipelines, with built-in support for multiple execution environments including local machines and Slurm clusters.

## Installation

To install Fairway, ensure you have Python 3.10+ installed.

1. Install directly from GitHub:
   ```bash
   # Core installation (lightweight)
   pip install "git+https://github.com/DISSC-yale/fairway.git"

   # With DuckDB support
   pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[duckdb]"

   # With PySpark support
   pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[spark]"

   # For generating test data only
   pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[test-data-gen]"
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
Fairway uses YAML configuration files to define data sources and processing steps.

Key files created:
- `config/fairway.yaml`: Main pipeline configuration
- `config/spark.yaml`: Spark cluster settings (nodes, memory, etc.)
- `nextflow.config`: Execution profiles (Slurm, Kubernetes, etc.)

Key configuration elements include:
- **Project Metadata**: Name, description, and owner.
- **Engine**: Execution backend (e.g., `duckdb`, `pyspark`).
- **Data Source**: Path and format of input data.
- **Transformations**: Steps to clean or modify data.
- **Sink**: Output destination and format.

### 3. Run the Pipeline

**Local Execution**
Run the pipeline on your local machine (auto-discovers config from `config/` folder):
```bash
fairway run

# Or specify a config explicitly
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

## Examples

### Generate Test Data

Quickly create sample datasets for testing your pipeline:

```bash
# Generate a small partitioned CSV dataset
fairway generate-data --size small --partitioned

# Generate a large Parquet dataset (non-partitioned)
fairway generate-data --size large --no-partitioned --format parquet
```

### Auto-Generate Schema

Infer schema from existing data files or partitioned directories:

```bash
# Generate schema from a CSV file
fairway generate-schema data/raw/sales.csv

# Generate schema from a partitioned directory
fairway generate-schema data/raw/events/ --output config/events_schema.yaml
```

This outputs a YAML schema file that can be used in your config.

### Sample Configuration

A minimal `config.yaml` for processing data:

```yaml
dataset_name: "sales_data"
engine: "duckdb"

storage:
  raw_dir: "data/raw"
  final_dir: "data/final"

sources:
  - name: "sales"
    path: "data/raw/sales.csv"
    format: "csv"
    schema:
      id: "int"
      date: "date"
      amount: "double"

validations:
  level1:
    check_column_count: true
    min_rows: 10
```

See `config/example_config.yaml` for a complete configuration example with all options.

### Running the Pipeline

```bash
# Local execution with DuckDB
fairway run --config config/my_config.yaml

# Slurm cluster with PySpark
fairway run --config config/my_config.yaml --profile slurm --with-spark --account my_account

# Customize Slurm resources
fairway run --config config/my_config.yaml --slurm \
  --cpus 8 \
  --mem 32G \
  --time 04:00:00 \
  --nodes 4
```

## Development

A `Makefile` is provided to streamline common development tasks:

```bash
# Install the package in editable mode
make install

# Run tests
make test

# Generate small test data
make generate-data

# Clean build artifacts
make clean

# Show all available commands
make help
```

