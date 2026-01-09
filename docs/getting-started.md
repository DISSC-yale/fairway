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
    *   `fairway.yaml`: Main pipeline config
    *   `spark.yaml`: Spark cluster settings (nodes, CPUs, memory)
*   `data/`: Local storage for raw, intermediate, and final data.
*   `src/transformations/`: Custom Python scripts for data reshaping.
*   `docs/`: Project-specific documentation.
*   `logs/`: Slurm and Nextflow execution logs.
*   `nextflow.config`: Execution profiles (edit to customize Slurm, Kubernetes, etc.)

## Generating Test Data

Before building your first pipeline, generate sample data to work with:

```bash
# Generate a small partitioned CSV dataset
fairway generate-data --size small --partitioned

# Generate a large Parquet dataset
fairway generate-data --size large --format parquet
```

The generated data will appear in `data/raw/`.

## Inferring Schema Automatically

Use `generate-schema` to auto-detect column types from your data:

```bash
# From a single file
fairway generate-schema data/raw/sales.csv

# From a partitioned directory (detects partition columns automatically)
fairway generate-schema data/raw/events/

# Specify custom output location
fairway generate-schema data/raw/users.parquet --output config/users_schema.yaml
```

This creates a YAML schema file like:

```yaml
name: sales
columns:
  id: INTEGER
  date: DATE
  amount: DOUBLE
  category: VARCHAR
```

## Creating Your Configuration

Create a config file at `config/my_pipeline.yaml`:

```yaml
dataset_name: "my_dataset"
engine: "duckdb"  # or "pyspark" for large-scale processing

storage:
  raw_dir: "data/raw"
  intermediate_dir: "data/intermediate"
  final_dir: "data/final"

sources:
  - name: "transactions"
    path: "data/raw/transactions.csv"
    format: "csv"
    schema:
      id: "int"
      timestamp: "timestamp"
      amount: "double"

validations:
  level1:
    check_column_count: true
    min_rows: 100
  level2:
    strict_schema: true
    check_nulls: ["id", "timestamp"]

enrichment:
  geocode: true
  h3_index: true
```

See [Configuration Reference](configuration.md) for all available options.

## Running a Pipeline

### Local Execution (DuckDB)

For small datasets or testing, run fairway locally using the default `standard` profile which uses DuckDB:

```bash
# Auto-discovers config from config/ folder
fairway run

# Or specify explicitly
fairway run --config config/my_pipeline.yaml
```

### Slurm Execution (PySpark)

To run on a Slurm cluster with PySpark support:

```bash
fairway run --config config/my_pipeline.yaml --profile slurm --with-spark
```

This command:
1.  Provisions a Spark-on-Slurm cluster.
2.  Submits the Nextflow pipeline to the Slurm controller.
3.  Executes the ingestion using the PySpark engine.

### Customizing Slurm Resources

```bash
fairway run --config config/my_pipeline.yaml --slurm \
  --account my_account \
  --cpus 8 \
  --mem 32G \
  --time 04:00:00 \
  --nodes 4
```

## Next Steps

- [Configuration Reference](configuration.md) - All config options
- [Transformations](transformations.md) - Custom data transformations
- [Engines](engines.md) - DuckDB vs PySpark comparison
- [Validations](validations.md) - Data quality checks
