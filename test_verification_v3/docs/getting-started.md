# Getting Started with test_verification_v3

## Prerequisites

- Python 3.9+
- fairway installed (`pip install git+https://github.com/DISSC-yale/fairway.git`)

## Generating Test Data

Create sample data to test your pipeline:

```bash
# Small partitioned CSV dataset
fairway generate-data --size small --partitioned

# Large Parquet dataset
fairway generate-data --size large --format parquet
```

## Schema Inference

Auto-generate schema from your data:

```bash
# From a single file
fairway generate-schema data/raw/example.csv

# From a partitioned directory
fairway generate-schema data/raw/partitioned_data/
```

## Configuration

Your pipeline is configured in `config/fairway.yaml`:

```yaml
dataset_name: "test_verification_v3"
engine: "duckdb"

sources:
  - name: "my_source"
    path: "data/raw/my_data.csv"
    format: "csv"
    schema:
      id: "int"
      value: "double"

validations:
  level1:
    min_rows: 100
    check_column_count: true

enrichment:
  geocode: true
```

## Running Your Pipeline

### Local (DuckDB)
```bash
fairway run --config config/fairway.yaml
```

### Slurm Cluster (PySpark)
```bash
fairway run --config config/fairway.yaml --profile slurm --with-spark --account YOUR_ACCOUNT
```

### Custom Slurm Resources
```bash
fairway run --config config/fairway.yaml --slurm \
  --cpus 8 --mem 32G --time 04:00:00 --nodes 4
```
