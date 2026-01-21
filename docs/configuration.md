# Configuration Guide

**fairway** pipelines are driven by YAML configuration files. This allows you to define data sources, metadata extraction, validations, and enrichments without writing pipeline code.

## Root Options

| Field | Description | Default |
| :--- | :--- | :--- |
| `dataset_name` | A unique identifier for the dataset. | Required |
| `engine` | Data processing engine (`duckdb` or `pyspark`). | `duckdb` |
| `partition_by` | List of columns to partition the output Parquet files by. | `[]` |

## Data Sources

The `sources` section defines where your raw data lives and how to identify it.

```yaml
sources:
  - name: "provider_extract"
    path_pattern: "data/raw/provider_*.csv"
    naming_pattern: "provider_(?P<state>[A-Z]{2})_(?P<date>\\d{8})\\.csv"
    format: "csv"
```

### Source Expansion

fairway uses `glob` to discover files matching the `path_pattern`. Each discovered file becomes a separate task in the pipeline.

### Metadata Extraction

If a `naming_pattern` (Python regex) is provided, fairway extracts named groups from the filename and injects them as columns into the data. In the example above, a file named `provider_CT_20230101.csv` will have `state='CT'` and `date='20230101'` added to every row.

## Validations

fairway supports multi-level validations to ensure data quality.

```yaml
validations:
  level1:
    min_rows: 100
  level2:
    check_nulls:
      - "provider_id"
      - "state"
```

*   **Level 1**: Basic sanity checks (e.g., minimum row counts).
*   **Level 2**: Schema and distribution checks (e.g., checking for nulls in mandatory columns).

## Enrichment

Enable built-in enrichments like geospatial processing:

```yaml
enrichment:
  geocode: true
```

## Custom Transformations

If your data requires complex reshaping, you can point to a custom transformation script for each source.

```yaml
sources:
  - name: "sales"
    path: "data/raw/sales.csv"
    format: "csv"
    transformation: "src/transformations/sales_cleaner.py"
```

The pipeline will look for a class in the specified script that implements the transformation logic. Global transformations (under `data.transformation`) are deprecated.

## Config Auto-Discovery

When running `fairway run` without specifying `--config`, fairway will automatically discover the config file:

1. Scans the `config/` directory for `.yaml` or `.yml` files
2. Excludes `*_schema.yaml` files and `spark.yaml`
3. If exactly **one** config is found, uses it automatically
4. If **multiple** configs exist, shows an error listing them—use `--config` to specify

```bash
# Auto-discovers config/fairway.yaml (if it's the only config file)
fairway run

# Explicit config selection (required when multiple configs exist)
fairway run --config config/my_pipeline.yaml
```

## Spark Cluster Config (`spark.yaml`)

For Slurm/PySpark execution, resource settings are configured in `config/spark.yaml`:

```yaml
# config/spark.yaml
nodes: 2
cpus_per_node: 32
mem_per_node: "200G"

account: "my_account"
partition: "day"
time: "24:00:00"

dynamic_allocation:
  enabled: true
  min_executors: 5
  max_executors: 150
  initial_executors: 15
```

CLI options (e.g., `--slurm-nodes 4`) override `spark.yaml` values.

## Nextflow Profiles

Fairway uses Nextflow for execution orchestration. The `nextflow.config` file (copied to your project on `fairway init`) defines execution profiles:

| Profile | Executor | Use Case |
| :--- | :--- | :--- |
| `standard` | local | Development, testing |
| `slurm` | Slurm | HPC clusters |
| `kubernetes` | k8s | Cloud-native |
| `google_batch` | Google Batch | GCP |
| `docker` | local + Docker | Containerized local |
| `apptainer` | local + Apptainer | HPC containers |

Select a profile with `--profile`:

```bash
fairway run --profile slurm
```

