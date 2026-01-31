# Configuration Guide

**fairway** pipelines are driven by YAML configuration files. This allows you to define data sources, metadata extraction, validations, and enrichments without writing pipeline code.

## Root Options

| Field | Description | Default |
| :--- | :--- | :--- |
| `dataset_name` | A unique identifier for the dataset. | Required |
| `engine` | Data processing engine (`duckdb` or `pyspark`). | `duckdb` |
| `partition_by` | List of columns to partition the output Parquet files by. | `[]` |
| `temp_location` | Global temporary location for file writes. | `None` |

## Storage

The `storage` section defines where processed data is written.

```yaml
storage:
  raw_dir: "data/raw"
  intermediate_dir: "data/intermediate"
  final_dir: "data/final"
  # Optional: Fast scratch storage for intermediate writes (HPC clusters)
  scratch_dir: "/scratch/$USER/fairway"
```

| Field | Description | Default |
| :--- | :--- | :--- |
| `raw_dir` | Directory for raw input files. | Required |
| `intermediate_dir` | Directory for intermediate processed files. | Required |
| `final_dir` | Directory for final output files. | Required |
| `scratch_dir` | Fast scratch storage for intermediate writes. Supports environment variables like `$USER`. Useful on HPC clusters where `intermediate_dir` may be on slow or quota-limited storage. | `None` |

## Performance

The `performance` section controls optimization settings for the pipeline.

```yaml
performance:
  target_rows: 500000           # Rows per partition (for salting calculation)
  target_file_size_mb: 128      # Target parquet file size in MB
  max_records_per_file: 1000000 # Direct control (overrides heuristic)
  salting: false                # Enable partition salting for data skew prevention
  compression: snappy           # Parquet compression codec
```

| Field | Description | Default |
| :--- | :--- | :--- |
| `target_rows` | Target number of rows per partition. Used when salting is enabled to calculate the number of salt buckets. | `500000` |
| `target_file_size_mb` | Target size for output Parquet files in megabytes. Uses a heuristic (~8000 rows/MB) to estimate `maxRecordsPerFile`. Actual file size varies by data characteristics. | `128` |
| `max_records_per_file` | Direct control over Spark's `maxRecordsPerFile` option. Overrides the `target_file_size_mb` heuristic. Use when files are consistently too small or too large. | `None` |
| `salting` | Enable partition salting to prevent data skew. When enabled, adds a `salt` column to distribute data evenly across partitions. Only applies when `partition_by` is set. | `false` |
| `compression` | Parquet compression codec. Options: `snappy`, `gzip`, `zstd`. | `snappy` |

### File Size Tuning

The `target_file_size_mb` option uses a heuristic to estimate how many rows fit in a given file size. This varies significantly based on:

- **Column count**: Wide tables (100+ columns) → ~500-2,000 rows/MB
- **Narrow tables**: 10 columns → ~5,000-20,000 rows/MB
- **Data types**: Strings compress less than integers

If your output files are consistently the wrong size, use `max_records_per_file` for direct control:

```yaml
performance:
  # If files are too small (e.g., 20MB instead of 128MB), increase this:
  max_records_per_file: 2000000
```

### When to Enable Salting

Salting is useful when:
- Your data has highly skewed partition keys (e.g., 90% of data in one partition)
- You're experiencing slow writes due to uneven data distribution
- You need to balance load across Spark executors

**Note:** Salting adds a `salt` column to your output data and requires a full data count operation, which can be expensive for very large datasets.

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

