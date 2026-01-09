# Compute Engines

**fairway** supports multiple compute engines, allowing you to choose the best tool for your data size and environment.

## DuckDB Engine

The **DuckDB** engine is the default for local development and smaller datasets.

*   **When to use**: Testing, local development, datasets that fit on a single machine.
*   **Key Features**:
    *   Zero-dependency (embedded database).
    *   Highly optimized for analytical queries on Parquet files.
    *   Supports SQL-based data manipulation.

## PySpark Engine

The **PySpark** engine is designed for large-scale distributed processing.

*   **When to use**: Large datasets (TB+), high-compute tasks, running on Slurm/YCRC clusters.
*   **Key Features**:
    *   **Distributed Processing**: Scales across multiple nodes.
    *   **Resource Management**: Integrates with Slurm to dynamically provision clusters.
    *   **Data Skew Protection**: Implements "salting" techniques to ensure balanced partitioning.
    *   **Metadata Injection**: Efficiently adds external metadata (like state or date from filenames) to billions of rows.

### Configuring Engines

You can switch engines in your YAML configuration:

```yaml
engine: "pyspark" # options: "duckdb", "pyspark"
```

When using `pyspark` on a cluster, ensure you run with the `--profile slurm --with-spark` flags to manage the Spark cluster lifecycle.
