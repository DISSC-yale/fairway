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
    *   **Data Skew Protection**: Supports optional "salting" techniques to ensure balanced partitioning (opt-in via `performance.salting: true`).
    *   **File Size Control**: Configurable target file size for optimal storage and query performance.
    *   **Metadata Injection**: Efficiently adds external metadata (like state or date from filenames) to billions of rows.

### Performance Tuning

PySpark performance can be tuned via the `performance` section in your config:

```yaml
performance:
  target_file_size_mb: 128   # Target ~128MB parquet files (default)
  salting: false             # Disabled by default; enable for skewed data
  compression: snappy        # Compression codec (snappy is fastest)
```

**File Size Control**: The `target_file_size_mb` setting controls output parquet file sizes. Larger files (128-256MB) are better for analytical queries, while smaller files provide more parallelism for processing.

**Salting**: When enabled (`salting: true`), adds a random `salt` column to distribute data evenly across partitions. This prevents data skew but requires a full count operation. Only use when partition keys are highly skewed.

### Configuring Engines

You can switch engines in your YAML configuration:

```yaml
engine: "pyspark" # options: "duckdb", "pyspark"
```

When using `pyspark` on a cluster, ensure you run with the `--profile slurm --with-spark` flags to manage the Spark cluster lifecycle.

### Slurm Cluster Sizing

When running on Slurm, you can control the physical size of the Spark cluster using CLI arguments.

**Node Allocation (Physical Layer)**:

| CLI Argument | Default | Internal Fallback | Description |
| :--- | :--- | :--- | :--- |
| `--slurm-nodes` | `1` | `2` | Number of compute nodes to allocate for Spark workers |
| `--slurm-cpus` | `4` | `32` | CPUs requested per node |
| `--slurm-mem` | `16G` | `200G` | Memory requested per node |
| `--slurm-time` | `24:00:00` | `24:00:00` | Max duration of the cluster job |

**Example**:
```bash
python src/cli.py run --slurm --with-spark \
    --slurm-nodes 5 \
    --slurm-mem 100G \
    --slurm-cpus 32 ...
```

**Spark Dynamic Allocation (Logical Layer)**:

The system automatically manages the number of active executors within your allocated nodes using Spark's **Dynamic Allocation**.
*   **Min Executors**: 5
*   **Max Executors**: 150
*   **Behavior**: Spark will scale the number of executors up and down based on the workload, up to the limits of the allocated Slurm nodes or the 150 executor cap, whichever is reached first.

