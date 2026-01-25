# Schema Inference Scalability

## Status: Accepted
## Date: 2025-01-24

## Context

Schema inference needs to work across multiple deployment environments:
- **HPC clusters** (Slurm-based, fast parallel filesystems like Lustre)
- **Cloud environments** (AWS/GCP/Azure with S3/GCS/ADLS storage)

The challenge is balancing portability, scalability, and simplicity.

## Decision

**Use DuckDB for schema inference** as the default, portable solution.

### Rationale

1. **Portability**: DuckDB runs identically on HPC and cloud without cluster provisioning
2. **Simplicity**: No Spark cluster setup, SASL configuration, or environment-specific code
3. **Sufficient for typical use case**: Fast storage (local SSD, Lustre, mounted volumes)

### Implementation

```python
def infer_schema(self, path, format='csv', sampling_ratio=0.1, sample_files=50, rows_per_file=1000):
    """
    Infers schema using DuckDB with random sampling across files.

    1. Discover all files matching pattern
    2. Randomly sample subset of files (catches type variations)
    3. Sample rows from each file, union together
    4. Infer types from combined sample
    """
    all_files = glob(pattern)
    sampled_files = random.sample(all_files, num_to_sample)

    subqueries = [f"SELECT * FROM read_csv_auto('{f}') LIMIT {rows_per_file}"
                  for f in sampled_files]
    query = " UNION ALL BY NAME ".join(subqueries)

    rel = self.con.sql(query)
    # ... type mapping
```

**Defaults**: 50 files × 1000 rows = up to 50,000 rows sampled across dataset.

## Current Limitations

### Scalability with 1000s of Files on Slow Storage

| Scenario | Performance |
|----------|-------------|
| 1000s files on fast storage (Lustre, local SSD) | ✅ Good |
| 1000s files on slow storage (S3, NFS over WAN) | ⚠️ May be slow |

**Root cause**: DuckDB opens files sequentially. With 1000s of files on high-latency storage, file open overhead accumulates.

### When This Becomes a Problem

- Files stored on S3/GCS without local caching
- Network-mounted storage with high latency
- Very large number of files (10,000+)

## Future Enhancement: Distributed Schema Inference

If scalability becomes an issue, consider these approaches:

### Option 1: Spark-based Schema Inference (Parallel I/O)

```python
# Spark samples across all files in parallel
reader = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('samplingRatio', 0.1)  # 10% from ALL files, distributed
df = reader.load('s3://bucket/data/*.csv')
```

**Pros**: True parallel I/O across executors
**Cons**: Requires Spark cluster (environment-specific setup)

### Option 2: Cloud-Native Schema Discovery

- **AWS**: Glue Crawlers, Athena
- **GCP**: BigQuery schema auto-detection
- **Azure**: Synapse schema inference

**Pros**: Managed, scalable, no cluster management
**Cons**: Cloud-specific, vendor lock-in

### Option 3: Parallel DuckDB with File Batching

```python
# Sample subset of files, process in parallel threads
import concurrent.futures

def sample_file(f):
    return duckdb.sql(f"SELECT * FROM read_csv_auto('{f}') USING SAMPLE 1000 ROWS")

files = glob.glob('data/*.csv')
sampled_files = random.sample(files, min(100, len(files)))

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(sample_file, sampled_files))

# Merge schemas from samples
```

**Pros**: Portable, parallel I/O within single node
**Cons**: Still limited by single-node network bandwidth

## Configuration

Add config option to select inference strategy:

```yaml
# fairway.yaml
schema_inference:
  engine: duckdb  # or 'spark' for distributed
  sampling_ratio: 0.1
  # Future: strategy: 'parallel_batch' for Option 3
```

## Related

- [Engine Abstraction Refactor](engine_abstraction_refactor.md)
- [Flexible Ingestion RFC](flexible_ingestion_rfc.md)

## References

- DuckDB CSV Auto-detection: https://duckdb.org/docs/data/csv/auto_detection
- DuckDB Sampling: https://duckdb.org/docs/sql/samples
- Spark CSV Schema Inference: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
