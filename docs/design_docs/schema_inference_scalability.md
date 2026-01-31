# Schema Inference Scalability

## Status: Updated
## Date: 2026-01-31

## Context

Schema inference needs to work across multiple deployment environments:
- **HPC clusters** (Slurm-based, fast parallel filesystems like Lustre)
- **Cloud environments** (AWS/GCP/Azure with S3/GCS/ADLS storage)

The challenge is balancing portability, scalability, and completeness.

## Problem: Column Loss from File Sampling

**Previous approach** (random file sampling) had a critical bug:
- Randomly sampled N files for schema inference
- Columns unique to non-sampled files were **missing** from the schema
- This caused data loss during ingestion

**Example:**
```
file_a.csv: id, name, age
file_b.csv: id, name, city
file_c.csv: id, name, country

If only file_b.csv was sampled:
  Schema = {id, name, city}  ← Missing 'age' and 'country'!
```

## Decision

**Use Two-Phase Schema Inference** to guarantee completeness while maintaining performance.

See also: [RULE-117 in rules.md](../../rules.md#rule-117-schema-inference-completeness)

## Implementation

### Two-Phase Approach

```python
def infer_schema(self, path, format='csv', sample_files=50, rows_per_file=1000):
    """
    Two-phase schema inference:
    - Phase 1: Discover ALL columns from ALL files (headers only, fast)
    - Phase 2: Sample files with coverage guarantee for type inference
    """
    all_files = sorted(glob(pattern))  # Sorted for determinism

    # ========== PHASE 1: Column Discovery ==========
    # Scan ALL files, but only read headers (fast)
    all_columns = set()
    column_sources = {}  # {col: [files_with_col]}

    for f in all_files:
        cols = get_column_names_only(f)  # Headers only, no data
        all_columns.update(cols)
        for col in cols:
            column_sources.setdefault(col, []).append(f)

    # ========== PHASE 2: Type Inference with Coverage Guarantee ==========
    # Ensure at least one file per unique column is sampled
    required_files = set()
    columns_covered = set()

    for col in sorted(all_columns):
        if col not in columns_covered:
            source_file = column_sources[col][0]
            required_files.add(source_file)
            columns_covered.update(get_column_names_only(source_file))

    # Add more files up to sample_files limit
    sample = list(required_files) + remaining_files[:budget]

    # Union samples with type inference
    subqueries = [f"SELECT * FROM read_csv_auto('{f}') LIMIT {rows_per_file}"
                  for f in sample]
    query = " UNION ALL BY NAME ".join(subqueries)
```

### Key Properties

| Property | Guarantee |
|----------|-----------|
| **Completeness** | ALL columns from ALL files captured |
| **Determinism** | Same input → same output (sorted files, no random) |
| **Type Safety** | Coverage guarantee ensures real types, not defaults |
| **Performance** | Phase 1 reads headers only; Phase 2 samples limited rows |

### Type Conflict Resolution

When the same column has different types across files:

```
TYPE_HIERARCHY = ['BOOLEAN', 'INTEGER', 'BIGINT', 'DOUBLE', 'STRING']
```

Broader type always wins. STRING is the catch-all.

## Engine Support

Both engines implement the same two-phase approach:

| Engine | Phase 1 | Phase 2 |
|--------|---------|---------|
| **DuckDB** | `DESCRIBE` or `LIMIT 0` for headers | `UNION ALL BY NAME` |
| **PySpark** | Per-file schema read | Schema union with type resolution |

## Performance Characteristics

### Phase 1: Column Discovery

| Scenario | Performance |
|----------|-------------|
| 100 files on fast storage | ~100ms |
| 1,000 files on fast storage | ~1s |
| 10,000 files on fast storage | ~10s |
| 1,000 files on S3 | ~30s (parallel prefetch helps) |

**Note:** Phase 1 only reads file headers/metadata, not data rows.

### Phase 2: Type Inference

| Files Sampled | Rows per File | Total Rows | Performance |
|---------------|---------------|------------|-------------|
| 10 | 1,000 | 10,000 | ~500ms |
| 50 | 1,000 | 50,000 | ~2s |
| 100 | 1,000 | 100,000 | ~5s |

## Configuration

```yaml
# fairway.yaml
schema_inference:
  engine: duckdb  # or 'pyspark' for distributed
  sample_files: 50  # Max files for type inference (Phase 2)
  rows_per_file: 1000  # Rows per file for type inference
```

## Future Enhancements

### Parallel Phase 1 (for slow storage)

```python
# Parallel header scanning for S3/GCS
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(get_column_names_only, all_files))
```

### Caching Column Discovery

For datasets that rarely change structure:
```yaml
schema_inference:
  cache_columns: true
  cache_ttl: 3600  # seconds
```

## Related

- [RULE-117: Schema Inference Completeness](../../rules.md)
- [Engine Abstraction Refactor](engine_abstraction_refactor.md)
- [Flexible Ingestion RFC](flexible_ingestion_rfc.md)

## References

- DuckDB CSV Auto-detection: https://duckdb.org/docs/data/csv/auto_detection
- DuckDB DESCRIBE: https://duckdb.org/docs/sql/statements/describe
- Spark CSV Schema Inference: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
