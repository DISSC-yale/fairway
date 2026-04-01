# Fairway Data Codemap

> Freshness: 2026-03-20

## Configuration Schema (YAML)

```yaml
# fairway.yaml — top-level structure
engine: duckdb | pyspark          # Execution engine
output_dir: path/to/output        # Base output directory
log_level: INFO | DEBUG | ...     # Logging verbosity
log_format: json | text           # Log output format

tables:                           # List of table definitions
  - name: string                  # SQL-safe identifier (required)
    path: string                  # Glob pattern for source files (required)
    format: csv | json | parquet | fixed_width  # File format
    output_format: parquet | delta  # Write format

    # CSV-specific
    delimiter: string             # Field separator (default: ",")
    header: bool                  # Has header row (default: true)
    skip: int                     # Skip N leading rows
    columns: [col1, col2, ...]    # Explicit column names
    types: {col: type}            # Column type overrides
    min_line_length: int          # Skip lines shorter than N chars

    # Fixed-width specific
    layout_file: path             # Column layout definition

    # Batching
    batch_strategy: bulk | partition_aware
    naming_pattern: regex         # Named groups for metadata extraction
    partition_by: [field, ...]    # Partition columns (from naming_pattern groups)

    # Processing
    preprocess: [{script: path}]  # Pre-ingestion scripts
    transformations: [...]        # Post-ingestion transforms
    enrichments: [...]            # Column additions

    # Per-table validations (inline)
    validations:
      min_rows: int               # Minimum row count
      max_rows: int               # Maximum row count
      check_nulls: [col, ...]    # Columns that must not be null
      expected_columns: [col, ...] | {col: type, ...}  # Required columns
      check_range: {col: {min: N, max: N}}  # Numeric range checks
      check_values: {col: [val, ...]}  # Allowed value sets
      check_pattern: {col: regex}  # Regex pattern checks
      check_unique: [col, ...]   # Uniqueness constraints
      check_custom: sql | [sql, ...]  # Custom SQL predicates

    # Flags
    balanced: bool                # Balance partition sizes
```

## Manifest Schema (JSON)

```json
// manifest/{table_name}.json
{
  "version": "3.0",
  "table_name": "string",
  "files": {
    "relative/path/to/file.csv": {
      "hash": "sha256:abcdef...",
      "status": "success | failed | skipped",
      "last_processed": "ISO-8601 timestamp",
      "batch_id": "table_partition-key_hash8",
      "metadata": {
        "partition_key": "value",
        ...
      }
    }
  },
  "preprocessing": {
    "relative/path/to/archive.zip": {
      "hash": "sha256:...",
      "extracted_to": "path/to/extracted/",
      "last_processed": "ISO-8601"
    }
  }
}
```

## Output Structure

### Bulk Strategy
```
output_dir/
└── {table_name}/
    └── part-0.parquet
```

### Partition-Aware Strategy
```
output_dir/
└── {table_name}/
    ├── state=CT/
    │   ├── year=2023/
    │   │   └── part-0.parquet
    │   └── year=2024/
    │       └── part-0.parquet
    └── state=NY/
        └── year=2023/
            └── part-0.parquet
```

## ValidationResult Schema

```python
ValidationResult(
    passed: bool,           # True if no errors (warnings don't block)
    errors: [               # Blocking findings
        {column, check, message, severity, failed_count, total_count}
    ],
    warnings: [             # Non-blocking findings (threshold-downgraded)
        {column, check, message, severity, failed_count, total_count, threshold}
    ]
)
```

## Supported Input Formats

| Format | Engine Support | Key Options |
|--------|---------------|-------------|
| CSV | DuckDB, PySpark | delimiter, header, skip, columns, types, min_line_length |
| JSON | DuckDB, PySpark | (auto-inferred schema) |
| Parquet | DuckDB, PySpark | (pass-through, schema preserved) |
| Fixed-Width | DuckDB | layout_file (column positions) |

## Supported Output Formats

| Format | Engine | Notes |
|--------|--------|-------|
| Parquet | Both | Default, columnar, compressed |
| Delta | PySpark only | ACID transactions, time travel |

## Data Flow Through Pipeline Stages

```
Source Files (CSV/JSON/Parquet/FW)
  │
  ▼ [Preprocess] — unzip, custom scripts, caching
  │
  ▼ [Manifest Check] — SHA-256 hash comparison, skip unchanged
  │
  ▼ [Engine.ingest()] — format-specific read → in-memory
  │
  ▼ [Validate] — Validator.run_all() → ValidationResult (errors/warnings with thresholds)
  │
  ▼ [Transform] — user-defined transformations via registry
  │
  ▼ [Enrich] — geospatial, computed columns
  │
  ▼ [Write] — Parquet/Delta to output_dir
  │
  ▼ [Manifest Update] — record hash, status, timestamp
  │
  ▼ [Summarize] — row counts, column stats, null rates
  │
  ▼ [Export] — optional Redivis upload
```

## Type System

DuckDB type mappings (config `types` field):
```
string/varchar/text  → VARCHAR
int/integer/bigint   → BIGINT
float/double/numeric → DOUBLE
bool/boolean         → BOOLEAN
date                 → DATE
timestamp            → TIMESTAMP
```

PySpark uses native type inference with optional overrides via the same config field.
