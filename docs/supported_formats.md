# Supported Formats

Fairway supports ingestion of the following file formats:

| Format | Extension | Engines | Config Key |
|--------|-----------|---------|------------|
| CSV | `.csv` | DuckDB, PySpark | `csv` |
| TSV | `.tsv`, `.tab` | DuckDB, PySpark | `tsv`, `tab` |
| JSON | `.json`, `.jsonl` | DuckDB, PySpark | `json` |
| Parquet | `.parquet` | DuckDB, PySpark | `parquet` |
| Fixed-Width | `.txt`, `.dat` | DuckDB, PySpark | `fixed_width` |

## CSV

**Default format** when not specified.

```yaml
tables:
  - name: "sales_data"
    path: "data/raw/sales.csv"
    format: "csv"
```

**Features:**
- Automatic type inference
- Header detection
- Configurable delimiter via `read_options`

## TSV / Tab-Separated

Tab-delimited files, common in bioinformatics and legacy systems.

```yaml
tables:
  - name: "gene_data"
    path: "data/raw/*.tsv"
    format: "tsv"
```

## JSON / JSONL

Supports standard JSON arrays or newline-delimited JSON (JSONL).

```yaml
tables:
  - name: "clickstream"
    path: "data/raw/clicks.jsonl"
    format: "json"
```

## Parquet

Efficient pass-through ingestion for pre-processed data.

```yaml
tables:
  - name: "preprocessed"
    path: "data/staged/*.parquet"
    format: "parquet"
```

## Fixed-Width

Text files where columns are defined by character positions (no delimiters). Common in mainframe/legacy data exports.

```yaml
tables:
  - name: "legacy_records"
    path: "data/raw/*.txt"
    format: "fixed_width"
    fixed_width_spec: "specs/legacy_spec.yaml"
```

**Requires** a spec file defining column positions. See [Fixed-Width Format](fixed_width.md) for details.

**Spec file format:**
```yaml
columns:
  - name: id
    start: 0        # 0-indexed position
    length: 5
    type: INTEGER
    trim: true      # Strip whitespace (optional)
  - name: description
    start: 5
    length: 30
    type: VARCHAR
```

## Read Options

Pass engine-specific options via `read_options`:

```yaml
tables:
  - name: "pipe_delimited"
    path: "data/*.csv"
    format: "csv"
    read_options:
      delim: "|"
      header: false
      skip: 1
```

## Adding New Formats

New formats require (per RULE-116):

1. Test fixtures in `tests/fixtures/formats/<format>/`
2. Engine implementation in `engines/duckdb_engine.py` and `engines/pyspark_engine.py`
3. Tests in `tests/test_fixed_width.py` (or similar)
4. Documentation update
