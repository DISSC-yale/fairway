# Implementation Plan - Iceberg Support

This document outlines the changes required to support Apache Iceberg as an output format for `fairway`.

## Complexity Assessment
**Level: Medium**

Supporting Iceberg introduces "Table" semantics into a pipeline that is currently heavily "File-centric".
*   **Dependencies**: Requires adding `pyiceberg` for local catalog and table management.
*   **Pipeline Logic**: The current `overwrite` logic (deleting directories) must be replaced with `INSERT OVERWRITE` or `REPLACE TABLE` semantics provided by Iceberg.
*   **Downstream**: `RedivisExporter` currently expects files. It will need to be adapted to export a snapshot of the Iceberg table to Parquet before uploading.

## Proposed Changes

### 1. Dependencies
*   Add `pyiceberg` to `requirements.txt` / `pyproject.toml`.
*   Ensure `duckdb` version is compatible (current 1.4.3 is sufficient, but requires `install iceberg`).

### 2. Configuration
Update `config.yaml` to support `iceberg` specific settings.

```yaml
storage:
  type: "iceberg" # vs "files" (default)
  catalog:
    type: "sql" # or "glue", "rest", "hive"
    uri: "sqlite:///path/to/catalog.db" # Example for local
    warehouse: "data/iceberg_warehouse"
```

### 3. Engine Refactor
*   Modify `DuckDBEngine` or create `IcebergEngine`.
*   **Write Path**: Instead of `COPY ... TO ...`, use `pyiceberg` to create the table schema if not exists, then use DuckDB to write (via Arrow or experimental Iceberg writes if configured with an external catalog).
    *   *Alternative*: Use `pyiceberg` to write data directly (slower than DuckDB) OR use DuckDB's `experimental_httpfs` + Iceberg extension to write to the catalog if possible.
    *   *Recommended*: Use `pyiceberg` to manage metadata/create table. Convert DuckDB result to Arrow `df.to_arrow_table()` and use `pyiceberg_table.append(arrow_table)`.

### 4. Pipeline Updates
*   `src/pipeline.py` needs to branch logic based on output format.
    *   If `Parquet`: Keep existing `ingest_csv` -> `query` -> `to_parquet` flow.
    *   If `Iceberg`: `ingest_csv` (to temp) -> `query` -> `write_to_iceberg` (append/replace).

### 5. Redivis Export
*   Iceberg tables are directories of files + metadata. Redivis expects a file upload.
*   Update `RedivisExporter` to accept a DuckDB connection and a table name.
*   Exporter will execute `COPY (SELECT * FROM iceberg_table) TO 'temp.parquet' (FORMAT PARQUET)` and then upload `temp.parquet`.

## Verification Plan

### Automated Tests
1.  Create `tests/test_iceberg_ingestion.py`.
2.  Test full flow: CSV -> Iceberg Table (verify via `pyiceberg` reading) -> Redivis Export (mocked).

### Manual Verification
1.  Run `fairway` with `format: iceberg`.
2.  Inspect `data/final` (or warehouse dir) to see `metadata/` and `data/` folders structure.
3.  Use `duckdb` CLI to query the generated Iceberg table.
