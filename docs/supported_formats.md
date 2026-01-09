# Ingestion and Supported Formats

This document outlines the current data ingestion capabilities of Fairway.

## Supported Formats

Fairway supports ingestion of the following file formats:

### CSV (`.csv`)
- **Default**: Yes
- **Engines**: DuckDB, PySpark
- **Features**: Automatic type inference, header detection.

### JSON (`.json`, `.jsonl`)
- **Engines**: DuckDB, PySpark
- **Features**: Automatic schema inference. Supports standard JSON arrays or newline-delimited JSON (JSONL).

### Parquet (`.parquet`)
- **Engines**: DuckDB, PySpark
- **Features**: Efficient pass-through ingestion. Useful for unifying pipelines where some sources are already in Parquet.

## Configuration

To specify a format for a source, use the `format` key in your `config.yaml`:

```yaml
sources:
  - name: "sales_data"
    path: "data/raw/sales.csv"
    format: "csv"
    
  - name: "clickstream"
    path: "data/raw/clicks.json"
    format: "json"
```

If `format` is omitted, it defaults to `csv`.
