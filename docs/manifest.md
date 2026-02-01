# Manifest & Caching

Fairway uses a manifest system to track processed files and avoid redundant work. This enables incremental processing where only changed files are reprocessed.

## Directory Structure

```
project/
├── manifest/
│   ├── _global.json      # Shared state (archive extractions)
│   ├── sales.json        # Per-table: files, preprocessing, schema
│   └── customers.json
├── schema/
│   ├── sales.yaml        # Inferred schema for each table
│   └── customers.yaml
```

## Per-Table Manifests

Each table gets its own manifest file at `manifest/<table_name>.json`. This provides:

- **Isolation**: No merge conflicts when processing tables in parallel
- **Clarity**: Easy to inspect state for a specific table
- **Granularity**: Can selectively reset a single table's state

### Table Manifest Structure

```json
{
  "version": "3.0",
  "table_name": "sales",
  "files": {
    "2023/data.csv": {
      "hash": "mtime:1234567890_size:1024",
      "last_processed": "2024-01-15T10:30:00",
      "status": "success",
      "metadata": {"row_count": 50000}
    }
  },
  "preprocessing": {
    "archive.zip": {
      "file_hash": "mtime:..._size:...",
      "preprocessed_path": "/tmp/extracted/",
      "action": "unzip"
    }
  },
  "schema": {
    "generated_at": "2024-01-15T10:00:00",
    "output_path": "schema/sales.yaml",
    "combined_hash": "abc123..."
  }
}
```

## Global Manifest

Shared state that spans multiple tables is stored in `manifest/_global.json`:

```json
{
  "version": "3.0",
  "extractions": {
    "archive:/path/to/shared_data.zip": {
      "extracted_dir": ".fairway_cache/archives/shared_data_zip_a1b2c3d4",
      "archive_hash": "mtime:..._size:...",
      "extracted_at": "2024-01-15T09:00:00",
      "used_by_tables": ["sales", "customers"]
    }
  }
}
```

This allows multiple tables to share the same archive extraction without redundant unzipping.

## Change Detection

Fairway uses fast change detection based on file metadata:

1. **Fast check (default)**: Uses `mtime + size` signature
2. **Full hash**: SHA-256 of file contents (slower, more reliable)

Files are only reprocessed when their hash changes.

## Resetting State

To force reprocessing:

```bash
# Reset a single table
rm manifest/sales.json

# Reset all tables
rm -rf manifest/

# Reset extractions only
rm manifest/_global.json
```

## Schema Tracking

When using `fairway discover-schema`, schema staleness is tracked per-table. A schema is considered stale when:

- No previous schema exists
- Source file hashes have changed since last generation

This prevents unnecessary schema regeneration on repeated runs.
