# Zip Handling Test Fixtures

This directory contains test data for zip file preprocessing scenarios.

## Test Files

| File | Contents | Purpose |
|------|----------|---------|
| `single_file.zip` | `data.csv` | Basic single file extraction |
| `multi_file_same_schema.zip` | `part1.csv`, `part2.csv`, `part3.csv` | Multiple files, same schema |
| `multi_file_diff_schema.zip` | `file_a.csv` (age), `file_b.csv` (city) | Schema merge across files |
| `multi_table.zip` | `sales_*.csv`, `customers_*.csv` | Multiple logical tables |
| `nested_dirs.zip` | `2024/01/data.csv`, `2024/02/data.csv`, etc. | Nested directory structure |

## Current Zip Handling Behavior

In Fairway config:
- `path` matches zip files
- `format` specifies what's INSIDE the zip (e.g., `csv`)
- `include` pattern filters files inside the zip

## Multi-Table Workaround

When a single zip contains files for multiple tables, use multiple source entries:

```yaml
sources:
  - name: "sales"
    path: "data.zip"
    format: "csv"
    preprocess:
      action: "unzip"
      include: "sales_*.csv"

  - name: "customers"
    path: "data.zip"
    format: "csv"
    preprocess:
      action: "unzip"
      include: "customers_*.csv"
```

## Notes

- Zip extraction is handled by the preprocessing step
- `recursiveFileLookup` handles nested directories
- A better multi-table solution is planned for Config Redesign
