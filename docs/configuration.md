# Configuration Guide

**fairway** pipelines are driven by YAML configuration files. This allows you to define data sources, metadata extraction, validations, and enrichments without writing pipeline code.

## Root Options

| Field | Description | Default |
| :--- | :--- | :--- |
| `dataset_name` | A unique identifier for the dataset. | Required |
| `engine` | Data processing engine (`duckdb` or `pyspark`). | `duckdb` |
| `partition_by` | List of columns to partition the output Parquet files by. | `[]` |

## Data Sources

The `sources` section defines where your raw data lives and how to identify it.

```yaml
sources:
  - name: "provider_extract"
    path_pattern: "data/raw/provider_*.csv"
    naming_pattern: "provider_(?P<state>[A-Z]{2})_(?P<date>\\d{8})\\.csv"
    format: "csv"
```

### Source Expansion

fairway uses `glob` to discover files matching the `path_pattern`. Each discovered file becomes a separate task in the pipeline.

### Metadata Extraction

If a `naming_pattern` (Python regex) is provided, fairway extracts named groups from the filename and injects them as columns into the data. In the example above, a file named `provider_CT_20230101.csv` will have `state='CT'` and `date='20230101'` added to every row.

## Validations

fairway supports multi-level validations to ensure data quality.

```yaml
validations:
  level1:
    min_rows: 100
  level2:
    check_nulls:
      - "provider_id"
      - "state"
```

*   **Level 1**: Basic sanity checks (e.g., minimum row counts).
*   **Level 2**: Schema and distribution checks (e.g., checking for nulls in mandatory columns).

## Enrichment

Enable built-in enrichments like geospatial processing:

```yaml
enrichment:
  geocode: true
```

## Custom Transformations

If your data requires complex reshaping, you can point to a custom transformation script:

```yaml
data:
  transformation: "my_custom_transform"
```

The pipeline will look for a class in `src/transformations/my_custom_transform.py` that implements the transformation logic.
