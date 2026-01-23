# Schema Evolution Guide

Fairway provides flexible options for handling schema changes as your data evolves. This guide explains the two primary patterns: **Strict Schema Enforcement** and **Delta Lake Evolution**.

## 1. Strict Schema Enforcement (The "Contract" Approach)

This approach prioritizes data integrity and predictability. It ensures that all output files have an identical schema, making reads extremely fast (no `mergeSchema` required).

### Logic
- **Missing Columns**: If the source is missing a column defined in the schema, it is filled with `null`.
- **Extra Columns**: If the source has columns NOT in the schema, the pipeline **FAILS** with a `Data Integrity Error` ([RULE-115]). This prevents silent data loss.
- **Type Casting**: Columns are cast to the target types defined in the schema.

### Configuration
To enable, simply define a `schema` in your `sources` config:

```yaml
sources:
  - name: "patient_data"
    path: "data/raw/patients/*.csv"
    schema:
      id: "integer"
      name: "string"
      admit_date: "timestamp"
    # output_format defaults to 'parquet'
```

## 2. Delta Lake Evolution (The "Lakehouse" Approach)

This approach leverages [Delta Lake](https://delta.io/) to manage schema evolution. The Delta transaction log tracks schema changes over time, allowing different Parquet files to have different schemas while presenting a unified view to readers.

### Logic
- **New Columns**: Automatically added to the table schema (`mergeSchema=true`).
- **Missing Columns**: Handled by Delta (read as null for old files).
- **Time Travel**: You can query previous versions of the table.

### Configuration
To enable, set the `output_format` to `delta`:

```yaml
dataset_name: "hospital_admissions"
engine: "pyspark" # Required for Delta

storage:
  intermediate_dir: "data/intermediate"
  final_dir: "data/delta_lake"
  format: "delta"  # <--- Global setting

sources:
  - name: "admissions"
    path: "data/raw/*.csv"
    # format: "delta" (optional per-source override)
```

## Comparison

| Feature | Strict Schema (Parquet) | Delta Lake |
| :--- | :--- | :--- |
| **Schema enforcement** | Strict (Fail on extra) | Flexible (Evolves) |
| **Read Performance** | ⚡️ Fast | ⚡️ Fast (Metadata driven) |
| **Write Performance** | Slower (Validation) | Fast |
| **Dependencies** | None (Native Spark) | `delta-spark` |
| **Use Case** | Regulatory / Fixed Contract | Discovery / Bronze Layer |
