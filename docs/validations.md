# Validations

Data quality is a core pillar of **fairway**. The framework provides built-in validation checks that can be configured globally or per-table.

## Config Format

Validations are declared as flat keys under `validations:`. Both global (root-level) and per-table configs are supported.

```yaml
# Global validations (apply to all tables by default)
validations:
  min_rows: 100
  check_nulls:
    - "person_id"
    - "year"

tables:
  - name: demographics
    path: "data/raw/demographics.csv"
    format: csv
    # Per-table overrides (shallow merge with global)
    validations:
      min_rows: 500
      check_nulls: ["person_id"]
      check_range:
        year: { min: 1900, max: 2030 }
        age: { min: 0, max: 120 }
      check_pattern:
        fips_code: "^\\d{5}$"
      check_values:
        state: ["CT", "MA", "NY"]
```

### Merge Behavior

Per-table validations use **shallow merge** with global defaults:

- Per-table keys **replace** the same global key entirely (not deep-merged)
- Global keys not overridden by the table are inherited
- If neither global nor table-level validations exist, no checks run

For example, if global defines `check_range` for 3 columns and a table overrides `check_range` for 1 column, only that 1 column is checked — the other 2 global columns are not included.

### Legacy Format

The older `level1`/`level2` nesting is still accepted but normalized to flat format internally:

```yaml
# Legacy format (still works, not recommended for new configs)
validations:
  level1:
    min_rows: 100
  level2:
    check_nulls: ["person_id"]
```

Mixing flat keys with `level1`/`level2` nesting raises a config error.

## Available Checks

### min_rows

Fail if the dataset has fewer rows than the threshold.

```yaml
validations:
  min_rows: 100
```

### max_rows

Fail if the dataset has more rows than the threshold. Useful as a sanity check against accidental cross-joins or duplicate ingestion.

```yaml
validations:
  max_rows: 10000000
```

### check_nulls

Fail if any of the listed columns contain null values.

```yaml
validations:
  check_nulls:
    - "person_id"
    - "year"
```

### expected_columns

Fail if the dataset is missing any of the listed columns. Optionally use `strict: true` to also fail on unexpected extra columns.

```yaml
# Basic: fail if columns are missing
validations:
  expected_columns: ["person_id", "year", "state"]

# Strict: also fail if extra columns exist
validations:
  expected_columns:
    columns: ["person_id", "year", "state"]
    strict: true
```

### check_range

Fail if values in a numeric column fall outside the specified bounds. **Null values are excluded** — they are not counted as violations.

```yaml
validations:
  check_range:
    year: { min: 1900, max: 2030 }
    age: { min: 0, max: 120 }
```

### check_values

Fail if a column contains values not in the allowed set (enum check). Null values are excluded.

```yaml
validations:
  check_values:
    state: ["CT", "MA", "NY", "CA"]
```

### check_pattern

Fail if string values in a column do not match the given regex pattern. Uses **full-string matching** (equivalent to `re.fullmatch` in Python). Null values are excluded.

```yaml
validations:
  check_pattern:
    fips_code: "^\\d{5}$"
    zip_code: "^\\d{5}(-\\d{4})?$"
```

Regex patterns are validated at config load time — invalid patterns raise a `ConfigValidationError`.

### check_unique (not yet implemented)

Full-scan uniqueness check. Raises `NotImplementedError` if configured.

### check_custom (not yet implemented)

Plugin hook for custom validation logic. Raises `NotImplementedError` if configured.

## Validation Result

Each check produces findings with:

| Field | Description |
| :--- | :--- |
| `column` | Column name (or `null` for row-level checks like `min_rows`) |
| `check` | Check name (e.g., `check_range`, `check_nulls`) |
| `message` | Human-readable description of the violation |
| `severity` | `error` (blocks pipeline) or `warn` (logged only) |
| `failed_count` | Number of rows/items that failed |
| `total_count` | Total rows/items checked |

### Severity and Thresholds

By default, all violations are `severity: error` (pipeline stops). Threshold support allows downgrading errors to warnings when the violation rate is below a threshold:

- A finding with `threshold: 0.05` and 1% violation rate → downgraded to warning
- A finding at exactly the threshold (e.g., 5% with `threshold: 0.05`) → stays error (strict less-than)

### Pipeline Behavior

- **Passed**: Data proceeds through the pipeline (transform → type-enforce → curated)
- **Failed**: Data is not written. Manifest records `status: failed` with error details
- **Warnings**: Logged but do not block the pipeline

## Cross-Engine Consistency

Fairway runs the same validation logic on both DuckDB/Pandas and PySpark:

- `check_pattern` uses `str.fullmatch()` (Pandas) and `rlike()` (Spark) — both do full-string matching when patterns include anchors
- Null values are explicitly excluded from `check_range`, `check_values`, and `check_pattern` on both engines
- Missing columns produce a warning (not a crash) on both engines

## Config Validation

Invalid validation configs are caught at load time:

- Unknown keys → `ConfigValidationError`
- Wrong types (e.g., `min_rows: "banana"`) → `ConfigValidationError`
- Invalid regex in `check_pattern` → `ConfigValidationError`
- `check_custom` or `check_unique` → `NotImplementedError` at runtime
