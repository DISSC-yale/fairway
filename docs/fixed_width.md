# Fixed-Width File Format Support

Fixed-width files are text files where columns are defined by character positions rather than delimiters. This format is common in legacy mainframe systems, government data feeds, and financial data exports.

## Quick Start

### 1. Create a Spec File

Create a YAML file defining your column positions:

```yaml
# specs/legacy_spec.yaml
columns:
  - name: id
    start: 0        # 0-indexed character position
    length: 5       # Number of characters
    type: INTEGER   # Native engine type
    trim: true      # Strip whitespace (optional, default: false)

  - name: name
    start: 5
    length: 30
    type: VARCHAR
    trim: true

  - name: amount
    start: 35
    length: 10
    type: DOUBLE
```

### 2. Configure Your Table

In `fairway.yaml`:

```yaml
tables:
  - name: "legacy_records"
    path: "data/raw/*.txt"
    format: "fixed_width"
    fixed_width_spec: "specs/legacy_spec.yaml"
```

### 3. Run Ingestion

```bash
fairway run
```

## Spec File Format

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Column name in output |
| `start` | Yes | 0-indexed start position |
| `length` | Yes | Number of characters to read |
| `type` | No | Engine-native type (default: VARCHAR) |
| `trim` | No | Strip whitespace (default: false) |

### Supported Types

Use native engine types directly:

| DuckDB | PySpark | Description |
|--------|---------|-------------|
| INTEGER | INTEGER | 32-bit integer |
| BIGINT | LONG | 64-bit integer |
| DOUBLE | DOUBLE | 64-bit float |
| VARCHAR | STRING | Text |

## Hierarchical Data (Record Type Filtering)

Some fixed-width formats (e.g., IPUMS census data) interleave multiple record types in a single file — household records (`H`) and person records (`P`) on alternating lines. Use `record_type_filter` in the spec to select only one record type:

```yaml
# specs/us1950_H_spec.yaml
record_type_filter:
  position: 0       # 0-indexed position of the record type indicator
  length: 1         # Length of the indicator field
  value: "H"        # Keep only lines where this field equals "H"

columns:
  - name: rectype
    start: 0
    length: 1
    type: VARCHAR
  - name: serial
    start: 1
    length: 8
    type: BIGINT
    trim: true
  # ... household-specific columns
```

Generate separate spec files for each record type (e.g., `us1950_H_spec.yaml` and `us1950_P_spec.yaml`) and configure separate tables for each:

```yaml
tables:
  - name: "census_1950_household"
    path: "data/*.dat"
    format: "fixed_width"
    fixed_width_spec: "specs/us1950_H_spec.yaml"

  - name: "census_1950_person"
    path: "data/*.dat"
    format: "fixed_width"
    fixed_width_spec: "specs/us1950_P_spec.yaml"
```

## Type Enforcement

Fixed-width columns are initially read as VARCHAR. Fairway applies two-layer type enforcement to convert them to the types declared in your spec:

1. **Spec types**: Column types defined in the spec file (e.g., `BIGINT`, `DOUBLE`)
2. **TRY_CAST**: Safe casting that converts invalid values to NULL instead of failing

Configure the failure behavior per table:

```yaml
tables:
  - name: "census_data"
    format: "fixed_width"
    fixed_width_spec: "specs/census_spec.yaml"
    type_enforcement:
      on_fail: null    # TRY_CAST: invalid values become NULL (default)
```

| `on_fail` | Behavior |
|-----------|----------|
| `null` (default) | Use TRY_CAST — invalid values become NULL |
| `strict` | Use CAST — fail on any invalid value |

## Data Validation

Fixed-width ingestion enforces strict validation per **RULE-115**:

- **Line Length Check**: All lines must be at least as long as the rightmost column endpoint
- **Short Lines Fail**: If any line is too short, ingestion fails with a clear error showing samples
- **No Silent Truncation**: Partial data is never silently dropped

### Skipping Corrupted Lines

If your data has a small number of corrupted or truncated lines, use `min_line_length` to filter them out before validation:

```yaml
tables:
  - name: "legacy_records"
    path: "data/*.dat"
    format: "fixed_width"
    fixed_width_spec: "specs/legacy_spec.yaml"
    min_line_length: 500    # Skip lines shorter than 500 characters
```

Lines shorter than `min_line_length` are silently dropped before the RULE-115 check runs. This is useful for fixed-width files with occasional corrupted records that would otherwise fail the entire ingestion.

Example error (without `min_line_length`):
```
[RULE-115] Data Integrity Error: 15 lines are shorter than expected
line length 45. Samples: len=20: '001Alice...'; len=15: '002Bob...'
```

## Example Data

**Input file** (`data.txt`):
```
001Alice               030
002Bob                 025
003Carol               028
```

**Spec file** (`spec.yaml`):
```yaml
columns:
  - name: id
    start: 0
    length: 3
    type: INTEGER
    trim: true
  - name: name
    start: 3
    length: 20
    type: VARCHAR
    trim: true
  - name: age
    start: 23
    length: 3
    type: INTEGER
    trim: true
```

**Output** (Parquet):
| id | name | age |
|----|------|-----|
| 1 | Alice | 30 |
| 2 | Bob | 25 |
| 3 | Carol | 28 |

## Engine Support

| Feature | DuckDB | PySpark |
|---------|--------|---------|
| Basic read | ✅ | ✅ |
| Type conversion | ✅ | ✅ |
| Trim whitespace | ✅ | ✅ |
| Metadata injection | ✅ | ✅ |
| Partitioning | ✅ | ✅ |
| Line validation | ✅ | ✅ |
| Record type filter | ✅ | ✅ |
| min_line_length | ✅ | ✅ |
| Type enforcement | ✅ | ✅ |

## Troubleshooting

### "fixed_width_spec file not found"

The spec file path is resolved relative to your config file. Ensure the path is correct:

```yaml
# If config is at project/fairway.yaml
# and spec is at project/specs/my_spec.yaml
fixed_width_spec: "specs/my_spec.yaml"  # Relative to config
```

### "RULE-115 Data Integrity Error"

Your data file has lines shorter than the spec expects. Check:
1. Trailing newline characters
2. Truncated records
3. Spec column positions match actual data

### Type conversion errors

If you see cast errors, verify your `type` matches the actual data:
- Numeric columns with spaces need `trim: true`
- Non-numeric values in INTEGER columns will fail
