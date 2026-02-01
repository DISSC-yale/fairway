# Code Review: Fixed-Width File Support (Chunk F)

**Reviewer**: Senior Data Engineer
**Date**: 2025-01-31
**Status**: Issues Found - Requires Changes

---

## Summary

Implementation adds fixed-width file format support with spec-based column extraction. Overall architecture is sound but several issues need addressing before merge.

## Files Changed

| File | Changes | LOC |
|------|---------|-----|
| `src/fairway/fixed_width.py` | NEW - Spec loader/validator | 249 |
| `src/fairway/config_loader.py` | Add fixed_width format + validation | +23 |
| `src/fairway/engines/duckdb_engine.py` | Add `_ingest_fixed_width()` | +101 |
| `src/fairway/engines/pyspark_engine.py` | Add `_ingest_fixed_width()` + localhost fix | +137 |
| `tests/test_fixed_width.py` | NEW - 15 tests | 254 |

---

## Rules Compliance

| Rule | Status | Notes |
|------|--------|-------|
| RULE-103 Scalable Collection | ⚠️ VIOLATION | PySpark uses `.count()` on full dataset |
| RULE-104 Engine Agnostic | ✅ PASS | Shared spec loader, engine-specific readers |
| RULE-112 Import Isolation | ✅ PASS | Lazy imports in engine methods |
| RULE-115 Data Integrity | ✅ PASS | Strict line validation, clear errors |
| RULE-116 Test Coverage | ⚠️ PARTIAL | Missing schema_merge fixtures |
| RULE-117 Schema Inference | N/A | Spec file required, no inference |
| RULE-118 Performance Defaults | ✅ PASS | trim=false by default |

---

## Issues Found

### Issue 1: RULE-103 Violation - Full Dataset Count (MEDIUM)

**Location**: `pyspark_engine.py:331`

```python
short_lines_count = df.filter(F.length(F.col("value")) < line_length).count()
```

**Problem**: `.count()` triggers full dataset scan. For billion-row files, this adds significant overhead.

**Recommendation**: Sample first N rows for validation, or make strict validation opt-in:

```python
# Option A: Sample-based validation (fast, catches most issues)
sample_df = df.limit(10000)
short_lines_count = sample_df.filter(F.length(F.col("value")) < line_length).count()

# Option B: Opt-in strict validation
if kwargs.get('strict_line_validation', False):
    short_lines_count = df.filter(...).count()
```

---

### Issue 2: SQL Injection Risk (MEDIUM)

**Location**: `duckdb_engine.py:186-189`

```python
name = col['name']  # From user-provided spec file
select_parts.append(f"{cast_expr} AS {name}")
```

**Problem**: Column names from spec file are interpolated directly into SQL. A malicious spec file could inject SQL.

**Recommendation**: Validate column names match identifier pattern:

```python
import re
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

if not VALID_IDENTIFIER.match(name):
    raise FixedWidthSpecError([f"Invalid column name '{name}': must be valid SQL identifier"])
```

---

### Issue 3: Missing Type Validation (LOW)

**Location**: `fixed_width.py:146`

```python
col_type = col.get('type', 'VARCHAR')  # No validation
```

**Problem**: Invalid types (e.g., `type: BANANA`) pass validation but cause cryptic engine errors at runtime.

**Recommendation**: Add type whitelist:

```python
VALID_TYPES = {'INTEGER', 'INT', 'BIGINT', 'LONG', 'DOUBLE', 'FLOAT', 'VARCHAR', 'STRING', 'DATE', 'TIMESTAMP'}

col_type = col.get('type', 'VARCHAR').upper()
if col_type not in VALID_TYPES:
    errors.append(f"{prefix}: Invalid type '{col_type}'. Must be one of {VALID_TYPES}")
```

---

### Issue 4: Dead Code (LOW)

**Location**: `fixed_width.py:177-248`

```python
def infer_types_from_data(data_lines, spec, sample_size=1000):
    ...
```

**Problem**: Function is defined but never called. Per plan, type inference was "allow for B" (optional fallback).

**Recommendation**: Either:
1. Integrate into engine readers when `type` is missing from spec
2. Remove until needed (avoid dead code)

---

### Issue 5: RULE-116 Partial Compliance (LOW)

**Missing per RULE-116 checklist**:
- [ ] `tests/fixtures/schema_merge/fixed_width/` - Files with different columns
- [ ] `tests/test_ingestion_formats.py::test_fixed_width_basic_read`

**Recommendation**: Add missing fixtures and test entry for full compliance.

---

### Issue 6: Empty Line Handling (LOW)

**Problem**: No explicit handling for blank lines in data files. Behavior is undefined.

**Recommendation**: Document behavior or explicitly filter:

```python
# Filter empty lines before processing
df = df.filter(F.length(F.trim(F.col("value"))) > 0)
```

---

### Issue 7: PySpark Test Uses Private Method (LOW)

**Location**: `test_fixed_width.py:222`

```python
result = spark_engine._ingest_fixed_width(...)  # Private method
```

**Recommendation**: Test through public API:

```python
result = spark_engine.ingest(
    input_path, output_path,
    format="fixed_width",
    fixed_width_spec=spec_path
)
```

---

## Suggested New Rules

### RULE-119: Input Validation for User-Provided Identifiers

**Priority:** MUST
**Category:** [SEC] Security

**Rule:**
User-provided identifiers (column names, table names) that are interpolated into SQL or code MUST be validated against a safe pattern before use.

```python
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
if not VALID_IDENTIFIER.match(user_input):
    raise ValueError(f"Invalid identifier: {user_input}")
```

**Rationale:** Prevents SQL injection and code injection from untrusted configuration files.

---

### RULE-120: Validation Costs Must Be Bounded

**Priority:** SHOULD
**Category:** [ARC] Architectural Principles

**Rule:**
Data validation operations SHOULD have bounded cost (O(1) or O(sample_size)), not O(n) where n is dataset size. Full-dataset validation SHOULD be opt-in.

**Rationale:** Validation should not become the bottleneck for large datasets.

---

## Approval Status

**APPROVED WITH NOTES**

Fixed in this revision:
- [x] Issue 1: RULE-103 violation → Now uses sampling (first 10k rows)
- [x] Issue 2: SQL injection risk → Column names validated against identifier pattern
- [x] Issue 3: Type validation → Added VALID_TYPES whitelist
- [x] Issue 7: Test public API → PySpark test now uses `ingest()` method

Deferred (non-blocking):
- [ ] Issue 5: RULE-116 compliance (schema_merge fixtures)
- [ ] Issue 4: Dead code (`infer_types_from_data`)
- [ ] Issue 6: Empty line handling documentation

New rules added:
- RULE-119: Input Validation for User-Provided Identifiers
- RULE-120: Validation Costs Must Be Bounded
