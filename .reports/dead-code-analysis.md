# Dead Code Analysis Report

**Generated**: 2026-02-05
**Tool**: vulture 2.14 + manual analysis
**Baseline Tests**: 168 passed, 1 failed (pre-existing)

---

## Summary

| Severity | Count | Action |
|----------|-------|--------|
| SAFE     | 5     | Remove |
| CAUTION  | 2     | Review |
| FALSE POSITIVE | 8 | Ignore |

---

## SAFE: Unreachable / Unused Code (Remove)

### 1. Duplicate `except` block (BUG)
**File**: `src/fairway/engines/pyspark_engine.py:9-12`
**Confidence**: 100%
```python
except ImportError as e:   # Line 9 - UNREACHABLE
    SparkSession = None
    F = None
    _spark_import_error = e
```
**Reason**: Duplicate `except` block after the first one - syntactically valid but unreachable dead code.

### 2. Unused import `random`
**File**: `src/fairway/engines/pyspark_engine.py:14`
**Confidence**: 100%
```python
import random  # Never used in file
```

### 3. Unused import `os`
**File**: `src/fairway/summarize.py:2`
**Confidence**: 100%
```python
import os  # Never used in file
```

### 4. Unused import `pd` (pandas)
**File**: `src/fairway/validations/checks.py:1`
**Confidence**: 100%
```python
import pandas as pd  # Never used - uses native DataFrame methods
```

### 5. Unused import `json`
**File**: `src/fairway/exporters/redivis_exporter.py:7`
**Confidence**: 100%
```python
import json  # Never used in file
```

---

## CAUTION: Potentially Unused (Review First)

### 1. Unused function parameter `lon_series`
**File**: `src/fairway/enrichments/geospatial.py:66`
**Confidence**: 100%
```python
def get_h3(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    # lon_series is declared but never used in function body
    return lat_series.apply(lambda x: hex(random.getrandbits(60))[2:].zfill(15))
```
**Action**: This is a mock function. The parameter is part of the API signature but unused in the mock implementation. Keep for API consistency.

### 2. Unused import `data`
**File**: `src/fairway/templates.py:2`
**Confidence**: 80%
```python
from . import data  # Import exists but not directly referenced
```
**Action**: This import may be intentional to ensure the subpackage is loaded. Common pattern for package resources. Keep for safety.

---

## FALSE POSITIVES (Ignore)

These are **NOT** dead code - vulture incorrectly flagged them:

| File | Line | Variable | Reason |
|------|------|----------|--------|
| `geospatial.py` | 6 | `address` | Function parameter (used in callers) |
| `geospatial.py` | 13 | `lat`, `lon`, `resolution` | Function parameters (used in callers) |
| `geospatial.py` | 53, 61 | `addr` | Lambda parameter (used in `.apply()`) |
| `logging_config.py` | 102 | `exc_type`, `exc_val`, `exc_tb` | Standard `__exit__` protocol |

---

## Test Files (Informational Only)

Test files with unused fixtures (pytest fixtures are called implicitly):

| File | Variable | Status |
|------|----------|--------|
| `test_cli_manifest.py` | `setup_manifest` (multiple) | Pytest fixture - OK |
| `test_logging_config.py` | `cls` | Class method parameter - OK |
| `test_pyspark_salting.py` | `F` import | May be unused - low priority |
| `test_schema_evolution.py` | `pyspark`, `delta` imports | May be unused - low priority |

---

## Cleanup Plan

### Phase 1: Safe Removals (No Behavioral Change)
1. Remove duplicate `except` block in `pyspark_engine.py`
2. Remove unused `import random` from `pyspark_engine.py`
3. Remove unused `import os` from `summarize.py`
4. Remove unused `import pandas as pd` from `validations/checks.py`
5. Remove unused `import json` from `redivis_exporter.py`

### Phase 2: Test Verification
- Run full test suite after each change
- Rollback if any new failures

---

## Pre-existing Test Failure (Unrelated)

```
FAILED tests/test_schema_evolution.py::test_strict_schema_validation_extra_col_fail
```
**Reason**: Test expects `ValueError[RULE-115]` when CSV has extra columns vs schema, but Spark only emits a warning instead of raising an exception. This is a behavioral issue, not dead code related.
