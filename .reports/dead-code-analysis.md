# Dead Code Analysis Report

**Generated:** 2026-02-24 (Updated)
**Project:** fairway-1

## Summary

| Category | Count | Status |
|----------|-------|--------|
| SAFE to remove | 10 | 9 FIXED |
| CAUTION (review needed) | 1 | KEPT |
| BUGS discovered | 2 | 2 FIXED |

---

## COMPLETED CHANGES

### 1. ✅ Unused function: `get_logger()` - REMOVED
- **File:** `src/fairway/logging_config.py:178-189`
- Also removed associated tests in `test_logging_config.py`

### 2. Unused file: `custom_unzip.py` - KEPT AS EXAMPLE
- **File:** `src/fairway/data/scripts/custom_unzip.py`
- Decision: Keep as example preprocessing script for users

### 3. ✅ Unused lambda parameters `addr` - FIXED
- **File:** `src/fairway/enrichments/geospatial.py:53,61`
- Renamed to `_addr` to indicate intentional non-use

### 4. ✅ Unused UDF parameter `lon_series` - FIXED
- **File:** `src/fairway/enrichments/geospatial.py:66`
- Renamed to `_lon_series`

### 5. ✅ Redundant `import os` - REMOVED
- **File:** `src/fairway/engines/pyspark_engine.py:42`

### 6. ✅ Redundant `import os` and `import sys` - REMOVED
- **File:** `src/fairway/cli.py:222-223`

### 7. ✅ Unused dependency: `fsspec` - REMOVED
- **File:** `pyproject.toml`

### 8. ✅ Misplaced dependency: `mkdocs-material` - MOVED
- **File:** `pyproject.toml`
- Moved to `[project.optional-dependencies.docs]`

---

## CAUTION (Kept)

### 9. `BaseTransformer` helper methods - KEPT
- **File:** `src/fairway/transformations/base.py:13-30`
- Part of public API for user transformers

---

## BUGS FIXED

### BUG 1: ✅ `_preprocess_archives()` undefined variable - FIXED
- **File:** `src/fairway/pipeline.py:404`
- Now returns appropriate path pattern for extracted files

### BUG 2: ✅ `@classmethod` with `self` parameter - FIXED
- **File:** `src/fairway/enrichments/geospatial.py:20`
- Changed to `@staticmethod` and updated method calls

---

## Test Results

- **Passed:** 312
- **Failed:** 21 (PySpark-related, pre-existing)
- **Skipped:** 1

---

## NEW FINDINGS (2026-02-24)

### 9. ✅ Unused import `F` in test file - REMOVED
- **File:** `tests/test_pyspark_salting.py:3`
- **Code:** `import pyspark.sql.functions as F`
- **Status:** FIXED

### 10. ✅ Unused import `PropertyMock` in test file - REMOVED
- **File:** `tests/test_pipeline_lifecycle.py:12`
- **Code:** `from unittest.mock import patch, MagicMock, PropertyMock, call`
- **Status:** FIXED
