# Dead Code Analysis Report

**Date**: 2026-02-03
**Branch**: explore/orchestration-submission
**Tool**: vulture (Python dead code finder)

## Summary

Analyzed the fairway Python codebase for dead code, unused imports, and potential issues.

## Issues Fixed

### 2026-02-03 Cleanup

| Issue | File | Status |
|-------|------|--------|
| Unused import `Path` | `batch_processor.py:9` | ✅ FIXED |
| Unused variable `dirnames` | `cli.py:935` | ✅ FIXED (renamed to `_`) |
| Unused variable `ext` + dead comments | `pipeline.py:493` | ✅ FIXED |
| Unused variable `overwrite_option` + dead comments | `duckdb_engine.py:227` | ✅ FIXED |
| Unused variable `target_format` + dead comments | `pyspark_engine.py:246` | ✅ FIXED |

### 2026-02-02 Cleanup

| Issue | File | Status |
|-------|------|--------|
| Duplicate `except ImportError` block (unreachable) | `pyspark_engine.py:9-12` | ✅ FIXED |
| Undefined `result_path` variable | `pipeline.py:370` | ✅ FIXED |
| Redundant imports | `templates.py` | ✅ FIXED |
| Unused `import importlib.resources` | `templates.py` | ✅ FIXED |
| Unused `from . import data` | `templates.py` | ✅ FIXED |
| Pointless version conditional | `templates.py` | ✅ FIXED |

## Remaining Items (CAUTION - Review Before Removing)

### Legacy Code (Keep for Backward Compatibility)

| Item | File | Notes |
|------|------|-------|
| `ManifestManager` class | `manifest.py` | Superseded by ManifestStore, but kept for migration |
| `record_schema_run()` | `manifest.py` | Legacy method, may be used externally |
| `is_schema_stale()` | `manifest.py` | Legacy method, may be used externally |
| `check_files_bulk()` | `manifest.py` | Legacy method, may be used externally |

### Unused Utility Methods (SAFE to keep as API)

| Item | File | Notes |
|------|------|-------|
| `BaseTransformer.rename_columns()` | `transformations/base.py` | Utility for custom transformers |
| `BaseTransformer.cast_types()` | `transformations/base.py` | Utility for custom transformers |
| `BaseTransformer.clean_strings()` | `transformations/base.py` | Utility for custom transformers |
| `infer_types_from_data()` | `fixed_width.py` | Planned functionality |

### Debug Statements (Replace with Logging)

| Location | File | Line |
|----------|------|------|
| `print(f"WARNING: Table path not found...")` | `config_loader.py` | 146 |
| `print(f"DEBUG: ConfigLoader globbing...")` | `config_loader.py` | 205 |
| `print(f"WARNING: ConfigLoader found no files...")` | `config_loader.py` | 208 |

## Test Results After Cleanup

```
Before: 162 passed, 14 failed (pre-existing PySpark issues)
After:  162 passed, 14 failed (same pre-existing issues)
```

All new batch orchestration tests pass (53 tests).

## Recommendations

1. **Consider** adding a `logging` module setup to replace `print()` statements
2. **Document** ManifestManager as deprecated in favor of ManifestStore
3. **Keep** BaseTransformer utility methods as documented API for custom transformers
4. **Verify** `mkdocs-material` and `tabulate` dependencies are actually needed
