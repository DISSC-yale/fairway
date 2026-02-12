# Dead Code Analysis Report

**Generated:** 2026-02-12
**Project:** fairway
**Tools Used:** vulture, ruff

---

## Summary

| Category | Count | Status |
|----------|-------|--------|
| Unused Imports | 8 | **CLEANED** |
| Unused Variables | 3 | **CLEANED** |
| Remaining Issues | 6 | CAUTION - signature params/context managers |
| Unused Dependencies | 1 | CAUTION - mkdocs-material |

---

## Cleanup Completed

### Removed Unused Imports (8 total)
- `cli.py`: `.templates.APPTAINER_DEF`, `.templates.DOCKERFILE_TEMPLATE`, `.config_loader.Config`
- `pyspark_engine.py`: `re`
- `pipeline.py`: `DuckDBEngine`, `PySparkEngine`, `hashlib`, `shutil`

### Removed Unused Variables (3 total)
- `cli.py:502`: `cfg = Config(config)` - removed
- `pipeline.py:769`: `ext = ...` - removed with stale comments
- `pyspark_engine.py:295`: `target_format = ...` - removed with stale comments

### Tests Verified
All 40 core tests pass after cleanup.

---

## Findings by Severity

### SAFE - Auto-fixable with ruff --fix

These are unused imports that can be safely removed:

| File | Line | Issue |
|------|------|-------|
| `cli.py` | 119 | `.templates.APPTAINER_DEF` imported but unused |
| `cli.py` | 119 | `.templates.DOCKERFILE_TEMPLATE` imported but unused |
| `pyspark_engine.py` | 10 | `re` imported but unused |
| `pipeline.py` | 12 | `.engines.duckdb_engine.DuckDBEngine` imported but unused |
| `pipeline.py` | 13 | `.engines.pyspark_engine.PySparkEngine` imported but unused |
| `pipeline.py` | 207 | `hashlib` imported but unused |
| `pipeline.py` | 246 | `shutil` imported but unused |

### CAUTION - Unused Variables (Review Required)

These variables are assigned but never used:

| File | Line | Variable | Confidence | Recommendation |
|------|------|----------|------------|----------------|
| `cli.py` | 502 | `cfg` | 100% | SAFE - Remove assignment |
| `pyspark_engine.py` | 296 | `target_format` | 100% | CAUTION - May be intentional placeholder |
| `pipeline.py` | 773 | `ext` | 100% | SAFE - Remove assignment |
| `enrichments/geospatial.py` | 6 | `address` | 100% | CAUTION - Signature parameter |
| `enrichments/geospatial.py` | 13 | `lat, lon, resolution` | 100% | CAUTION - Signature parameters |
| `enrichments/geospatial.py` | 53 | `addr` | 100% | SAFE - Remove assignment |
| `enrichments/geospatial.py` | 61 | `addr` | 100% | SAFE - Remove assignment |
| `enrichments/geospatial.py` | 66 | `lon_series` | 100% | SAFE - Remove assignment |
| `logging_config.py` | 101 | `exc_tb, exc_type, exc_val` | 100% | CAUTION - Context manager protocol |

### DANGER - Do Not Remove

These items should NOT be removed without careful consideration:

| Item | Reason |
|------|--------|
| `mkdocs-material` in dependencies | Used for documentation build, not runtime |
| Engine imports in `pipeline.py` | May be used dynamically or for type hints |

---

## Dependencies Analysis

### Core Dependencies (pyproject.toml)
- `click` - CLI framework - **USED**
- `pyyaml` - Config loading - **USED**
- `fsspec` - File system abstraction - **USED**
- `mkdocs-material` - Documentation - **BUILD ONLY** (could move to dev deps)
- `pandas` - Data processing - **USED**
- `tabulate` - Table formatting - **USED**

### Recommendation
Move `mkdocs-material` to `[project.optional-dependencies.docs]` since it's not needed at runtime.

---

## Proposed Safe Fixes

### Phase 1: Unused Imports (Auto-fix)
```bash
ruff check src/fairway --select F401 --fix
```

### Phase 2: Unused Variables (Manual Review)
1. Remove `cfg` assignment in `cli.py:502`
2. Remove `ext` assignment in `pipeline.py:773`
3. Prefix unused signature parameters with `_` (e.g., `_address`, `_lat`)

### Phase 3: Dependency Cleanup
Move `mkdocs-material` to optional docs dependencies.

---

## Test Verification Required

Before applying any changes:
```bash
pytest tests/ -v
```

After each change:
```bash
pytest tests/ -v
```
