# Chunk C: Config Redesign - Implementation Plan

## Overview

This plan implements the config redesign for Fairway, making it more intuitive for data engineers.

**Branch**: `config-redesign`

---

## Summary of Changes

| ID | Task | Risk | Status |
|----|------|------|--------|
| C.1 | Rename `sources` → `tables` | HIGH (breaking) | Pending |
| C.2 | Document `root` purpose | LOW | Pending |
| C.3 | Separate archive handling (`archives`/`files` keys) | MEDIUM | Pending |
| C.4 | Add `--dry-run` preview | LOW | Pending |
| C.5 | Config validation (fail fast, glob must match) | LOW | Pending |
| C.7 | Multi-table zip caching | MEDIUM | Pending |
| NEW | `fairway cache clean` command | LOW | Pending |

---

## Phase 1: Core Rename (C.1)

### Files to Modify

**Priority 1 - Core Logic:**

| File | Changes |
|------|---------|
| `src/fairway/config_loader.py` | `sources` → `tables`, `source_*` → `table_*`, method renames |
| `src/fairway/pipeline.py` | Loop vars, method params, manifest calls |
| `src/fairway/schema_pipeline.py` | `sources_info` → `tables_info`, loop vars |
| `src/fairway/manifest.py` | Param renames: `source_name` → `table_name`, `source_root` → `table_root` |

**Priority 2 - Supporting:**

| File | Changes |
|------|---------|
| `src/fairway/cli.py` | `cfg.sources` → `cfg.tables` |
| `src/fairway/engines/pyspark_engine.py` | `source_root` → `table_root` param |

**Priority 3 - Templates/Docs:**

| File | Changes |
|------|---------|
| `src/fairway/data/fairway.yaml` | `sources:` → `tables:` |
| `src/fairway/templates.py` | Update CONFIG_TEMPLATE |
| `tests/test_config_validation.py` | Update test configs |

### Variable Rename Reference

| Old | New |
|-----|-----|
| `source` | `table` |
| `sources` | `tables` |
| `source_name` | `table_name` |
| `source_root` | `table_root` |
| `raw_srcs` | `raw_tables` |
| `src` (loop var) | `tbl` |
| `sources_info` | `tables_info` |
| `sources_hash` | `tables_hash` |

### Specific Changes by File

#### config_loader.py
- Line 23: `raw_srcs = self.data.get('sources')` → `raw_tables = self.data.get('tables')`
- Line 25: Fallback `data.sources` → `data.tables`
- Line 27: `self.sources` → `self.tables`
- Line 70: `_expand_sources()` → `_expand_tables()`
- Line 76: `for src in raw_sources` → `for tbl in raw_tables`
- Lines 77-204: All `src.get()` → `tbl.get()`
- Line 125: `source_name` → `table_name`
- Line 146-188: `source_root` vars → `table_root`
- Line 207: `get_source_by_name()` → `get_table_by_name()`

#### pipeline.py
- Line 38: `_preprocess(self, source)` → `_preprocess(self, table)`
- Lines 43-230: All `source.get()` → `table.get()`
- Line 237: `self.config.sources` → `self.config.tables`
- Line 268: `for source in self.config.sources` → `for table in self.config.tables`
- Lines 269-497: All `source['name']` → `table['name']`

#### manifest.py
- Line 54: `get_file_key(file_path, source_name, source_root)` → params renamed
- Line 129: `should_process(..., source_name, source_root)` → params renamed
- Line 149: `entry["source_name"]` → `entry["table_name"]`
- Line 164: `update_manifest(..., source_name, source_root)` → params renamed
- Line 183: Dict key `"source_name"` → `"table_name"`
- Line 208: `_compute_sources_hash()` → `_compute_tables_hash()`
- Line 217-229: `sources_info` → `tables_info`, `sources_hash` → `tables_hash`
- Line 248: `record_preprocessing(..., source_name, source_root)` → params renamed

#### schema_pipeline.py
- Line 49: `"sources": []` → `"tables": []`
- Line 53: `sources_info = []` → `tables_info = []`
- Line 56-59: `self.config.sources` → `self.config.tables`
- Line 118: `consolidated_schema["sources"]` → `consolidated_schema["tables"]`

---

## Phase 2: Config Validation (C.5)

### New Exception Class

Add to `config_loader.py`:

```python
class ConfigValidationError(Exception):
    """Raised when config validation fails."""
    def __init__(self, errors):
        self.errors = errors
        message = "Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)
```

### Validation Rules

**Errors (block execution):**
1. Missing required `name` field
2. Missing required `path` field (unless `archives` specified)
3. Duplicate table names
4. Invalid format type
5. Schema file specified but not found
6. Non-existent `root` directory
7. `archives` without `files` or vice versa
8. Both `path` and `archives` specified
9. **Glob pattern matches no files** (fail fast)

**Warnings (log but continue):**
1. Unknown config keys (typo detection)

### Implementation Location

Add `_validate()` method to `Config.__init__()` after `_expand_tables()`.

---

## Phase 3: Dry Run (C.4)

### CLI Changes

Add to `run` command in `cli.py`:

```python
@click.option('--dry-run', is_flag=True, help='Show matched files without processing')
def run(config, spark_master, dry_run):
```

### Pipeline Changes

Add to `IngestionPipeline.run()`:

```python
def run(self, dry_run=False):
    if dry_run:
        self._dry_run_report()
        return
    # ... existing logic
```

---

## Phase 4: Archive Handling (C.3 + C.7)

### New Config Syntax

```yaml
tables:
  - name: sales
    root: data/raw
    archives: "*.zip"           # NEW: archive pattern
    files: "**/*.csv"           # NEW: pattern inside archive
    format: csv
```

### ArchiveCache Class

Add to `pipeline.py`:

```python
class ArchiveCache:
    """Persistent cache for extracted archives."""

    def __init__(self, config, manifest):
        self.config = config
        self.manifest = manifest
        self._session_cache = {}

    def get_extracted_path(self, archive_path):
        """Return extraction path, extracting if needed."""
        # Check session cache
        # Check manifest for valid extraction
        # Extract if needed, record in manifest
        pass

    def _get_extraction_dir(self, archive_path):
        """Deterministic extraction location."""
        # .fairway_cache/archives/{name}_{hash}/
        pass
```

### Manifest Extensions

Add to `manifest.py`:

```python
def record_extraction(self, archive_path, extracted_dir, archive_hash):
    """Record archive extraction."""
    pass

def get_extraction(self, archive_path):
    """Get existing extraction info."""
    pass

def is_extraction_valid(self, archive_path):
    """Check if extraction is still valid."""
    pass
```

---

## Phase 5: Cache Clean Command

### CLI Addition

Add to `cli.py`:

```python
@main.group()
def cache():
    """Manage fairway cache."""
    pass

@cache.command()
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def clean(force):
    """Clear the archive extraction cache."""
    cache_dir = '.fairway_cache'
    if not os.path.exists(cache_dir):
        click.echo("No cache directory found.")
        return

    if not force:
        size = sum(os.path.getsize(os.path.join(dp, f))
                   for dp, dn, fn in os.walk(cache_dir) for f in fn)
        size_mb = size / (1024 * 1024)
        if not click.confirm(f"Delete {cache_dir}/ ({size_mb:.1f} MB)?"):
            return

    import shutil
    shutil.rmtree(cache_dir)
    click.echo(f"Removed {cache_dir}/")
```

**Usage:**
```bash
fairway cache clean        # With confirmation
fairway cache clean --force  # No confirmation
```

---

## Phase 6: Documentation (C.2)

Update `src/fairway/data/fairway.yaml` template with comprehensive comments explaining `root` purpose.

---

## Implementation Order

```
Step 1: C.1 Rename (config_loader.py first)
    ↓
Step 2: C.1 Rename (pipeline.py, manifest.py, schema_pipeline.py)
    ↓
Step 3: C.1 Rename (cli.py, pyspark_engine.py)
    ↓
Step 4: C.5 Validation (config_loader.py)
    ↓
Step 5: C.4 Dry Run (cli.py, pipeline.py)
    ↓
Step 6: C.3+C.7 Archive Handling (manifest.py, then pipeline.py, then config_loader.py)
    ↓
Step 7: Cache Clean Command (cli.py)
    ↓
Step 8: C.2 Documentation (templates, fairway.yaml)
    ↓
Step 9: Update all tests
```

---

## Breaking Changes

1. **Config key `sources:` → `tables:`** - All existing configs must update
2. **Manifest key `source_name` → `table_name`** - Old manifests incompatible

### Migration Script

```bash
# Config migration
sed -i 's/^sources:/tables:/g' config/*.yaml
sed -i 's/sources:/tables:/g' config/*.yaml

# Manifest migration (optional - or just regenerate)
sed -i 's/"source_name"/"table_name"/g' data/fmanifest.json
```

---

## Test Requirements

### Existing Tests to Update
- `tests/test_config_validation.py` - Change `sources` → `tables`
- `tests/test_manifest.py` - Update param names
- `tests/test_preprocessing.py` - Update source refs

### New Tests Needed

```python
# C.1 Tests
def test_config_loads_tables_key(): pass
def test_get_table_by_name(): pass

# C.5 Tests
def test_validation_error_missing_name(): pass
def test_validation_error_missing_path(): pass
def test_validation_error_duplicate_names(): pass
def test_validation_error_invalid_format(): pass
def test_validation_error_glob_no_matches(): pass

# C.4 Tests
def test_dry_run_shows_files(): pass
def test_dry_run_no_processing(): pass

# C.3+C.7 Tests
def test_archive_extraction(): pass
def test_archive_cache_reuse(): pass
def test_multi_table_same_archive(): pass
def test_archive_validation_errors(): pass

# Cache Clean Tests
def test_cache_clean_removes_directory(): pass
def test_cache_clean_no_cache_exists(): pass
```

---

## Decisions Made

1. **Manifest Migration**: **Fresh start** - Users delete old manifests and regenerate
2. **Archive Cache Cleanup**: **Add now** - Include `fairway cache clean` command in this PR
3. **Glob Behavior**: **Error** - Glob patterns must match at least one file (fail fast)
4. **Archive Handling**: **Include now** - C.3+C.7 in this PR
5. **Root Validation**: At config load (fail fast)

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Breaking existing configs | HIGH | Provide sed migration script |
| Manifest incompatibility | MEDIUM | Document fresh start approach |
| Archive hash performance | LOW | Use fast mtime+size check by default |
| Test failures | LOW | Update tests in same PR |
