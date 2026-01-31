# Phase 2: UX Improvements - Implementation Plan

## Overview

Phase 2 consists of two parallel workstreams:
- **Chunk C**: Config Redesign (make config intuitive)
- **Chunk D**: Performance Fixes (right-sized files, remove salting)

---

## Current State Analysis

### Config Structure (`config_loader.py`)
- Uses `sources` list with `name`, `root`, `path`, `include/exclude` patterns
- `root` used for file relocation and manifest portability
- Zip handling embedded in preprocessing logic
- Minimal validation: only engine type and format type checked
- No validation for: missing required fields, duplicate names, non-existent paths

### Performance Settings (`pyspark_engine.py`)
- Salting enabled by default via `balanced=True` parameter
- Salt column added when `balanced=True` and `partition_by` set
- No explicit parquet file size control
- `target_rows=500000` default for partition sizing

### Key Files
| File | Purpose |
|------|---------|
| `src/fairway/config_loader.py` | Config parsing, storage settings |
| `src/fairway/engines/pyspark_engine.py` | Write options, salting (lines 195-207) |
| `src/fairway/cli.py` | CLI commands |
| `src/fairway/pipeline.py` | Main pipeline, preprocessing |

---

## Chunk C: Config Redesign

### Design Rule: Self-Documenting Config Template

The default `fairway.yaml` template must include **all options with comments** so users can comment/uncomment as needed.

**Principles**:
1. Every config option appears in the template (no hidden options)
2. Each option has a comment explaining its purpose
3. Optional sections are commented out by default with example values
4. Users should never need to look up docs to find an option name

**Example template structure**:
```yaml
# Fairway Configuration
# See docs for full reference: <link>

dataset_name: "my_dataset"

# Engine: duckdb (single-node) or pyspark (distributed)
engine: duckdb

# ─────────────────────────────────────────────────────────────
# Tables - define your data sources
# ─────────────────────────────────────────────────────────────
tables:
  - name: my_table
    path: "data/raw/*.csv"
    format: csv                    # csv, tsv, json, parquet
    # root: "data/raw"             # Base path for file discovery
    # schema: "schemas/my_table.yaml"
    # partition_by: [year, month]
    # naming_pattern: "(?P<date>\\d{8})"
    # write_mode: overwrite        # overwrite, append

    # Processing mode (many files → one table)
    # expand_glob: false           # false = engine reads all at once (default)
                                   # true = process each file separately
    # hive_partitioning: false     # true = extract partitions from dir structure

    # Archive handling (for zipped data)
    # archives: "*.zip"
    # files: "**/*.csv"

    # Read options (format-specific)
    # read_options:
    #   header: true
    #   delimiter: ","
    #   encoding: "utf-8"

    # Preprocessing
    # preprocess:
    #   unzip: true
    #   execution_mode: driver     # driver = on driver node (default)
                                   # cluster = distributed (for 1000s of files)

# ─────────────────────────────────────────────────────────────
# Storage paths
# ─────────────────────────────────────────────────────────────
storage:
  raw_dir: "data/raw"
  intermediate_dir: "data/intermediate"
  final_dir: "data/final"
  # scratch_dir: "/scratch/$USER/fairway"
  # format: parquet

# ─────────────────────────────────────────────────────────────
# Performance tuning
# ─────────────────────────────────────────────────────────────
# performance:
#   target_rows: 500000
#   target_file_size_mb: 128
#   compression: snappy           # snappy, gzip, zstd
#   salting: false

# ─────────────────────────────────────────────────────────────
# Validations
# ─────────────────────────────────────────────────────────────
# validations:
#   null_check: [required_column]
#   unique_check: [id_column]

# ─────────────────────────────────────────────────────────────
# Enrichment
# ─────────────────────────────────────────────────────────────
# enrichment:
#   add_columns:
#     load_date: "current_date()"

# ─────────────────────────────────────────────────────────────
# Redivis export
# ─────────────────────────────────────────────────────────────
# redivis:
#   organization: "my_org"
#   dataset: "my_dataset"
```

---

### Processing Modes: Many Files → One Table

When many files flow into a single table, there are different processing strategies:

**Current options**:

| Option | Default | Purpose |
|--------|---------|---------|
| `expand_glob` | `false` | How files are grouped |
| `hive_partitioning` | `false` | Use directory structure as partitions |
| `preprocess.execution_mode` | `driver` | Where preprocessing runs |

**`expand_glob` behavior**:
```yaml
tables:
  # expand_glob: false (default) - all files read at once by engine
  - name: sales
    path: "data/sales/*.csv"      # 1000 files → engine reads all at once
    # expand_glob: false

  # expand_glob: true - each file processed separately
  - name: sales_with_metadata
    path: "data/sales/*.csv"
    expand_glob: true              # 1000 files → 1000 separate reads
    naming_pattern: "(?P<date>\\d{8})"  # extract metadata per file
```

**When to use each**:
| Scenario | `expand_glob` | Why |
|----------|---------------|-----|
| Simple ingestion, homogeneous files | `false` | Engine handles parallelism efficiently |
| Need per-file metadata extraction | `true` | Each file processed to capture metadata |
| Files have different schemas | `true` | Process individually to handle variations |
| Memory constrained | `true` | Process one file at a time |

**`execution_mode` for preprocessing**:
```yaml
tables:
  - name: large_dataset
    path: "data/*.csv.gz"
    preprocess:
      unzip: true
      execution_mode: driver    # Default: preprocess on driver node
      # execution_mode: cluster # For 1000s of files, distribute preprocessing
```

| Mode | Use when |
|------|----------|
| `driver` | < 100 files, or files are small |
| `cluster` | 1000s of files, preprocessing is bottleneck (requires pyspark) |

**`hive_partitioning`**:
```yaml
tables:
  - name: partitioned_data
    path: "data/year=*/month=*/*.parquet"
    hive_partitioning: true    # Extract year, month from directory structure
```

**Update to config template** - add these options:

```yaml
tables:
  - name: my_table
    path: "data/*.csv"
    # expand_glob: false         # true = process each file separately
    # hive_partitioning: false   # true = use directory structure as partitions

    # preprocess:
    #   execution_mode: driver   # driver (default) or cluster (for 1000s of files)
```

---

### C.1: Rename `sources` to `tables`

**Decision**: Rename all `source`/`sources` terminology to `table`/`tables` throughout codebase. No backwards compatibility.

#### Files to Modify

**1. `src/fairway/config_loader.py`** (Priority 1 - Core)
| Line(s) | Change |
|---------|--------|
| 22-27 | `self.data.get('sources')` → `self.data.get('tables')` |
| 27 | `self.sources` → `self.tables` |
| 70 | Method `_expand_sources()` → `_expand_tables()` |
| 76 | Loop var `src` → `tbl` |
| 77-140 | All `src.get()` → `tbl.get()` |
| 125 | `source_name` → `table_name` |
| 118, 171 | `source_format` → `table_format` |
| 146-188 | `source_root` → `table_root` |
| 207-211 | Method `get_source_by_name()` → `get_table_by_name()` |

**2. `src/fairway/pipeline.py`** (Priority 1 - Core)
| Line(s) | Change |
|---------|--------|
| 38 | Method param `source` → `table` |
| 43-101 | All `source.get()` → `table.get()` |
| 237 | `self.config.sources` → `self.config.tables` |
| 247-253 | Loop vars `s` → `t`, `s.get()` → `t.get()` |
| 268 | `for source in self.config.sources` → `for table in self.config.tables` |
| 272-497 | All `source['name']`, `source.get()` → `table['name']`, `table.get()` |

**3. `src/fairway/schema_pipeline.py`** (Priority 1 - Core)
| Line(s) | Change |
|---------|--------|
| 49 | Dict key `"sources": []` → `"tables": []` |
| 53 | `sources_info` → `tables_info` |
| 56 | `self.config.sources` → `self.config.tables` |
| 59-130 | `for source` → `for table`, all `source['name']` → `table['name']` |
| 118 | `consolidated_schema["sources"]` → `consolidated_schema["tables"]` |
| 228-229 | Manifest keys `sources_hash` → `tables_hash`, `sources` → `tables` |

**4. `src/fairway/manifest.py`** (Priority 2)
| Line(s) | Change |
|---------|--------|
| 54-71 | Params `source_name` → `table_name`, `source_root` → `table_root` |
| 129-162 | Method `should_process()` param renames |
| 149-150 | `entry_source` → `entry_table`, key `"source_name"` → `"table_name"` |
| 164-186 | Method `update_manifest()` param renames |
| 182-183 | Dict key `"source_name"` → `"table_name"` |
| 208-240 | `sources_info` → `tables_info`, `sources_hash` → `tables_hash` |
| 248-282 | Preprocessing methods param renames |

**5. `src/fairway/cli.py`** (Priority 2)
| Line(s) | Change |
|---------|--------|
| 49-51 | `cfg.sources` → `cfg.tables`, `src` → `tbl` |

**6. `src/fairway/engines/pyspark_engine.py`** (Priority 3)
| Line(s) | Change |
|---------|--------|
| 323 | Param `source_root` → `table_root` |
| 338-339 | Variable refs |

**7. `src/fairway/data/fairway.yaml`** (Priority 4 - Template)
| Line(s) | Change |
|---------|--------|
| 2 | Comment text |
| 28 | Root key `sources:` → `tables:` |
| 29-92 | All example entries |

**8. `src/fairway/data/getting-started.md`** (Priority 4 - Docs)
| Line(s) | Change |
|---------|--------|
| 54-56 | YAML example `sources:` → `tables:` |

#### Variable Naming Reference
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

---

### C.2: Clarify Root Purpose

**Current**: `root` sets base path for file discovery and output organization.

**Changes**:
1. Add inline documentation in config template
2. Create "Config Reference" doc section

**Files**: `src/fairway/data/fairway.yaml`, docs

---

### C.3: Separate Archive Handling

**Design**: Table-centric with shared archive cache. Each table can reference an archive, and if multiple tables reference the same archive, it's extracted once and cached.

**Proposed Syntax**:
```yaml
tables:
  # Simple case: single table from archive
  - name: my_table
    root: data/raw
    archives: "*.zip"        # NEW: archive pattern(s) to extract
    files: "**/*.csv"        # NEW: pattern for files inside extracted archive

  # Multi-table from same archive
  - name: sales
    root: data/raw
    archives: "data.zip"
    files: "sales_*.csv"     # Only sales files → sales table

  - name: customers
    root: data/raw
    archives: "data.zip"     # Same zip - extracted once, cached
    files: "customers_*.csv" # Only customer files → customers table

  - name: inventory
    root: data/raw
    archives: "data.zip"     # Reuses cached extraction
    files: "inventory_*.csv"
```

**How it works**:
1. When `archives` is specified, extract matching archives to temp directory
2. Cache extraction by archive path (see C.7)
3. Apply `files` pattern to extracted contents
4. If `archives` not specified, `path` works as before (direct file access)

**Config keys**:
| Key | Purpose |
|-----|---------|
| `path` | Direct file path/pattern (existing behavior) |
| `archives` | Archive file pattern(s) to extract |
| `files` | Pattern for files inside extracted archive |

**Validation**:
- Error if both `path` and `archives` specified
- Error if `files` specified without `archives`
- Error if `archives` specified without `files`

**Implementation**:
1. Add `archives` and `files` keys to config parsing
2. In preprocessing, check for `archives` key
3. Extract to temp, apply `files` pattern, set as `path` for downstream

**Files**: `config_loader.py`, `pipeline.py`

---

### C.4: Add --dry-run Preview

**Implementation**:
```python
# In cli.py, add to run command:
@click.option('--dry-run', is_flag=True, help='Show matched files without processing')

# In pipeline.py:
def run(self, dry_run=False):
    for table in self.config.tables:
        matched_files = self._discover_files(table)
        if dry_run:
            print(f"\n{table['name']}:")
            print(f"  Root: {table.get('root')}")
            print(f"  Pattern: {table['path']}")
            print(f"  Matched files ({len(matched_files)}):")
            for f in matched_files[:20]:
                print(f"    - {f}")
            if len(matched_files) > 20:
                print(f"    ... and {len(matched_files) - 20} more")
            continue
        # ... normal processing
```

**Files**: `cli.py`, `pipeline.py`

---

### C.5: Config Validation with Clear Warnings

**Goal**: Catch config errors early with actionable messages.

#### Validation Categories

**Errors (block execution)**:
1. Missing required fields (`name`, `path`)
2. Duplicate table names
3. Invalid format type
4. Schema file specified but not found
5. Invalid engine type
6. Path does not exist (for non-glob paths)
7. Root directory does not exist

**Warnings (log but continue)**:
1. Glob pattern matches no files
2. Unused config keys (possible typos)
3. Deprecated options

#### Implementation

```python
# In config_loader.py - new ConfigValidationError class
class ConfigValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        message = "Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)

# In Config.__init__, add validation call:
def __init__(self, config_path):
    # ... existing parsing ...
    self.tables = self._expand_tables(raw_tables)
    self._validate()  # NEW

def _validate(self):
    errors = []
    warnings = []

    # Check for empty tables
    if not self.tables:
        warnings.append("No tables defined in config")

    table_names = set()
    for i, table in enumerate(self.tables):
        prefix = f"tables[{i}]"

        # Required: name
        name = table.get('name')
        if not name:
            errors.append(f"{prefix}: Missing required 'name' field")
        elif name in table_names:
            errors.append(f"{prefix}: Duplicate table name '{name}'")
        else:
            table_names.add(name)
            prefix = f"tables['{name}']"

        # Required: path
        if not table.get('path'):
            errors.append(f"{prefix}: Missing required 'path' field")

        # Valid format
        fmt = table.get('format', 'csv')
        valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet'}
        if fmt not in valid_formats:
            errors.append(f"{prefix}: Invalid format '{fmt}'. Must be one of {valid_formats}")

        # Schema file exists
        schema = table.get('schema')
        if isinstance(schema, str) and not os.path.exists(schema):
            errors.append(f"{prefix}: Schema file not found: {schema}")

        # Error: path doesn't exist (for non-glob paths)
        path = table.get('path', '')
        if path and '*' not in path and not os.path.exists(path):
            errors.append(f"{prefix}: Path does not exist: {path}")

        # Error: root doesn't exist
        root = table.get('root')
        if root and not os.path.isdir(root):
            errors.append(f"{prefix}: Root directory does not exist: {root}")

        # Warning: glob pattern matches no files
        if path and '*' in path:
            import glob as g
            search_path = os.path.join(root, path) if root else path
            if not g.glob(search_path, recursive=True):
                warnings.append(f"{prefix}: Glob pattern matches no files: {search_path}")

    # Print warnings
    for w in warnings:
        print(f"WARNING: {w}")

    # Raise on errors
    if errors:
        raise ConfigValidationError(errors)
```

#### Example Output

**Error case**:
```
fairway.ConfigValidationError: Config validation failed:
  - tables['sales']: Missing required 'path' field
  - tables[2]: Duplicate table name 'customers'
  - tables['inventory']: Invalid format 'xlsx'. Must be one of {'csv', 'tsv', 'tab', 'json', 'parquet'}
  - tables['orders']: Path does not exist: data/raw/orders_2024.csv
  - tables['customers']: Root directory does not exist: /data/archive
```

**Warning case**:
```
WARNING: tables['sales']: Glob pattern matches no files: data/raw/*.csv
Processing 3 tables...
```

**Files**: `config_loader.py`

---

### C.6: Unzip Support Decision

**Recommendation**: Keep current behavior, make explicit in docs.
- Current approach works
- Removing would break existing configs
- Document clearly in config reference

---

### C.7: Multi-table Zip Caching

**Purpose**: When multiple tables reference the same archive (C.3), extract it only once.

**Example scenario**:
```yaml
tables:
  - name: sales
    archives: "data.zip"
    files: "sales_*.csv"
  - name: customers
    archives: "data.zip"      # Same archive
    files: "customers_*.csv"
  - name: inventory
    archives: "data.zip"      # Same archive
    files: "inventory_*.csv"
```

Without caching: `data.zip` extracted 3 times.
With caching: `data.zip` extracted once, reused for all 3 tables.

**Implementation**:
```python
# In pipeline.py:
class ArchiveCache:
    """Cache for extracted archives to avoid redundant extraction."""
    _extracted = {}  # absolute_archive_path -> temp_dir

    @classmethod
    def get_extracted_path(cls, archive_path):
        """Return path to extracted contents, extracting if needed."""
        abs_path = os.path.abspath(archive_path)
        if abs_path not in cls._extracted:
            temp_dir = tempfile.mkdtemp(prefix='fairway_archive_')
            print(f"Extracting archive: {archive_path} → {temp_dir}")
            cls._extract(archive_path, temp_dir)
            cls._extracted[abs_path] = temp_dir
        else:
            print(f"Using cached extraction for: {archive_path}")
        return cls._extracted[abs_path]

    @classmethod
    def _extract(cls, archive_path, dest_dir):
        """Extract archive based on type."""
        if archive_path.endswith('.zip'):
            import zipfile
            with zipfile.ZipFile(archive_path, 'r') as zf:
                zf.extractall(dest_dir)
        elif archive_path.endswith(('.tar.gz', '.tgz')):
            import tarfile
            with tarfile.open(archive_path, 'r:gz') as tf:
                tf.extractall(dest_dir)
        elif archive_path.endswith('.tar'):
            import tarfile
            with tarfile.open(archive_path, 'r') as tf:
                tf.extractall(dest_dir)
        else:
            raise ValueError(f"Unsupported archive type: {archive_path}")

    @classmethod
    def cleanup(cls):
        """Remove all extracted temp directories."""
        import shutil
        for path, temp_dir in cls._extracted.items():
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        cls._extracted.clear()
```

**Usage in preprocessing**:
```python
def _preprocess(self, table):
    archives_pattern = table.get('archives')
    if archives_pattern:
        # Find matching archives
        root = table.get('root', '')
        search_path = os.path.join(root, archives_pattern) if root else archives_pattern
        archive_files = glob.glob(search_path)

        if not archive_files:
            raise ConfigValidationError([f"No archives match pattern: {search_path}"])

        # Extract each archive (cached) and collect file paths
        all_files = []
        files_pattern = table.get('files', '*')
        for archive in archive_files:
            extracted_dir = ArchiveCache.get_extracted_path(archive)
            matched = glob.glob(os.path.join(extracted_dir, files_pattern), recursive=True)
            all_files.extend(matched)

        # Update table path to extracted files
        table['_extracted_files'] = all_files
        return all_files

    # ... existing path-based logic
```

**Partial runs / Resume behavior**:
Uses persistent extraction with manifest tracking. On resume, skips extraction if archive unchanged.

**Persistent extraction location**:
```python
# Extract to deterministic location based on archive path + hash
def _get_extraction_dir(self, archive_path):
    archive_hash = hashlib.md5(open(archive_path, 'rb').read()).hexdigest()[:8]
    archive_name = os.path.basename(archive_path).replace('.', '_')
    # Use temp_location from config, or default
    base_dir = self.config.temp_location or os.path.join(os.getcwd(), '.fairway_cache')
    return os.path.join(base_dir, 'archives', f"{archive_name}_{archive_hash}")
```

**Manifest tracking for extractions**:
```python
# In manifest.py - add extraction tracking
def record_extraction(self, archive_path, extracted_dir, archive_hash):
    """Record that an archive was extracted."""
    key = f"archive:{os.path.abspath(archive_path)}"
    self.data['extractions'] = self.data.get('extractions', {})
    self.data['extractions'][key] = {
        'extracted_dir': extracted_dir,
        'archive_hash': archive_hash,
        'extracted_at': datetime.now().isoformat()
    }
    self._save()

def get_extraction(self, archive_path):
    """Get existing extraction if archive unchanged."""
    key = f"archive:{os.path.abspath(archive_path)}"
    return self.data.get('extractions', {}).get(key)

def is_extraction_valid(self, archive_path):
    """Check if existing extraction is still valid."""
    entry = self.get_extraction(archive_path)
    if not entry:
        return False
    # Check archive hasn't changed
    current_hash = hashlib.md5(open(archive_path, 'rb').read()).hexdigest()[:8]
    if entry['archive_hash'] != current_hash:
        return False
    # Check extracted dir still exists
    if not os.path.isdir(entry['extracted_dir']):
        return False
    return True
```

**Updated ArchiveCache with persistence**:
```python
class ArchiveCache:
    """Persistent cache for extracted archives."""

    def __init__(self, config, manifest):
        self.config = config
        self.manifest = manifest
        self._session_cache = {}  # in-memory for current run

    def get_extracted_path(self, archive_path):
        """Return path to extracted contents, extracting if needed."""
        abs_path = os.path.abspath(archive_path)

        # Check in-memory cache first (same-run optimization)
        if abs_path in self._session_cache:
            return self._session_cache[abs_path]

        # Check manifest for valid existing extraction
        if self.manifest.is_extraction_valid(archive_path):
            entry = self.manifest.get_extraction(archive_path)
            print(f"Using cached extraction for: {archive_path}")
            self._session_cache[abs_path] = entry['extracted_dir']
            return entry['extracted_dir']

        # Need to extract
        extraction_dir = self._get_extraction_dir(archive_path)
        os.makedirs(extraction_dir, exist_ok=True)

        print(f"Extracting archive: {archive_path} → {extraction_dir}")
        self._extract(archive_path, extraction_dir)

        # Record in manifest
        archive_hash = hashlib.md5(open(archive_path, 'rb').read()).hexdigest()[:8]
        self.manifest.record_extraction(archive_path, extraction_dir, archive_hash)

        self._session_cache[abs_path] = extraction_dir
        return extraction_dir
```

**Cleanup**: Extracted archives persist in `.fairway_cache/archives/` until manually cleaned or archive changes.

**Files**: `pipeline.py`, `manifest.py`, `config_loader.py` (for temp_location)

---

## Chunk D: Performance Fixes

### D.1: Remove/Disable Salting

**Changes**:
1. Default `balanced=False` in `ingest()`
2. Add `performance.salting` config option (opt-in)
3. Pass config value through pipeline

**Config**:
```yaml
performance:
  salting: false  # default, set true to enable
```

**Files**: `config_loader.py`, `pyspark_engine.py`, `pipeline.py`

---

### D.2: Increase Parquet File Size

**Target**: 128-256MB per file

**Changes**:
```python
# In pyspark_engine.py:
writer = writer.option("maxRecordsPerFile", rows_per_file)
writer = writer.option("compression", "snappy")
```

**Config**:
```yaml
performance:
  target_file_size_mb: 128
  compression: "snappy"
```

**Files**: `config_loader.py`, `pyspark_engine.py`

---

### D.3: SLURM Tuning Guide

**Create**: `docs/slurm_tuning.md` with:
- Default values and when to change
- Memory sizing rules
- Common scenario templates

---

### D.4: Scratch Output Location

**Config**:
```yaml
storage:
  scratch_dir: "/scratch/$USER/fairway"
```

**Implementation**: Use scratch for intermediate, copy to final on success.

**Files**: `config_loader.py`, `pipeline.py`

---

## Implementation Order

### Step 1: Core Rename (C.1 + C.5)
Do these together since C.5 validation uses the new terminology.

| Order | File | Description |
|-------|------|-------------|
| 1.1 | `config_loader.py` | Rename sources→tables, add validation |
| 1.2 | `pipeline.py` | Update all source→table references |
| 1.3 | `schema_pipeline.py` | Update sources_info→tables_info |
| 1.4 | `manifest.py` | Update source_name→table_name params |
| 1.5 | `cli.py` | Update cfg.sources→cfg.tables |
| 1.6 | `pyspark_engine.py` | Update source_root param |
| 1.7 | `fairway.yaml` | Update template |
| 1.8 | `getting-started.md` | Update docs |

### Step 2: Quick Wins
| Task | Risk |
|------|------|
| D.1 Disable salting default | LOW |
| C.4 Add --dry-run | LOW |
| D.2 Parquet file size | LOW |

### Step 3: Documentation
| Task | Risk |
|------|------|
| C.2 Root documentation | LOW |
| D.3 SLURM guide | LOW |

### Step 4: Archive Handling (C.3 + C.7 together)
| Order | File | Description |
|-------|------|-------------|
| 4.1 | `config_loader.py` | Add `archives` and `files` config keys |
| 4.2 | `manifest.py` | Add extraction tracking methods |
| 4.3 | `pipeline.py` | Add `ArchiveCache` class with persistence |
| 4.4 | Validation | Add archive/files validation rules |

### Step 5: Other Changes
| Task | Risk |
|------|------|
| D.4 Scratch location | MEDIUM |

---

## Breaking Changes

This phase introduces breaking changes:
- Config key `sources:` renamed to `tables:`
- All existing fairway.yaml configs must be updated
- Manifest keys change from `source_name` to `table_name`

---

## Test Requirements

```python
# tests/test_phase2.py

# C.1: Terminology rename
def test_config_loads_tables_key(): pass
def test_config_tables_attribute_exists(): pass
def test_get_table_by_name(): pass

# C.5: Validation
def test_validation_error_missing_name(): pass
def test_validation_error_missing_path(): pass
def test_validation_error_duplicate_table_name(): pass
def test_validation_error_invalid_format(): pass
def test_validation_error_schema_not_found(): pass
def test_validation_error_path_not_found(): pass
def test_validation_error_root_not_found(): pass
def test_validation_warning_glob_no_matches(): pass

# C.3/C.7: Archive handling
def test_archive_extraction(): pass
def test_archive_files_pattern_filtering(): pass
def test_archive_cache_extracts_once(): pass
def test_multi_table_same_archive(): pass
def test_archive_without_files_errors(): pass
def test_files_without_archive_errors(): pass
def test_archive_cache_persists_across_runs(): pass
def test_archive_cache_invalidates_on_change(): pass
def test_archive_extraction_recorded_in_manifest(): pass

# C.4: Dry run
def test_dry_run_shows_matched_files(): pass
def test_dry_run_no_processing(): pass

# D.1: Salting
def test_salting_disabled_by_default(): pass
def test_salting_opt_in(): pass

# D.2: File size
def test_parquet_file_size_config(): pass
```
