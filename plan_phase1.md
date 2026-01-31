# Phase 1: Testing Foundation & Schema Merging Fix

## Overview

Phase 1 establishes a test safety net and fixes the schema merging bug before making other changes.

**Chunks Covered:**
- Chunk A: Testing Foundation
- Chunk B: Schema Merging Fix

---

## Current State Assessment

### Existing Tests

| File | Coverage |
|------|----------|
| `tests/test_schema_evolution.py` | PySpark schema validation, type enforcement |
| `tests/test_ingestion_formats.py` | DuckDB engine format reading |
| `tests/conftest.py` | Does not exist - needs creation |

### Schema Inference - Current Behavior

**DuckDB Engine** (`src/fairway/engines/duckdb_engine.py`):
- `infer_schema()` method at lines 157-238
- **Issue:** May sample only first file(s) instead of all files
- Uses `DESCRIBE` or reads sample to get schema

**PySpark Engine** (`src/fairway/engines/pyspark_engine.py`):
- `infer_schema()` method at lines 434-499
- Likely works correctly (Spark's native schema merge)
- May need logging improvements

**Schema Pipeline** (`src/fairway/schema_pipeline.py`):
- Orchestrates schema generation
- Calls engine's `infer_schema()`
- Writes output to schema files

---

## Chunk A: Testing Foundation

### A.1: Audit Existing Tests

**Goal:** Document what's currently tested

**Actions:**
1. Run `pytest --collect-only` to list all tests
2. Document coverage by module
3. Identify gaps

**Acceptance Criteria:**
- Markdown table of existing tests and what they cover
- List of untested modules/functions

---

### A.2: Create Dummy Datasets

**Goal:** Test data for each supported format

**Directory Structure:**
```
tests/fixtures/
├── formats/
│   ├── csv/
│   │   ├── simple.csv
│   │   └── with_headers.csv
│   ├── tsv/
│   │   └── simple.tsv
│   ├── json/
│   │   ├── records.json
│   │   └── lines.jsonl
│   └── parquet/
│       └── simple.parquet
```

**CSV Test Data (`simple.csv`):**
```csv
id,name,value
1,alice,100
2,bob,200
3,carol,300
```

**JSON Test Data (`records.json`):**
```json
[
  {"id": 1, "name": "alice", "value": 100},
  {"id": 2, "name": "bob", "value": 200}
]
```

**Acceptance Criteria:**
- Files created for CSV, TSV, JSON, JSONL, Parquet
- Each file has consistent schema (id, name, value)
- Files are small but representative

---

### A.3: Schema Merge Test Case

**Goal:** Files with different column sets to verify merge behavior

**Directory Structure:**
```
tests/fixtures/schema_merge/
├── file_a.csv    # id, name, age
├── file_b.csv    # id, name, city
├── file_c.csv    # id, name, country
├── config.yaml   # Test config pointing to these files
└── expected_schema.yaml  # Expected merged result
```

**file_a.csv:**
```csv
id,name,age
1,alice,30
2,bob,25
```

**file_b.csv:**
```csv
id,name,city
1,alice,NYC
2,bob,LA
```

**file_c.csv:**
```csv
id,name,country
1,alice,USA
2,bob,USA
```

**expected_schema.yaml:**
```yaml
id: BIGINT
name: VARCHAR
age: BIGINT
city: VARCHAR
country: VARCHAR
```

**Acceptance Criteria:**
- Three files with overlapping + unique columns
- Expected schema contains ALL columns (superset)
- Config file ready to use with `fairway generate_schema`

---

### A.4: Zip Handling Test Case

**Goal:** Test zip file extraction scenarios and document current behavior

**Directory Structure:**
```
tests/fixtures/zip_handling/
├── single_file.zip           # Contains one data.csv
├── multi_file_same_schema.zip    # Multiple CSVs, same columns
├── multi_file_diff_schema.zip    # Multiple CSVs, different columns (schema merge)
├── multi_table.zip           # Files for different tables (sales_*.csv, customers_*.csv)
├── nested_dirs.zip           # Subdirectories inside zip
├── config_single.yaml        # Config for single table from zip
├── config_multi_table.yaml   # Config showing workaround for multiple tables
└── README.md                 # Documents current zip handling behavior
```

**Test Cases:**

| Test | Description |
|------|-------------|
| Single file extraction | Basic: one zip, one CSV |
| Multiple files same schema | One zip, multiple CSVs, all same columns |
| Multiple files different schema | One zip, CSVs with different columns → schema merge |
| Multiple tables (workaround) | Two sources pointing to same zip with different `include` patterns |
| Nested directories | Zip with subdirs, verify `recursiveFileLookup` works |

**Multi-Table Workaround Test (`config_multi_table.yaml`):**
```yaml
# Current workaround: multiple sources, same zip, different include patterns
sources:
  - name: "sales"
    path: "tests/fixtures/zip_handling/multi_table.zip"
    format: "csv"
    preprocess:
      action: "unzip"
      include: "sales_*.csv"

  - name: "customers"
    path: "tests/fixtures/zip_handling/multi_table.zip"
    format: "csv"
    preprocess:
      action: "unzip"
      include: "customers_*.csv"
```

**Documentation (`README.md` in fixture dir):**
Document current zip handling behavior:
- `path` matches zip files, `format` is what's INSIDE
- `include` filters files inside zip
- Multiple tables in one zip requires multiple source entries
- Note: This is a workaround; better solution planned for Config Redesign (Chunk C)

**Acceptance Criteria:**
- All test cases pass
- README documents current behavior and limitations
- Multi-table workaround is tested and documented

---

### A.5: Pipeline Step Tests

**Goal:** Tests for preprocessing, transformation, validation

**Test Files to Create:**
```
tests/
├── test_preprocessing.py    # unzip, custom scripts
├── test_transformation.py   # custom transform scripts
├── test_validation.py       # level1, level2 checks
└── test_pipeline_e2e.py     # Full pipeline run
```

**test_preprocessing.py:**
```python
def test_unzip_single_file(duckdb_engine, fixtures_dir):
    """Test unzipping a single CSV from zip."""
    pass

def test_unzip_multiple_files(duckdb_engine, fixtures_dir):
    """Test unzipping multiple CSVs from zip."""
    pass
```

**test_validation.py:**
```python
def test_level1_min_rows(duckdb_engine):
    """Test level1 validation fails when row count too low."""
    pass

def test_level2_null_check(duckdb_engine):
    """Test level2 validation detects null values."""
    pass
```

**Acceptance Criteria:**
- Tests for each pipeline step
- Tests use fixtures from A.2-A.4
- All tests pass on current code (baseline)

---

### A.6: Update rules.md with Testing Standards

**Goal:** Document the process for adding tests when new file formats are added

**Add to `rules.md`:**
```markdown
### [RULE-116] Test Coverage for File Formats

**Priority:** MUST
**Category:** [PRO] Process & Workflow

**Rule:**
When adding support for a new file format (e.g., fixed-width, Avro, ORC):
1. Add dummy test data to `tests/fixtures/formats/<format_name>/`
2. Add schema merge test case to `tests/fixtures/schema_merge/` with that format
3. Add test in `tests/test_ingestion_formats.py` covering read + schema inference
4. Update the checklist in `tests/fixtures/README.md`

**Checklist for new formats:**
- [ ] `tests/fixtures/formats/<format>/sample.<ext>` — Basic file
- [ ] `tests/fixtures/schema_merge/<format>/` — Files with different columns
- [ ] `tests/test_ingestion_formats.py::test_<format>_basic_read`
- [ ] `tests/test_schema_merge.py::test_<format>_schema_union`
```

**Also create `tests/fixtures/README.md`:**
- Document fixture directory structure
- List supported formats and their test files
- Include checklist template for new formats

**Acceptance Criteria:**
- Rule added to `rules.md`
- `tests/fixtures/README.md` created with structure documentation
- Checklist usable for future format additions (including fixed-width in Chunk F)

---

### A.7: Fixed-Width Test Fixture Preparation

**Goal:** Prepare test infrastructure for fixed-width support (Chunk F)

**Directory Structure:**
```
tests/fixtures/formats/fixed_width/
├── simple.txt              # Basic fixed-width data
├── simple_spec.yaml        # Column spec for simple.txt
├── multi_schema/
│   ├── file_a.txt          # Different column widths
│   ├── file_a_spec.yaml
│   ├── file_b.txt
│   └── file_b_spec.yaml
└── README.md               # Documents fixed-width spec format
```

**Sample Fixed-Width Data (`simple.txt`):**
```
ID        NAME                AGE
001       Alice               030
002       Bob                 025
003       Carol               028
```

**Sample Spec (`simple_spec.yaml`):**
```yaml
columns:
  - name: ID
    start: 0
    length: 10
    type: STRING
  - name: NAME
    start: 10
    length: 20
    type: STRING
  - name: AGE
    start: 30
    length: 3
    type: INTEGER
```

**Note:** This prepares fixtures for Chunk F (Fixed-Width Support). Actual reader implementation is not part of Phase 1.

**Acceptance Criteria:**
- Fixture files created
- Spec format documented in README
- Ready for Chunk F implementation

---

## Chunk B: Schema Merging Fix

### B.1: Document Current Behavior

**Goal:** Trace how schema inference works today

**Files to Analyze:**
- `src/fairway/schema_pipeline.py` - entry point
- `src/fairway/engines/duckdb_engine.py` - `infer_schema()` lines 157-238
- `src/fairway/engines/pyspark_engine.py` - `infer_schema()` lines 434-499

**Questions to Answer:**
1. How are files discovered? (glob pattern?)
2. How many files are read for schema?
3. How are schemas merged across files?
4. What happens with type conflicts?

**Acceptance Criteria:**
- Documented flow from CLI → schema file
- Identified where sampling occurs
- Documented current merge logic

---

### B.2: Identify Sampling Issue

**Goal:** Determine if schema inference reads all files or samples

**Investigation Steps:**
1. Add temporary debug logging
2. Run against test case A.3
3. Check which files were read

**Expected Finding:**
- DuckDB: Likely reads only first file or samples N files
- PySpark: Likely reads all (Spark's mergeSchema option)

**Acceptance Criteria:**
- Confirmed root cause of missing columns
- Documented which engine(s) have the issue

---

### B.3: Fix Schema Union (Two-Phase Approach)

**Goal:** Capture ALL columns with accurate types while maintaining performance

**Key Insight:** Phase 2 must sample files that cover ALL columns discovered in Phase 1. Otherwise, some columns default to STRING without seeing real data.

**DuckDB Fix Approach (Two-Phase with Coverage Guarantee):**
```python
def infer_schema(self, path, format='csv', sample_files=50, **kwargs):
    all_files = sorted(glob.glob(path))  # Sort for determinism

    # Phase 1: Column discovery from ALL files (fast - headers only)
    # Also track which files have which columns
    all_columns = set()
    column_sources = {}  # {column_name: [files_that_have_it]}
    errors = []

    for f in all_files:
        try:
            cols = self._get_column_names_only(f, format)
            all_columns.update(cols)
            for col in cols:
                column_sources.setdefault(col, []).append(f)
        except Exception as e:
            errors.append((f, str(e)))
            continue

    if errors:
        logger.warning(f"Failed to read {len(errors)} files during column discovery")

    # Phase 2: Smart sampling - MUST cover all columns
    # Step 2a: Ensure at least one file per column (minimum required set)
    required_files = set()
    columns_covered = set()

    for col in all_columns:
        if col not in columns_covered:
            # Pick first file that has this column
            source_file = column_sources[col][0]
            required_files.add(source_file)
            # This file covers all its columns
            file_cols = self._get_column_names_only(source_file, format)
            columns_covered.update(file_cols)

    # Step 2b: Add additional samples for type diversity (up to sample_files limit)
    remaining_budget = sample_files - len(required_files)
    if remaining_budget > 0:
        remaining_files = [f for f in all_files if f not in required_files]
        additional = remaining_files[:remaining_budget]  # Deterministic, not random
        sample = list(required_files) + additional
    else:
        sample = list(required_files)

    logger.info(f"Phase 2: Sampling {len(sample)} files to cover {len(all_columns)} columns")

    # Step 2c: Infer types from sampled files
    column_types = {}
    for f in sample:
        file_schema = self._infer_types_from_file(f, format, **kwargs)
        for col, dtype in file_schema.items():
            if col not in column_types:
                column_types[col] = dtype
            else:
                column_types[col] = self._resolve_type_conflict(column_types[col], dtype)

    # Combine: ALL columns from Phase 1, types from Phase 2
    # Every column WILL have a type (no defaults needed due to coverage guarantee)
    final_schema = {}
    for col in all_columns:
        if col in column_types:
            final_schema[col] = column_types[col]
        else:
            # Should never happen with coverage guarantee, but safety fallback
            logger.warning(f"Column '{col}' has no type - defaulting to STRING")
            final_schema[col] = 'STRING'

    return final_schema
```

**Helper Methods to Add:**
```python
def _get_column_names_only(self, path, format):
    """Fast column name extraction (header only)."""
    # For CSV: DESCRIBE or read first row
    # For Parquet: schema metadata
    # For JSON: scan first record
    pass

def _resolve_type_conflict(self, type_a, type_b):
    """Return broader type when conflict occurs."""
    TYPE_HIERARCHY = ['BOOLEAN', 'INTEGER', 'BIGINT', 'DOUBLE', 'STRING']
    # Higher index = broader type; STRING always wins
    idx_a = TYPE_HIERARCHY.index(type_a) if type_a in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
    idx_b = TYPE_HIERARCHY.index(type_b) if type_b in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
    return TYPE_HIERARCHY[max(idx_a, idx_b)]
```

**Why Coverage Guarantee Matters:**
```
Without coverage guarantee:
  Phase 1 discovers: {id, name, age, city, country}
  Phase 2 samples: file_a (id,name,age), file_b (id,name,city)
  Result: 'country' defaults to STRING ← never saw real data!

With coverage guarantee:
  Phase 1 discovers: {id, name, age, city, country}
  Phase 2 MUST include file_c (has 'country')
  Result: 'country' gets real type from actual data ✓
```

**Files to Modify:**
- `src/fairway/engines/duckdb_engine.py`
- `src/fairway/engines/pyspark_engine.py` (if needed)

**Acceptance Criteria:**
- Schema contains ALL columns from ALL files
- Performance scales to 10,000+ files (Phase 1 only reads headers)
- Type conflicts resolved (broader type wins)
- Deterministic output (files sorted before processing)
- Partial failures logged but don't break inference
- No regression in existing tests

---

### B.4: Add Verbose Logging

**Goal:** Log which files contributed to schema

**Logging Output:**
```
INFO: Found 3 files matching pattern: tests/fixtures/schema_merge/*.csv
  - tests/fixtures/schema_merge/file_a.csv
  - tests/fixtures/schema_merge/file_b.csv
  - tests/fixtures/schema_merge/file_c.csv
INFO: Schema contains 5 columns:
  id: BIGINT
  name: VARCHAR
  age: BIGINT (from file_a.csv)
  city: VARCHAR (from file_b.csv)
  country: VARCHAR (from file_c.csv)
```

**Files to Modify:**
- `src/fairway/engines/duckdb_engine.py`
- `src/fairway/engines/pyspark_engine.py`
- `src/fairway/schema_pipeline.py`

**Acceptance Criteria:**
- Logs show all files discovered
- Logs show final merged schema
- Verbosity controllable via parameter

---

### B.5: Validate Fix

**Goal:** Confirm fix works against test case A.3

**Validation Steps:**
1. Run `fairway generate_schema` against schema_merge fixtures
2. Compare output to expected_schema.yaml
3. All 5 columns present (id, name, age, city, country)

**Automated Test:**
```python
# tests/test_schema_merge.py
def test_schema_captures_all_columns():
    """Verify schema merging captures superset of columns."""
    engine = DuckDBEngine()
    schema = engine.infer_schema(
        path="tests/fixtures/schema_merge/*.csv",
        format="csv"
    )

    assert "id" in schema
    assert "name" in schema
    assert "age" in schema      # Only in file_a.csv
    assert "city" in schema     # Only in file_b.csv
    assert "country" in schema  # Only in file_c.csv
```

**Acceptance Criteria:**
- Test A.3 passes with fix applied
- No regression in existing tests

---

## Local Testing Strategy

**Environment:** Local + Java (PySpark local mode)

**Requirements:**
- Python 3.10+
- Java 17+ (for Spark)
- `pip install -e ".[dev]"` or equivalent

**Test Tiers:**

| Tier | Marker | Requires | Run Command |
|------|--------|----------|-------------|
| Local (DuckDB only) | `@pytest.mark.local` | Python, DuckDB | `pytest -m "local"` |
| Spark (local mode) | `@pytest.mark.spark` | Python, Java, Spark | `pytest -m "spark"` |
| All local tests | (default) | Python, Java, Spark | `pytest tests/` |
| HPC only | `@pytest.mark.hpc` | SLURM cluster | `pytest -m "hpc"` (on HPC) |

**Default:** Run all tests locally with Spark in local mode:
```bash
# Activate venv
source .venv/bin/activate

# Run all tests (Spark uses local[*] mode)
pytest tests/

# Run only DuckDB tests (no Java needed)
pytest tests/ -m "local"

# Run with coverage
pytest tests/ --cov=fairway --cov-report=html
```

**Spark Local Mode Config:**
Tests use `SparkSession.builder.master("local[*]")` — no cluster needed.

**Auto-Skip for Missing Dependencies:**
```python
# In conftest.py
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "local: runs without Spark")
    config.addinivalue_line("markers", "spark: requires PySpark + Java")
    config.addinivalue_line("markers", "hpc: requires SLURM cluster")

@pytest.fixture
def spark_available():
    try:
        from pyspark.sql import SparkSession
        return True
    except ImportError:
        return False

# Auto-skip spark tests if Java/Spark not available
def pytest_collection_modifyitems(config, items):
    try:
        from pyspark.sql import SparkSession
        spark_ok = True
    except:
        spark_ok = False

    if not spark_ok:
        skip_spark = pytest.mark.skip(reason="PySpark/Java not available")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip_spark)
```

**Test Both Engines:**
Most tests should run against both DuckDB and PySpark:

```python
@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        from fairway.engines.pyspark_engine import PySparkEngine
        return PySparkEngine(master="local[*]")

def test_schema_inference(engine, tmp_path):
    """Runs against both engines."""
    # ... test code
```

---

## Shared Test Infrastructure

---

### A.8: Set Up Test Infrastructure

**Goal:** Configure pytest with markers, fixtures, and dual-engine support

**Create `tests/conftest.py`:**
```python
import pytest
from pathlib import Path

# ============ Markers ============
def pytest_configure(config):
    config.addinivalue_line("markers", "local: runs without Spark (DuckDB only)")
    config.addinivalue_line("markers", "spark: requires PySpark + Java")
    config.addinivalue_line("markers", "hpc: requires SLURM cluster")

# ============ Auto-skip Spark if unavailable ============
def pytest_collection_modifyitems(config, items):
    try:
        from pyspark.sql import SparkSession
        spark_ok = True
    except:
        spark_ok = False

    if not spark_ok:
        skip_spark = pytest.mark.skip(reason="PySpark/Java not available")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip_spark)

# ============ Fixtures ============
@pytest.fixture
def fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def temp_output(tmp_path):
    """Temporary output directory."""
    output = tmp_path / "output"
    output.mkdir()
    return output

@pytest.fixture
def duckdb_engine():
    """DuckDB engine instance."""
    from fairway.engines.duckdb_engine import DuckDBEngine
    return DuckDBEngine()

@pytest.fixture
def pyspark_engine():
    """PySpark engine in local mode."""
    from fairway.engines.pyspark_engine import PySparkEngine
    return PySparkEngine(master="local[*]")

@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    """Parametrized fixture - tests run against both engines."""
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        pytest.importorskip("pyspark")
        from fairway.engines.pyspark_engine import PySparkEngine
        return PySparkEngine(master="local[*]")

@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()
```

**Create `pytest.ini` or add to `pyproject.toml`:**
```ini
[tool.pytest.ini_options]
markers = [
    "local: runs without Spark (DuckDB only)",
    "spark: requires PySpark + Java",
    "hpc: requires SLURM cluster",
]
testpaths = ["tests"]
```

**Acceptance Criteria:**
- `pytest tests/` runs all local tests (both engines if Java available)
- `pytest tests/ -m "local"` runs DuckDB-only tests
- Tests auto-skip Spark if Java/PySpark unavailable
- Dual-engine tests use `engine` fixture

---

## Execution Order

| Order | Task | Prerequisite |
|-------|------|--------------|
| 1 | A.1 Audit existing tests | None |
| 2 | A.8 Set up test infrastructure | None |
| 3 | A.2 Create dummy datasets | None |
| 4 | A.3 Schema merge test case | None |
| 5 | A.6 Update rules.md | None |
| 6 | A.7 Fixed-width fixture prep | None |
| 7 | B.1 Document current behavior | None |
| 8 | B.2 Identify sampling issue | B.1 |
| 9 | B.3 Fix schema union | B.2 |
| 10 | B.4 Add verbose logging | B.3 |
| 11 | B.5 Validate fix | A.3, A.8, B.3 |
| 12 | A.4 Zip handling test case | A.2, A.8 |
| 13 | A.5 Pipeline step tests | A.2, A.3, A.4, A.8 |

---

## Dependencies Diagram

```
A.1 (Audit) ───────────────────────────────────┐
                                               │
A.8 (Test Infrastructure) ─────────────────────┤
                                               │
A.2 (Dummy Data) ──────────────────────────────┤
                                               ├──→ A.5 (Pipeline Tests)
A.3 (Schema Merge Data) ───────────────────────┤          │
                                               │          │
A.4 (Zip Data) ────────────────────────────────┤          │
                                               │          │
A.6 (Rules.md) ────────────────────────────────┤          │
                                               │          │
A.7 (Fixed-Width Prep) ────────────────────────┘          │
                                                          │
B.1 (Document Current) ────────────────────────────────────┤
                                                          │
B.2 (Identify Issue) ──────────────────────────────────────┤
                                                          ▼
                                              B.3 (Fix Schema Union)
                                                          │
                                                          ▼
                                              B.4 (Verbose Logging)
                                                          │
                                                          ▼
                                              B.5 (Validate) ◄── Uses A.3, A.8
```

---

## Risks and Edge Cases

| Risk | Severity | Mitigation |
|------|----------|------------|
| Performance with 1000s of files | MEDIUM | Two-phase: 1 row/file for columns, sample for types |
| Type conflicts (INT vs STRING) | HIGH | Log warning, use broader type |
| Memory with massive file counts | LOW | Stream file list |
| Breaking existing workflows | HIGH | Add parameter, default to current behavior initially |

**Edge Cases to Test:**
1. Empty files (0 rows)
2. Files with only headers
3. Mixed delimiters
4. Unicode column names
5. Wide schemas (100+ columns)
6. Type variations across files

---

## TDD Review Findings

### Critical: Write Failing Tests FIRST

The test plan must follow TDD - write tests that FAIL before fixing code.

**Most Critical "Red" Test:**
```python
# tests/test_schema_merge.py
class TestSchemaMergeBug:
    """Tests that prove the schema merging bug exists."""

    def test_single_file_sampling_misses_columns(self, tmp_path):
        """
        RED TEST: This MUST fail before the fix.
        Proves that sampling only one file misses columns from other files.
        """
        from fairway.engines.duckdb_engine import DuckDBEngine
        engine = DuckDBEngine()

        # Create files with different columns
        (tmp_path / "file_a.csv").write_text("id,name,age\n1,alice,30\n")
        (tmp_path / "file_b.csv").write_text("id,name,city\n1,bob,NYC\n")
        (tmp_path / "file_c.csv").write_text("id,name,country\n1,carol,USA\n")

        # Force single-file sampling to trigger the bug
        schema = engine.infer_schema(
            path=str(tmp_path / "*.csv"),
            format="csv",
            sample_files=1  # This forces the bug to manifest
        )

        # This WILL FAIL before fix - only columns from sampled file present
        assert "age" in schema, "Missing 'age' column from file_a.csv"
        assert "city" in schema, "Missing 'city' column from file_b.csv"
        assert "country" in schema, "Missing 'country' column from file_c.csv"
```

### Additional TDD Tests Needed

**Type Conflict Test:**
```python
def test_type_conflict_uses_broader_type(self, tmp_path):
    """When same column has different types, use broader type."""
    engine = DuckDBEngine()

    (tmp_path / "ints.csv").write_text("id,value\n1,100\n")
    (tmp_path / "strings.csv").write_text("id,value\n2,hello\n")

    schema = engine.infer_schema(path=str(tmp_path / "*.csv"), format="csv")

    # Should use STRING (broader) not INTEGER
    assert schema["value"] == "VARCHAR" or schema["value"] == "STRING"
```

**Determinism Test:**
```python
def test_schema_inference_is_deterministic(self, tmp_path):
    """Same files should always produce same schema."""
    engine = DuckDBEngine()

    for i in range(5):
        (tmp_path / f"file_{i}.csv").write_text(f"id,col_{i}\n1,val\n")

    # Run multiple times
    schemas = [
        engine.infer_schema(path=str(tmp_path / "*.csv"), format="csv")
        for _ in range(5)
    ]

    # All should be identical
    assert all(s == schemas[0] for s in schemas), "Schema inference is non-deterministic"
```

**Empty File Test:**
```python
def test_empty_file_handled_gracefully(self, tmp_path):
    """Empty files should not break schema inference."""
    engine = DuckDBEngine()

    (tmp_path / "empty.csv").write_text("")
    (tmp_path / "valid.csv").write_text("id,name\n1,alice\n")

    schema = engine.infer_schema(path=str(tmp_path / "*.csv"), format="csv")

    assert "id" in schema
    assert "name" in schema
```

**Coverage Guarantee Test (Critical):**
```python
def test_all_columns_get_real_types_not_defaults(self, tmp_path):
    """
    Every column must get a type from actual data, not default to STRING.
    This tests the coverage guarantee in Phase 2 sampling.
    """
    engine = DuckDBEngine()

    # Create files where each has a unique column
    (tmp_path / "file_a.csv").write_text("id,name,age\n1,alice,30\n")
    (tmp_path / "file_b.csv").write_text("id,name,salary\n1,bob,50000.50\n")
    (tmp_path / "file_c.csv").write_text("id,name,is_active\n1,carol,true\n")

    # Even with sample_files=1, coverage guarantee should include all unique columns
    schema = engine.infer_schema(
        path=str(tmp_path / "*.csv"),
        format="csv",
        sample_files=1  # Restrictive, but coverage should override
    )

    # All columns present
    assert "age" in schema
    assert "salary" in schema
    assert "is_active" in schema

    # Types inferred from real data, not defaulted to STRING
    assert schema["age"] in ("INTEGER", "BIGINT", "INT")
    assert schema["salary"] in ("DOUBLE", "FLOAT", "DECIMAL")
    assert schema["is_active"] in ("BOOLEAN", "BOOL")
```

**Minimum Sample Set Test:**
```python
def test_minimum_sample_covers_all_columns(self, tmp_path):
    """
    With many files but few unique column sets, sampling should be minimal.
    """
    engine = DuckDBEngine()

    # 100 files but only 2 unique column sets
    for i in range(50):
        (tmp_path / f"type_a_{i}.csv").write_text("id,name,age\n1,x,30\n")
    for i in range(50):
        (tmp_path / f"type_b_{i}.csv").write_text("id,name,city\n1,x,NYC\n")

    # Should only need 2 files to cover all columns
    schema = engine.infer_schema(
        path=str(tmp_path / "*.csv"),
        format="csv",
        sample_files=5
    )

    assert "age" in schema
    assert "city" in schema
    # Verify we didn't read all 100 files (check logs or add instrumentation)
```

### TDD Execution Order (Updated)

| Order | Task | TDD Phase |
|-------|------|-----------|
| 1 | Create `tests/conftest.py` | Setup |
| 2 | Write `test_single_file_sampling_misses_columns` | RED |
| 3 | Run test - verify it FAILS | RED |
| 4 | Document failure output | RED |
| 5 | Fix `infer_schema()` to read ALL files | GREEN |
| 6 | Run test - verify it PASSES | GREEN |
| 7 | Add type conflict, determinism, empty file tests | REFACTOR |
| 8 | Add verbose logging | REFACTOR |

### Gaps Identified

| Gap | Severity | Action |
|-----|----------|--------|
| No "red" test proving bug | HIGH | Added above |
| No `sample_files` parameter test | HIGH | Force sampling in test |
| No type conflict test | MEDIUM | Added above |
| No determinism test | MEDIUM | Added above |
| No empty file test | MEDIUM | Added above |

---

## Architectural Review Findings

### Critical: Two-Phase Approach for Scalability

**Problem with "read ALL files":** Doesn't scale to 10,000+ files.

**Solution: Two-phase schema inference:**

```python
def infer_schema_two_phase(self, path, format='csv', **kwargs):
    """Two-phase schema inference for scalability."""

    # Phase 1: Discover ALL columns (fast - only reads headers)
    all_files = glob.glob(path, recursive=True)
    all_columns = set()

    for f in all_files:
        # DuckDB DESCRIBE only reads header row - scales to millions of files
        cols = self._get_column_names_only(f, format)
        all_columns.update(cols)

    # Phase 2: Type inference (sample data)
    sample_files = random.sample(all_files, min(50, len(all_files)))
    column_types = self._infer_types_from_sample(sample_files, format)

    # Columns from Phase 1, types from Phase 2
    return {col: column_types.get(col, 'STRING') for col in all_columns}
```

**Why this works:**
- Phase 1: `DESCRIBE` on CSV only reads header - O(N) but fast
- Phase 2: Sampling gives accurate types without reading all data
- Columns never missed; types accurate; scales well

### Engine Interface Consistency

**Current problem:** DuckDB and PySpark have different signatures:

```python
# DuckDB
def infer_schema(self, path, format='csv', sampling_ratio=0.1, sample_files=50, rows_per_file=1000, **kwargs)

# PySpark
def infer_schema(self, path, format='csv', sampling_ratio=1.0, **kwargs)
```

**Recommendation:** Define common interface (can defer to Phase 2+ if needed):

```python
# src/fairway/engines/base.py
from abc import ABC, abstractmethod
from typing import Dict

class EngineBase(ABC):
    @abstractmethod
    def infer_schema(
        self,
        path: str,
        format: str = 'csv',
        **kwargs
    ) -> Dict[str, str]:
        """Infer schema from files. Returns {column_name: type_string}."""
        pass
```

### Determinism Issue

**Problem:** `random.sample()` causes non-deterministic schema output.

**Fix options:**
1. Seed the RNG: `random.seed(42)`
2. Deterministic sampling: `files[::step]` (every Nth file)
3. Sort files first: `sorted(all_files)`

**Recommendation:** Sort files before sampling for reproducibility.

### Error Handling for Partial Failures

**Problem:** One corrupted file breaks entire schema inference.

**Fix:**
```python
schemas = []
errors = []

for f in files_to_scan:
    try:
        schemas.append(self._get_schema(f))
    except Exception as e:
        errors.append((f, str(e)))
        continue

if errors:
    logger.warning(f"Failed to read {len(errors)} files: {errors[:5]}...")
```

### Type Conflict Resolution

**Problem:** Same column might be INT in file_a, STRING in file_b.

**Solution:** Use type hierarchy - broader type wins:

```python
TYPE_HIERARCHY = ['BOOLEAN', 'INTEGER', 'BIGINT', 'DOUBLE', 'STRING']

def resolve_type_conflict(type_a: str, type_b: str) -> str:
    """Return broader type. STRING always wins."""
    idx_a = TYPE_HIERARCHY.index(type_a) if type_a in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
    idx_b = TYPE_HIERARCHY.index(type_b) if type_b in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
    return TYPE_HIERARCHY[max(idx_a, idx_b)]
```

### Summary of Architectural Changes for B.3

| Change | Priority | Notes |
|--------|----------|-------|
| Two-phase inference | HIGH | Column names from all, types from sample |
| Sort files before sampling | HIGH | Ensures determinism |
| Error handling | MEDIUM | Don't fail on one bad file |
| Type conflict resolution | MEDIUM | Broader type wins |
| Common engine interface | LOW | Can defer to future phase |

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/fairway/engines/duckdb_engine.py:157-238` | DuckDB `infer_schema()` - primary fix location |
| `src/fairway/engines/pyspark_engine.py:434-499` | PySpark `infer_schema()` - may need logging |
| `src/fairway/schema_pipeline.py` | Schema generation orchestration |
| `tests/test_schema_evolution.py` | Existing schema tests - pattern reference |
| `tests/test_ingestion_formats.py` | Existing format tests - pattern reference |
