# Docker Test Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 40 test failures in `make test-docker` by aligning test fixtures with real pipeline usage, fixing PySpark 4.x type compatibility, and resolving CLI template bugs.

**Architecture:** Source fixes in `pyspark_engine.py` (VARCHAR normalization, remove `__del__`), `cli.py` (Template substitution), template files (placeholder syntax). Test infrastructure fixes in `conftest.py` (real engine constructors, skip policy). Individual test files updated to use shared fixtures.

**Tech Stack:** Python, pytest, PySpark 4.1.1, DuckDB, Click CLI, Docker

---

### Task 1: Remove `__del__` from PySparkEngine

**Files:**
- Modify: `src/fairway/engines/pyspark_engine.py:105-107`

- [ ] **Step 1: Delete the `__del__` method**

In `src/fairway/engines/pyspark_engine.py`, delete lines 105-107:

```python
    def __del__(self):
        """Cleanup on garbage collection."""
        self.stop()
```

- [ ] **Step 2: Verify existing tests still pass locally**

Run: `source .venv/bin/activate && pytest tests/test_performance.py -m "not spark" -q`
Expected: All non-spark tests pass (spark tests may skip locally)

- [ ] **Step 3: Commit**

```bash
git add src/fairway/engines/pyspark_engine.py
git commit -m "fix: remove PySparkEngine.__del__ to prevent logging noise during teardown"
```

---

### Task 2: Add VARCHAR -> STRING normalization in PySpark engine

**Files:**
- Modify: `src/fairway/engines/pyspark_engine.py:253-260`
- Test: `tests/test_schema_enforcement.py` (existing test covers this)

- [ ] **Step 1: Write a focused unit test for VARCHAR normalization**

Add to `tests/test_schema_enforcement.py`:

```python
class TestVarcharNormalization:
    """VARCHAR must be normalized to STRING for PySpark 4.x compatibility."""

    def test_varchar_normalized_to_string_in_cast(self, pyspark_engine, tmp_path):
        """PySpark 4.x rejects bare VARCHAR — engine must normalize to STRING."""
        data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        df = pyspark_engine.spark.createDataFrame(data)
        input_path = str(tmp_path / "input")
        df.write.mode("overwrite").parquet(input_path)

        output_path = str(tmp_path / "output")
        pyspark_engine.ingest(
            input_path, output_path, format="parquet",
            schema={"id": "INTEGER", "name": "VARCHAR"},
        )

        result = pyspark_engine.spark.read.parquet(output_path)
        assert result.count() == 2
        assert "name" in result.columns
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `source .venv/bin/activate && pytest tests/test_schema_enforcement.py::TestVarcharNormalization -v`
Expected: FAIL — `[DATATYPE_MISSING_SIZE] DataType "VARCHAR" requires a length parameter` (or skip if PySpark unavailable locally; will verify in Docker later)

- [ ] **Step 3: Add `_normalize_spark_type` helper and apply it in the schema enforcement path**

In `src/fairway/engines/pyspark_engine.py`, add the helper function before the `PySparkEngine` class definition (after line 12):

```python
def _normalize_spark_type(type_str):
    """Normalize type strings for PySpark 4.x compatibility.

    PySpark 4.x rejects bare 'VARCHAR' — requires 'VARCHAR(n)' or 'STRING'.
    This normalizes 'VARCHAR' (without length) to 'STRING' so user configs
    work across engine versions. VARCHAR(n) is left unchanged.
    """
    if type_str.upper() == 'VARCHAR':
        return 'STRING'
    return type_str
```

Then in the `ingest` method, apply it in the two `.cast()` calls in the schema enforcement block. Change lines 253-260 from:

```python
            for col_name, col_type in schema.items():
                if col_name in df.columns:
                    select_exprs.append(F.col(col_name).cast(col_type))
                else:
                    select_exprs.append(F.lit(None).cast(col_type).alias(col_name))
```

to:

```python
            for col_name, col_type in schema.items():
                spark_type = _normalize_spark_type(col_type)
                if col_name in df.columns:
                    select_exprs.append(F.col(col_name).cast(spark_type))
                else:
                    select_exprs.append(F.lit(None).cast(spark_type).alias(col_name))
```

- [ ] **Step 4: Verify the normalization does NOT affect schema inference (RULE-117)**

Check: the `_normalize_spark_type` function is only called in the schema enforcement block (lines 253-260) where schema is user-provided. The schema inference paths (lines 158-176 for CSV reader schema, and the `infer_schema` method at line 731+) use their own `TYPE_MAP` dict which already maps `VARCHAR` -> `StringType()`. No changes needed there. Confirm by grepping:

Run: `grep -n "_normalize_spark_type" src/fairway/engines/pyspark_engine.py`
Expected: Only appears in the function definition and the two `.cast()` call sites.

- [ ] **Step 5: Run the test to verify it passes**

Run: `source .venv/bin/activate && pytest tests/test_schema_enforcement.py::TestVarcharNormalization -v`
Expected: PASS (or skip if PySpark unavailable locally)

- [ ] **Step 6: Commit**

```bash
git add src/fairway/engines/pyspark_engine.py tests/test_schema_enforcement.py
git commit -m "fix: normalize VARCHAR to STRING for PySpark 4.x compatibility"
```

---

### Task 3: Switch CLI templates from str.format() to string.Template

**Files:**
- Modify: `src/fairway/cli.py:137,170-172,197-199`
- Modify: `src/fairway/data/fairway.yaml:5,10`
- Modify: `src/fairway/data/README.md:1,3,5`
- Modify: `src/fairway/data/getting-started.md:1`
- Test: `tests/test_cli_coverage.py::TestInitCommand` (existing tests cover this)

- [ ] **Step 1: Update template placeholders in fairway.yaml**

In `src/fairway/data/fairway.yaml`, change:

Line 5: `dataset_name: "{name}"` -> `dataset_name: "$name"`

Line 10: `engine: "{engine_type}"` -> `engine: "$engine_type"`

- [ ] **Step 2: Update template placeholders in README.md**

In `src/fairway/data/README.md`, change:

Line 1: `# {name}` -> `# $name`

Line 3: `Initialized by fairway on {timestamp}` -> `Initialized by fairway on $timestamp`

Line 5: `**Engine**: {engine}` -> `**Engine**: $engine`

- [ ] **Step 3: Update template placeholders in getting-started.md**

In `src/fairway/data/getting-started.md`, change:

Line 1: `# Getting Started with {name}` -> `# Getting Started with $name`

Check for any other `{name}` or `{engine_type}` placeholders in this file and update them too.

- [ ] **Step 4: Update cli.py to use string.Template**

In `src/fairway/cli.py`, add import at top (after existing imports):

```python
from string import Template
```

Change line 137 from:

```python
    config_content = CONFIG_TEMPLATE.format(name=name, engine_type=engine_type)
```

to:

```python
    config_content = Template(CONFIG_TEMPLATE).safe_substitute(name=name, engine_type=engine_type)
```

Change lines 170-172 from:

```python
    readme_content = README_TEMPLATE.format(
        name=name,
        timestamp=datetime.now().isoformat(),
        engine=engine
    )
```

to:

```python
    readme_content = Template(README_TEMPLATE).safe_substitute(
        name=name,
        timestamp=datetime.now().isoformat(),
        engine=engine
    )
```

Change lines 197-199 from:

```python
    docs_content = DOCS_TEMPLATE.format(
        name=name,
        engine_type=engine_type
    )
```

to:

```python
    docs_content = Template(DOCS_TEMPLATE).safe_substitute(
        name=name,
        engine_type=engine_type
    )
```

- [ ] **Step 5: Run the CLI init tests**

Run: `source .venv/bin/activate && pytest tests/test_cli_coverage.py::TestInitCommand -v`
Expected: All 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/fairway/cli.py src/fairway/data/fairway.yaml src/fairway/data/README.md src/fairway/data/getting-started.md
git commit -m "fix: switch CLI templates to string.Template to avoid YAML brace conflicts"
```

---

### Task 4: Update schema documentation in fairway.yaml template

**Files:**
- Modify: `src/fairway/data/fairway.yaml:126,131`

- [ ] **Step 1: Update VARCHAR references in template comments**

In `src/fairway/data/fairway.yaml`:

Change line 126 from:

```yaml
  #   #       type: INTEGER # Native engine type (INTEGER, VARCHAR, DOUBLE, etc.)
```

to:

```yaml
  #   #       type: INTEGER # Native engine type (INTEGER, STRING, DOUBLE, etc.) — VARCHAR auto-converted to STRING for Spark compatibility
```

Change line 131 from:

```yaml
  #   #       type: VARCHAR
```

to:

```yaml
  #   #       type: STRING
```

- [ ] **Step 2: Commit**

```bash
git add src/fairway/data/fairway.yaml
git commit -m "docs: note VARCHAR auto-normalization in config template comments"
```

---

### Task 5: Fix test_cli_spark_lifecycle mock setup

**Files:**
- Modify: `tests/test_cli_spark_lifecycle.py:13-29`

- [ ] **Step 1: Fix test_spark_start to create config dir before invoking CLI**

The `spark start` command calls `discover_config()` which requires a `config/` directory. The test runs in `isolated_filesystem()` (empty dir) so it fails before the mock intercepts.

In `tests/test_cli_spark_lifecycle.py`, change `test_spark_start` from:

```python
def test_spark_start(runner):
    """Test fairway spark start command."""
    with runner.isolated_filesystem():
        with patch('fairway.engines.slurm_cluster.SlurmSparkManager') as MockManager:
            # Mock instance
            mock_instance = MockManager.return_value
            mock_instance.start_cluster.return_value = "spark://mock-master:7077"
            
            result = runner.invoke(main, ['spark', 'start', '--slurm-nodes', '4'])
            assert result.exit_code == 0
            assert "Spark cluster started" in result.output
            assert "spark://mock-master:7077" in result.output
            
            # Verify config passed
            MockManager.assert_called_once()
            call_args = MockManager.call_args[0][0]
            assert call_args['slurm_nodes'] == 4
```

to:

```python
def test_spark_start(runner):
    """Test fairway spark start command."""
    with runner.isolated_filesystem():
        os.makedirs('config', exist_ok=True)
        with open('config/fairway.yaml', 'w') as f:
            f.write('dataset_name: test\nengine: pyspark\ntables: []\n')
        with patch('fairway.engines.slurm_cluster.SlurmSparkManager') as MockManager:
            # Mock instance
            mock_instance = MockManager.return_value
            mock_instance.start_cluster.return_value = "spark://mock-master:7077"
            
            result = runner.invoke(main, ['spark', 'start', '--slurm-nodes', '4'])
            assert result.exit_code == 0
            assert "Spark cluster started" in result.output
            assert "spark://mock-master:7077" in result.output
            
            # Verify config passed
            MockManager.assert_called_once()
            call_args = MockManager.call_args[0][0]
            assert call_args['slurm_nodes'] == 4
```

- [ ] **Step 2: Run the test**

Run: `source .venv/bin/activate && pytest tests/test_cli_spark_lifecycle.py::test_spark_start -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/test_cli_spark_lifecycle.py
git commit -m "fix: create config dir in spark start test so discover_config() succeeds"
```

---

### Task 6: Overhaul conftest.py fixtures

**Files:**
- Modify: `tests/conftest.py`

- [ ] **Step 1: Replace all engine fixtures with real constructors**

Rewrite `tests/conftest.py` to:

```python
"""Shared pytest configuration and fixtures for Fairway tests."""
import os
import pytest
from pathlib import Path


# ============ PySpark Skip Policy (RULE-113) ============
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Warn when PySpark tests were skipped (local dev without Spark/Java)."""
    skipped = terminalreporter.stats.get("skipped", [])
    spark_skips = [s for s in skipped if "pyspark" in str(getattr(s, "longrepr", "")).lower()
                   or "pyspark" in str(getattr(s, "keywords", "")).lower()]
    if spark_skips:
        terminalreporter.write_line(
            f"WARNING: {len(spark_skips)} PySpark test(s) skipped "
            f"— run `make test-docker` for full coverage",
            yellow=True,
        )


def pytest_collection_modifyitems(config, items):
    """In Docker (FAIRWAY_TEST_ENV=docker), convert PySpark skips to failures."""
    if os.environ.get("FAIRWAY_TEST_ENV") != "docker":
        return
    for item in items:
        for marker in item.iter_markers("skip"):
            reason = marker.kwargs.get("reason", "")
            if "pyspark" in reason.lower():
                item.add_marker(pytest.mark.xfail(
                    reason=f"FAIRWAY_TEST_ENV=docker: PySpark must be available ({reason})",
                    strict=True,
                ))


# ============ Path Fixtures ============
@pytest.fixture
def fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def temp_output(tmp_path):
    """Temporary output directory for test artifacts."""
    output = tmp_path / "output"
    output.mkdir()
    return output


# ============ Engine Fixtures ============
@pytest.fixture
def duckdb_engine():
    """DuckDB engine instance."""
    from fairway.engines.duckdb_engine import DuckDBEngine
    return DuckDBEngine()


@pytest.fixture(scope="session")
def _pyspark_engine_shared():
    """Session-scoped real PySpark engine — full __init__ with JVM flags, Delta, etc.

    Skips entire session's PySpark tests if PySpark is unavailable.
    """
    pytest.importorskip("pyspark")
    from fairway.engines.pyspark_engine import PySparkEngine
    engine = PySparkEngine()
    yield engine
    engine.stop()


@pytest.fixture
def pyspark_engine(_pyspark_engine_shared):
    """Per-test PySpark engine (shared session, real constructor)."""
    return _pyspark_engine_shared


@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    """Parametrized fixture — tests run against both engines.

    DuckDB runs always. PySpark skips if not available.
    """
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        return request.getfixturevalue("pyspark_engine")


# ============ CLI Fixtures ============
@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()
```

- [ ] **Step 2: Run the full local test suite to check for regressions**

Run: `source .venv/bin/activate && pytest tests/ -m "not spark and not hpc" -q`
Expected: All non-spark, non-hpc tests pass. No import errors.

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "fix: replace __new__ engine fixtures with real PySparkEngine() constructors

Removes spark_session, spark_session_2, and duckdb_only_engine.
Single session-scoped PySparkEngine() with real __init__.
Adds RULE-113 skip policy: warn locally, fail in Docker."
```

---

### Task 7: Set FAIRWAY_TEST_ENV in Docker

**Files:**
- Modify: `Dockerfile.dev:26` (or `entrypoint.sh`)

- [ ] **Step 1: Add env var to Dockerfile.dev**

In `Dockerfile.dev`, add after the `WORKDIR /app` line (line 26):

```dockerfile
ENV FAIRWAY_TEST_ENV=docker
```

- [ ] **Step 2: Commit**

```bash
git add Dockerfile.dev
git commit -m "build: set FAIRWAY_TEST_ENV=docker so PySpark skips become failures"
```

---

### Task 8: Update test_schema_evolution.py to use shared fixtures

**Files:**
- Modify: `tests/test_schema_evolution.py:1-19`

- [ ] **Step 1: Remove local engine fixture and imports**

Replace the top of `tests/test_schema_evolution.py` (lines 1-26) from:

```python

import pytest
import os
import shutil
from pyspark.sql import SparkSession
from fairway.engines.pyspark_engine import PySparkEngine

# Skip if PySpark is not available (though project requires it for these features)
try:
    import pyspark
except ImportError:
    pytest.skip("PySpark not installed", allow_module_level=True)

@pytest.fixture
def engine(spark_session):
    """Use the shared spark session from conftest."""
    eng = PySparkEngine.__new__(PySparkEngine)
    eng.spark = spark_session
    return eng

@pytest.fixture
def temp_dir(tmp_path):
    d = tmp_path / "fairway_test"
    d.mkdir()
    return str(d)
```

with:

```python
import pytest
import os

pyspark = pytest.importorskip("pyspark", reason="PySpark not available")


@pytest.fixture
def temp_dir(tmp_path):
    d = tmp_path / "fairway_test"
    d.mkdir()
    return str(d)
```

Then update the test functions to use `pyspark_engine` instead of `engine`:

- `test_strict_schema_validation_success(engine, temp_dir)` -> `test_strict_schema_validation_success(pyspark_engine, temp_dir)` and replace `engine.` with `pyspark_engine.` in the body
- `test_strict_schema_validation_extra_col_fail(engine, temp_dir)` -> same pattern
- `test_strict_schema_validation_fill_missing(engine, temp_dir)` -> same pattern

- [ ] **Step 2: Run the tests**

Run: `source .venv/bin/activate && pytest tests/test_schema_evolution.py -v`
Expected: PASS (or skip if PySpark unavailable locally)

- [ ] **Step 3: Commit**

```bash
git add tests/test_schema_evolution.py
git commit -m "fix: use shared pyspark_engine fixture in schema evolution tests"
```

---

### Task 9: Update test_performance.py to use shared fixtures

**Files:**
- Modify: `tests/test_performance.py:200-209`

- [ ] **Step 1: Remove the class-level engine fixture**

In `tests/test_performance.py`, delete the `engine` fixture from `TestPySparkPerformance` (lines 204-209):

```python
    @pytest.fixture
    def engine(self, spark_session):
        """Use the shared spark session from conftest."""
        from fairway.engines.pyspark_engine import PySparkEngine
        engine = PySparkEngine.__new__(PySparkEngine)
        engine.spark = spark_session
        return engine
```

Then update all test methods in the class to use `pyspark_engine` instead of `engine`:

- `test_salting_disabled_by_default_in_engine(self, engine, tmp_path)` -> `test_salting_disabled_by_default_in_engine(self, pyspark_engine, tmp_path)` and replace `engine.` with `pyspark_engine.` in the body
- `test_salting_enabled_when_balanced_true(self, engine, tmp_path)` -> same pattern
- `test_max_records_per_file_applied(self, engine, tmp_path)` -> same pattern
- `test_compression_option_applied(self, engine, tmp_path)` -> same pattern

- [ ] **Step 2: Run the tests**

Run: `source .venv/bin/activate && pytest tests/test_performance.py::TestPySparkPerformance -v`
Expected: PASS (or skip if PySpark unavailable locally)

- [ ] **Step 3: Commit**

```bash
git add tests/test_performance.py
git commit -m "fix: use shared pyspark_engine fixture in performance tests"
```

---

### Task 10: Update test_pyspark_salting.py to use shared fixtures

**Files:**
- Modify: `tests/test_pyspark_salting.py`

- [ ] **Step 1: Remove inline engine construction and spark_session_2 dependency**

Rewrite `tests/test_pyspark_salting.py` to:

```python
"""Tests for PySpark salting — balanced partition distribution for skewed data."""
import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not available")
from pyspark.sql import functions as F


class TestSaltingDistribution:
    """Salting must spread skewed data across multiple salt buckets."""

    def test_salting_distributes_skewed_data(self, pyspark_engine, tmp_path):
        """
        950 rows with category=A, 50 with category=B.
        With target_rows=100, num_salts = 1000 // 100 = 10.
        Each category=A bucket must have <1000 rows (not all skewed data in one partition).
        """
        data = [{"id": i, "category": "A"} for i in range(950)]
        data += [{"id": i + 1000, "category": "B"} for i in range(50)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=100,
        )

        result = pyspark_engine.spark.read.parquet(str(output))

        # 1. Row count preserved
        assert result.count() == 1000

        # 2. Salt partition directory exists — proves salting was applied
        assert "salt" in result.columns

        # 3. Multiple distinct salt values — proves distribution happened
        distinct_salts = result.select("salt").distinct().count()
        assert distinct_salts > 1, f"Expected >1 salt bucket, got {distinct_salts}"

        # 4. No single bucket holds all skewed data
        rows_per_salt = result.groupBy("salt").count().collect()
        max_in_bucket = max(r["count"] for r in rows_per_salt)
        assert max_in_bucket < 1000, (
            f"All rows landed in one salt bucket — skew not resolved. "
            f"Max bucket size: {max_in_bucket}"
        )

        # 5. Dominant category (A) is spread across >1 salt value
        category_a_salts = (
            result.filter(F.col("category") == "A")
            .select("salt").distinct().count()
        )
        assert category_a_salts > 1, (
            f"category=A still confined to 1 salt bucket after salting"
        )

    def test_salting_preserves_all_rows(self, pyspark_engine, tmp_path):
        """Salting must never drop data."""
        data = [{"id": i, "category": "A" if i % 10 != 0 else "B"} for i in range(500)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=50,
        )

        result = pyspark_engine.spark.read.parquet(str(output))
        assert result.count() == 500

    def test_small_dataset_uses_single_salt(self, pyspark_engine, tmp_path):
        """
        When total_rows < target_rows, num_salts=1 — no distribution needed.
        This is correct: small datasets shouldn't pay the salting overhead.
        """
        data = [{"id": i, "category": "A"} for i in range(100)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=500_000,  # num_salts = 100 // 500000 = 0 -> max(1, 0) = 1
        )

        result = pyspark_engine.spark.read.parquet(str(output))
        distinct_salts = result.select("salt").distinct().count()
        assert distinct_salts == 1, (
            f"Expected 1 salt for small dataset, got {distinct_salts}"
        )
```

- [ ] **Step 2: Run the tests**

Run: `source .venv/bin/activate && pytest tests/test_pyspark_salting.py -v`
Expected: PASS (or skip if PySpark unavailable locally)

- [ ] **Step 3: Commit**

```bash
git add tests/test_pyspark_salting.py
git commit -m "fix: use shared pyspark_engine fixture in salting tests

Removes spark_session_2 dependency and inline __new__ construction.
Salting distribution works at partition level, not executor level."
```

---

### Task 11: Docker verification — full test suite

**Files:** None (verification only)

- [ ] **Step 1: Rebuild the Docker image (picks up FAIRWAY_TEST_ENV)**

Run: `docker build -f Dockerfile.dev -t fairway-dev .`
Expected: Builds successfully

- [ ] **Step 2: Run the full Docker test suite**

Run: `make test-docker`
Expected: 0 failures, 0 errors, 0 PySpark skips. All 560+ tests pass.

- [ ] **Step 3: Check for remaining warnings**

Grep the output for `PytestUnknownMarkWarning` — should be none (marks registered in Task 0 / prior work).
Grep for `ValueError: I/O operation on closed file` — should be none (`__del__` removed in Task 1).

- [ ] **Step 4: Run local tests to verify skip warning appears**

Run: `source .venv/bin/activate && pytest tests/ -m "not hpc" -q`
Expected: Tests pass. Terminal summary shows `WARNING: N PySpark test(s) skipped — run make test-docker for full coverage` (if PySpark unavailable locally).

- [ ] **Step 5: Commit any remaining fixes if needed**

If any tests still fail, diagnose and fix before committing. This step is a catch-all.
