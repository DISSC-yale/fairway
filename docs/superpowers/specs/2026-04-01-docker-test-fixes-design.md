# Docker Test Fixes Design
_Date: 2026-04-01 | Branch: simplify/codebase_

## Overview

40 tests fail in `make test-docker` across 5 root causes. The fixes align test fixtures with real pipeline usage, fix a PySpark 4.x type compatibility issue, and resolve a CLI template bug.

## Root Causes

| # | Root Cause | Tests | Type |
|---|---|---|---|
| 1 | **Dead SparkSession** — `spark_session_2` (salting) stops the session-scoped `spark_session`, poisoning ~25 subsequent tests | ~25 | Fixture bug |
| 2 | **`VARCHAR` rejected by PySpark 4.x** — requires `STRING` or `VARCHAR(n)` | 5 | Source bug |
| 3 | **CLI template `KeyError(' min')`** — `str.format()` interprets YAML `{ min: 0 }` as placeholders | 4 | Source bug |
| 4 | **`test_schema_evolution` + others** — `PySparkEngine.__new__()` bypass skips JVM flags | 3 | Fixture bug |
| 5 | **`__del__` logging noise** — `SparkSession.stop()` logs to closed stderr during teardown | all spark | Cosmetic |

## Changes

### 1. Fixture Overhaul (conftest.py)

**Problem:** `pyspark_engine` and the parametrized `engine` fixture use `PySparkEngine.__new__()` to skip `__init__()`, then hand-wire `engine.spark = spark_session`. This bypasses JVM flags, Delta config, spark_conf propagation, and local-mode binding. Tests pass against a fake engine that no user will ever have.

Additionally, `spark_session_2` (needed for salting's `local[2]`) stops the session-scoped `spark_session` via `SparkSession.getActiveSession().stop()`, leaving all subsequent tests with a dead session reference.

**Design:**

Remove `spark_session`, `spark_session_2`, and `duckdb_only_engine` fixtures. Replace with:

```python
@pytest.fixture(scope="session")
def _pyspark_engine_shared():
    pytest.importorskip("pyspark")
    from fairway.engines.pyspark_engine import PySparkEngine
    engine = PySparkEngine()  # real init — JVM flags, Delta, localhost binding
    yield engine
    engine.stop()

@pytest.fixture
def pyspark_engine(_pyspark_engine_shared):
    return _pyspark_engine_shared

@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        return request.getfixturevalue("pyspark_engine")
```

`PySparkEngine()` with no args = local mode. One Spark startup per test run (`scope="session"`), every test gets a real engine.

Salting tests currently use `spark_session_2` (`local[2]`) to test distribution. Salting works at the DataFrame partition level, not the executor level, so `local[1]` is sufficient. Remove `spark_session_2` entirely.

---

### 2. VARCHAR Normalization (pyspark_engine.py)

**Problem:** PySpark 4.x rejects bare `VARCHAR` — error: `[DATATYPE_MISSING_SIZE] DataType "VARCHAR" requires a length parameter`. Tests and real user configs can both hit this.

**Design:**

Add a `_normalize_spark_type()` helper in `pyspark_engine.py`:

- `VARCHAR` (no length) -> `STRING`
- `VARCHAR(n)` -> unchanged (Spark accepts this)
- All other types -> unchanged

Applied in `ingest()` where schema dict is first consumed, before any casting. This is a source fix that protects real users, not just a test patch.

---

### 3. CLI Template Fix (cli.py, fairway.yaml, README.md, getting-started.md)

**Problem:** `CONFIG_TEMPLATE.format(name=name, engine_type=engine_type)` fails because YAML comments contain `{ min: 0 }` which `str.format()` interprets as placeholders -> `KeyError(' min')`.

**Design:**

Switch from `str.format()` to `string.Template` with `safe_substitute()`:

- In template files (`fairway.yaml`, `README.md`, `getting-started.md`): change `{name}` -> `$name`, `{engine_type}` -> `$engine_type`, etc.
- In `cli.py`: replace `.format(...)` calls with `Template(...).safe_substitute(...)`.

`string.Template` uses `$` placeholders, so YAML braces are never interpreted. `safe_substitute` won't error on unrecognized `$` in templates. Users see clean, natural YAML when they copy the template.

---

### 4. Remove `__del__` (pyspark_engine.py)

**Problem:** `PySparkEngine.__del__()` calls `self.stop()` which logs via `logger.info()`. During pytest teardown, stderr is already closed, causing `ValueError: I/O operation on closed file` spam.

**Design:**

Remove `__del__` entirely. `stop()` is called explicitly by fixture teardown and pipeline cleanup. Python's `__del__` is unreliable for resource cleanup anyway — it runs at unpredictable times during interpreter shutdown.

---

### 5. Test File Updates

**Problem:** Three test files duplicate the `__new__` anti-pattern with local engine fixtures.

**Files and changes:**

- **test_schema_evolution.py**: Remove local `engine` fixture (lines 14-18), use conftest's `pyspark_engine`.
- **test_performance.py**: Remove class-level `engine` fixture (lines 204-209), use conftest's `pyspark_engine`.
- **test_pyspark_salting.py**: Remove inline `PySparkEngine.__new__()` in every test method, use conftest's `pyspark_engine`. Drop `spark_session_2` dependency.

Tests that use `engine.spark.createDataFrame()` directly (parquet_file_size, performance, salting, summarize_separation, validation) keep that pattern — it's appropriate for unit-testing specific engine behaviors. The change is they get a properly initialized engine.

Tests that already use `IngestionPipeline(config).run()` (ingestion_formats, schema_enforcement, write_behaviour, transformation, schema_merge) already test full pipeline. They should work once VARCHAR normalization is in place.

**Additional fix:** `test_cli_spark_lifecycle.py::test_spark_start` fails because the test runs in an empty `isolated_filesystem()` and the `spark start` command calls `discover_config()` which requires a `config/` directory. Fix: create a minimal config dir in the test setup before invoking the CLI.

---

### 6. Documentation

- **fairway.yaml** (template): Update schema comments to note `STRING` is preferred for Spark compatibility, `VARCHAR` is auto-converted. Change `VARCHAR` example to `STRING`.

---

### 7. PySpark Test Skip Policy

**Problem:** `pytest.importorskip("pyspark")` silently skips 30+ tests when PySpark isn't installed. Locally this gives false confidence — everything looks green while an entire engine is untested. In Docker, if PySpark somehow fails to install, tests skip silently instead of failing.

**Design:** Two complementary mechanisms:

**B. Warn on skip:** Add a `conftest.py` hook that counts skipped PySpark tests and emits a pytest terminal summary warning:
```
WARNING: 30 PySpark tests skipped — run `make test-docker` for full coverage
```
Uses `pytest_terminal_summary` hook. Only fires when skips > 0.

**C. Fail if in Docker:** Set `FAIRWAY_TEST_ENV=docker` in `Dockerfile.dev` (or `entrypoint.sh`). In `conftest.py`, add a fixture/hook that converts PySpark skips to failures when this env var is set. If PySpark is supposed to be available but isn't, that's a real error, not something to skip.

Implementation: a `conftest.py` autouse fixture or `pytest_collection_modifyitems` hook that checks the env var and replaces `importorskip` skips with hard failures.

---

## Rules Compliance

| Rule | Relevance | Status |
|---|---|---|
| **RULE-103** No collect/toPandas on full datasets | Tests use small synthetic data (100-1000 rows), not production paths | Compliant |
| **RULE-106** Single source of truth for templates | Templates stay in `src/fairway/data/`, loaded via `_read_data_file`. Only substitution method changes | Compliant |
| **RULE-112** Import isolation | `pytest.importorskip` preserved locally; Docker enforces availability via env var | Compliant |
| **RULE-113** Java environment | Handled by real `PySparkEngine.__init__()` instead of hand-wired fixtures | Improved |
| **RULE-114** Integration testing cleanliness | CLI init tests already use `runner.isolated_filesystem()` | Compliant |
| **RULE-115** Data integrity | VARCHAR -> STRING is lossless widening (no length limit) | Compliant |
| **RULE-117** Schema inference completeness | VARCHAR normalization applies only in casting/enforcement path, not schema inference | Must verify during implementation |
| **RULE-118** Performance defaults conservative | Salting behavior unchanged; only test executor count changes (local[2] -> local[1]) | Compliant |
| **RULE-123** Lazy engine init | Test fixtures use eager init (appropriate for tests); `IngestionPipeline` still lazy | Compliant |
