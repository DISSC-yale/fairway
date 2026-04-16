"""Shared pytest configuration and fixtures for Fairway tests."""
import os
import pytest
from pathlib import Path


# ============ PySpark Skip Policy (RULE-113) ============
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Warn when PySpark tests were skipped (local dev without Spark/Java)."""
    skipped = terminalreporter.stats.get("skipped", [])
    spark_skips = [s for s in skipped if "pyspark" in str(getattr(s, "longrepr", "")).lower()]
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
