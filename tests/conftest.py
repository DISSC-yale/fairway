"""Shared pytest configuration and fixtures for Fairway tests."""
import pytest
from pathlib import Path


# ============ Markers ============
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "local: runs without Spark (DuckDB only)")
    config.addinivalue_line("markers", "spark: requires PySpark + Java")
    config.addinivalue_line("markers", "hpc: requires SLURM cluster")


# ============ Auto-skip Spark if unavailable ============
def pytest_collection_modifyitems(config, items):
    """Auto-skip spark-marked tests if PySpark/Java not available."""
    try:
        from pyspark.sql import SparkSession
        spark_ok = True
    except ImportError:
        spark_ok = False

    if not spark_ok:
        skip_spark = pytest.mark.skip(reason="PySpark/Java not available")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip_spark)




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
def spark_session():
    """Shared Spark session for all tests. Properly configured for macOS/local dev."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
        .master("local[1]")
        .appName("fairway-tests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate())

    yield spark
    spark.stop()


@pytest.fixture
def pyspark_engine(spark_session):
    """PySpark engine using shared session. Skips if PySpark unavailable."""
    from fairway.engines.pyspark_engine import PySparkEngine
    engine = PySparkEngine.__new__(PySparkEngine)
    engine.spark = spark_session
    return engine


@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request, spark_session):
    """Parametrized fixture - tests run against both engines.

    Automatically skips PySpark tests if not available.
    Note: PySpark tests may fail with Java 17+ due to compatibility issues.
    """
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        pytest.importorskip("pyspark")
        from fairway.engines.pyspark_engine import PySparkEngine
        engine = PySparkEngine.__new__(PySparkEngine)
        engine.spark = spark_session
        return engine


@pytest.fixture
def duckdb_only_engine():
    """DuckDB engine only - for tests that shouldn't run on PySpark."""
    from fairway.engines.duckdb_engine import DuckDBEngine
    return DuckDBEngine()


# ============ CLI Fixtures ============
@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()
