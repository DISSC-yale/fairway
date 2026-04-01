"""Shared pytest configuration and fixtures for Fairway tests."""
import pytest
from pathlib import Path


# ============ Markers ============
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "hpc: requires SLURM cluster")
    config.addinivalue_line("markers", "slow: marks tests as slow-running")




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


@pytest.fixture(scope="session")
def spark_session_2():
    """Two-executor Spark session — required for salting distribution tests.

    PySpark allows only one SparkContext per JVM. This fixture stops any
    existing session and creates a fresh local[2] session for distribution tests.
    Tests using this fixture must NOT run concurrently with tests using spark_session.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from pyspark import SparkContext

    # Stop any active SparkContext before creating a new one with different config
    existing = SparkContext._active_spark_context
    if existing is not None:
        SparkSession.getActiveSession().stop()

    spark = (SparkSession.builder
        .master("local[2]")
        .appName("fairway-tests-salting")
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
def engine(request):
    """Parametrized fixture - tests run against both engines.

    DuckDB runs always. PySpark skips if not available.
    spark_session is lazy-loaded only when param == 'pyspark'.
    """
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        pytest.importorskip("pyspark")
        spark = request.getfixturevalue("spark_session")
        from fairway.engines.pyspark_engine import PySparkEngine
        eng = PySparkEngine.__new__(PySparkEngine)
        eng.spark = spark
        return eng


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
