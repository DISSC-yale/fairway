"""Shared pytest configuration and fixtures for Fairway tests."""
import pytest
import os
import subprocess
from pathlib import Path


# ============ Environment Setup ============
def _setup_test_environment():
    """Configure Java and PATH for tests.

    - Uses Java 11 or 17 if available (required for Spark compatibility)
    - Adds ~/.local/bin to PATH for Nextflow
    """
    # Add ~/.local/bin to PATH for Nextflow
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{local_bin}:{os.environ.get('PATH', '')}"

    # Find compatible Java version (11 or 17) for Spark
    # Java 18+ removed Subject.getSubject() which Hadoop/Spark requires
    if "JAVA_HOME" not in os.environ or "25" in os.environ.get("JAVA_HOME", ""):
        try:
            # Try to find Java 17 first, then 11
            for version in ["17", "11"]:
                result = subprocess.run(
                    ["/usr/libexec/java_home", "-v", version],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    java_home = result.stdout.strip()
                    os.environ["JAVA_HOME"] = java_home
                    # Also update PATH to use this Java
                    os.environ["PATH"] = f"{java_home}/bin:{os.environ['PATH']}"
                    break
        except FileNotFoundError:
            # java_home command not available (Linux), try common paths
            for java_path in [
                "/usr/lib/jvm/java-17-openjdk",
                "/usr/lib/jvm/java-11-openjdk",
                "/opt/homebrew/Cellar/openjdk@17/17.0.17/libexec/openjdk.jdk/Contents/Home",
            ]:
                if os.path.exists(java_path):
                    os.environ["JAVA_HOME"] = java_path
                    os.environ["PATH"] = f"{java_path}/bin:{os.environ['PATH']}"
                    break


# Run environment setup at module load time
_setup_test_environment()


# ============ Markers ============
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "local: runs without Spark (DuckDB only)")
    config.addinivalue_line("markers", "spark: requires PySpark + Java")
    config.addinivalue_line("markers", "hpc: requires SLURM cluster")


# ============ Check Spark availability (no auto-skip) ============
def pytest_collection_modifyitems(config, items):
    """Check if PySpark is available - tests will fail if misconfigured, not skip."""
    # PySpark tests should fail if there are issues, not skip
    # Only Redivis tests are allowed to skip
    pass




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


@pytest.fixture
def pyspark_engine():
    """PySpark engine in local mode."""
    from fairway.engines.pyspark_engine import PySparkEngine
    return PySparkEngine(spark_master="local[*]")


@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    """Parametrized fixture - tests run against both engines."""
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        return DuckDBEngine()
    else:
        from fairway.engines.pyspark_engine import PySparkEngine
        return PySparkEngine(spark_master="local[*]")


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
