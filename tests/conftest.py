"""Shared pytest configuration and fixtures for Fairway tests."""
import os
import pytest
from pathlib import Path

# Allow tests/scripts/* preprocessing scripts only during the test run.
# Production code keeps the stricter src/scripts/transformations allowlist.
os.environ.setdefault("FAIRWAY_ALLOW_TEST_SCRIPTS", "1")


# ============ Repo-root hygiene baseline + coverage path bootstrap ============
# Set at pytest_configure (runs before any test, fixture, or plugin data write)
# and compared at pytest_terminal_summary (fires even on abort/interrupt).
_repo_root_baseline: "set[str] | None" = None


def pytest_configure(config):
    """Bootstrap build dirs and snapshot repo root.

    Runs before any test or fixture, so:
      * the sanctioned build/ subdirs exist first-run,
      * the repo-root baseline is captured for the leak detector.

    Coverage data file path is configured via [tool.coverage.run].data_file
    in pyproject.toml (resolved relative to the config file).
    """
    global _repo_root_baseline
    repo_root = Path(__file__).parent.parent

    # Create sanctioned drop zones for coverage + pytest tmp.
    (repo_root / "build" / "coverage").mkdir(parents=True, exist_ok=True)
    (repo_root / "build" / "test-tmp").mkdir(parents=True, exist_ok=True)

    # Snapshot repo-root contents for the leak detector to diff against.
    try:
        _repo_root_baseline = set(os.listdir(repo_root))
    except OSError:
        _repo_root_baseline = None


# ============ Terminal summary: PySpark skips + leak detector ============
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Warn when PySpark tests were skipped; flag any repo-root leaks.

    Runs on terminal summary (including aborted/interrupted sessions), so
    leak reports survive Ctrl-C and plugin teardown order quirks. Uses
    terminalreporter.write_line which is not swallowed by output capture.
    """
    # --- PySpark skip policy (RULE-113) ---
    skipped = terminalreporter.stats.get("skipped", [])
    spark_skips = [s for s in skipped if "pyspark" in str(getattr(s, "longrepr", "")).lower()]
    if spark_skips:
        terminalreporter.write_line(
            f"WARNING: {len(spark_skips)} PySpark test(s) skipped "
            f"— run `make test-docker` for full coverage",
            yellow=True,
        )

    # --- Repo-root leak detector ---
    if _repo_root_baseline is None:
        return
    repo_root = Path(__file__).parent.parent
    ignored = {
        "build",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        ".reports",
        "htmlcov",
        "coverage.xml",
        "coverage.json",
    }
    try:
        after = set(os.listdir(repo_root))
    except OSError:
        return
    leaked = sorted((after - _repo_root_baseline) - ignored)
    if leaked:
        terminalreporter.write_line(
            f"TEST LEAK DETECTED — new repo-root entries after session: {leaked}",
            red=True,
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


# ============ FAIRWAY_HOME / FAIRWAY_SCRATCH isolation ============
@pytest.fixture(autouse=True)
def _fairway_home(request, tmp_path, monkeypatch):
    """Point FAIRWAY_HOME and FAIRWAY_SCRATCH at per-test tmp dirs.

    Autouse: every test gets an isolated state root so fairway can
    never scatter manifest / logs / cache into the repo under its CWD.

    Opt out with @pytest.mark.no_fairway_home — required for tests
    that exercise resolver behavior itself (env-unset paths,
    platformdirs fallback, etc.).
    """
    if request.node.get_closest_marker("no_fairway_home"):
        yield
        return
    state = tmp_path / "_state"
    scratch = tmp_path / "_scratch"
    state.mkdir()
    scratch.mkdir()
    monkeypatch.setenv("FAIRWAY_HOME", str(state))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(scratch))
    yield


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
    """DuckDB engine instance. Teardown releases the connection to avoid
    accumulating in-memory DuckDB handles across the test session."""
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    try:
        yield engine
    finally:
        engine.close()


@pytest.fixture(scope="session")
def _pyspark_engine_shared():
    """Session-scoped real PySpark engine — full __init__ with JVM flags, Delta, etc.

    Skips entire session's PySpark tests if PySpark is unavailable.
    """
    pytest.importorskip("pyspark")
    from fairway.engines.pyspark_engine import PySparkEngine
    engine = PySparkEngine()
    try:
        yield engine
    finally:
        engine.stop()


@pytest.fixture
def pyspark_engine(_pyspark_engine_shared):
    """Per-test PySpark engine (shared session, real constructor)."""
    return _pyspark_engine_shared


@pytest.fixture(params=["duckdb", "pyspark"])
def engine(request):
    """Parametrized fixture — tests run against both engines.

    DuckDB runs always. PySpark skips if not available. Teardown only
    fires for DuckDB; the PySpark engine is session-scoped and cleaned
    up once in `_pyspark_engine_shared`.
    """
    if request.param == "duckdb":
        from fairway.engines.duckdb_engine import DuckDBEngine
        e = DuckDBEngine()
        try:
            yield e
        finally:
            e.close()
    else:
        yield request.getfixturevalue("pyspark_engine")


# ============ CLI Fixtures ============
@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()
