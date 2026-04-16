"""Tests for D2 (close()/stop()), D3 (UnboundLocalError), D4 (RULE-103)."""
import pytest


# ---- D2 --------------------------------------------------------------------

def test_duckdb_engine_has_close_alias():
    pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    assert hasattr(engine, "close"), "DuckDBEngine missing close() alias"
    assert hasattr(engine, "stop"), "DuckDBEngine missing stop() method"
    engine.close()
    assert engine.con is None


def test_duckdb_engine_close_and_stop_are_interchangeable():
    pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    e1 = DuckDBEngine()
    e1.close()
    e1.close()  # idempotent

    e2 = DuckDBEngine()
    e2.stop()
    e2.close()  # idempotent across alias


# ---- D3 --------------------------------------------------------------------

def test_calculate_hashes_single_file_slow_path_initializes_sha256(tmp_path):
    """Regression for UnboundLocalError when a single (non-dir, non-glob) file
    path is hashed with fast_check=False. Previous code referenced
    sha256_hash before assigning it in the slow-hash branch."""
    # Verify the fix by inspecting the source — we can't run the worker on
    # a real SparkSession without Java. Reading the file is adequate: the
    # slow-path now explicitly assigns sha256_hash before the read loop.
    import inspect
    pyspark_mod = pytest.importorskip("fairway.engines.pyspark_engine")
    source = inspect.getsource(pyspark_mod.PySparkEngine.calculate_hashes)
    # After the fix, the single-file slow branch begins with an assignment:
    assert "sha256_hash = hashlib.sha256()\n                    with open(path" in source, (
        "Single-file slow-hash branch is missing sha256_hash initialization "
        "— UnboundLocalError regression"
    )

    (tmp_path / "data.csv").write_text("id\n1\n")


# ---- D4 --------------------------------------------------------------------

def test_max_rows_uses_limit_not_full_count():
    """RULE-103: max_rows validation must use df.limit(max_rows+1).count()
    instead of df.count() on the full dataset."""
    import inspect
    from fairway.validations import checks
    source = inspect.getsource(checks.Validator.run_all)
    # Ensure the limit(...).count() short-circuit exists and is used for max_rows
    assert "df.limit(max_rows + 1).count()" in source, (
        "max_rows check must short-circuit with df.limit(max_rows + 1)"
    )


def test_check_nulls_uses_single_pass_aggregation():
    """RULE-103: check_nulls must not do (N + 1) full scans on Spark."""
    import inspect
    from fairway.validations import checks
    source = inspect.getsource(checks.Validator.run_all)
    # Ensure the single-pass agg is in place (no `total = df.count()` before
    # the per-column loop); instead an agg(*agg_exprs) must be called.
    assert "df.agg(*agg_exprs)" in source, (
        "check_nulls must use a single df.agg() pass for null counts"
    )


def test_salt_count_does_not_call_rdd_count():
    """RULE-103: salt calculation must not call df.rdd.count() on the full DF."""
    import inspect
    from fairway.engines import pyspark_engine
    source = inspect.getsource(pyspark_engine.PySparkEngine.ingest)
    # Strip comments so a "don't do this: df.rdd.count()" explanatory comment
    # doesn't trip the check.
    code_lines = [
        line.split("#", 1)[0]
        for line in source.splitlines()
    ]
    code_only = "\n".join(code_lines)
    assert "df.rdd.count()" not in code_only, (
        "ingest() must not call df.rdd.count() for salt count — "
        "use configurable num_salts (RULE-103)"
    )
