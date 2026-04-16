"""Engine protocol conformance tests.

If these fail after adding a new engine or changing a method signature,
fix the engine — don't loosen the protocol, which exists exactly so
the pipeline doesn't need special-case branches per engine.
"""
import pytest


def test_protocol_defines_required_methods():
    from fairway.engines.base import EngineProtocol
    # Protocol members are recorded in __protocol_attrs__ (runtime_checkable
    # requirement). We only check the public surface we care about.
    for method in ("ingest", "enforce_types", "read_result", "infer_schema", "stop"):
        assert hasattr(EngineProtocol, method), f"EngineProtocol missing {method}"


def test_duckdb_engine_satisfies_protocol():
    duckdb = pytest.importorskip("duckdb")
    from fairway.engines.base import EngineProtocol
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    try:
        assert isinstance(engine, EngineProtocol)
    finally:
        engine.stop()


def test_pyspark_engine_class_satisfies_protocol():
    """Class-level check — doesn't spin up a SparkSession."""
    pyspark = pytest.importorskip("pyspark")
    from fairway.engines.base import EngineProtocol
    from fairway.engines.pyspark_engine import PySparkEngine
    # We check the class (not an instance) so we don't require Java.
    for method in ("ingest", "enforce_types", "read_result", "infer_schema", "stop"):
        assert callable(getattr(PySparkEngine, method, None)), (
            f"PySparkEngine missing {method}"
        )
    # Structural check: a subclass that inherits should also pass isinstance.
    # (runtime_checkable protocols test for attribute presence, not signatures.)
    # Constructing a PySparkEngine requires Java, so we only verify the class
    # has the right attribute surface for Protocol compliance.


def test_duckdb_engine_stop_is_idempotent():
    duckdb = pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    engine.stop()
    # A second call must not raise even though connection is already closed.
    engine.stop()
    assert engine.con is None
