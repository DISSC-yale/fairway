"""Engine protocol conformance tests.

If these fail after adding a new engine or changing a method signature,
fix the engine — don't loosen the protocol, which exists exactly so
the pipeline doesn't need special-case branches per engine.

@runtime_checkable Protocol only checks attribute *presence* — not
signatures. We compensate by comparing parameter names via `inspect`.
"""
import inspect

import pytest


def _protocol_methods():
    return ("ingest", "enforce_types", "read_result", "infer_schema", "stop")


def _signature_param_names(fn):
    """Return the parameter names of fn, dropping 'self' and any **kwargs sink."""
    sig = inspect.signature(fn)
    names = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            continue
        names.append(name)
    return names


def test_protocol_defines_required_methods():
    pytest.skip("EngineProtocol removed in v0.3 Step 1 (engines/base.py deleted)")


def test_duckdb_engine_satisfies_protocol_runtime():
    pytest.skip("EngineProtocol removed in v0.3 Step 1 (engines/base.py deleted)")


def test_duckdb_engine_signatures_match_protocol():
    pytest.skip("EngineProtocol removed in v0.3 Step 1 (engines/base.py deleted)")


def test_pyspark_engine_class_has_protocol_methods():
    pytest.skip("PySpark engine + EngineProtocol removed in v0.3 Step 1")


def test_pyspark_engine_signatures_match_protocol():
    pytest.skip("PySpark engine + EngineProtocol removed in v0.3 Step 1")


def test_duckdb_engine_stop_is_idempotent():
    pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    engine.stop()
    # A second call must not raise even though connection is already closed.
    engine.stop()
    assert engine.con is None


def test_duckdb_engine_after_stop_raises_clear_error():
    """Calling ingest/read_result/infer_schema/enforce_types after stop()
    must raise RuntimeError instead of a cryptic NoneType AttributeError."""
    pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    engine.stop()

    with pytest.raises(RuntimeError, match="stopped"):
        engine.read_result("/tmp/nowhere.parquet")
    with pytest.raises(RuntimeError, match="stopped"):
        engine.infer_schema("/tmp/nowhere")
    with pytest.raises(RuntimeError, match="stopped"):
        engine.ingest("/tmp/nowhere", "/tmp/out.parquet")
    with pytest.raises(RuntimeError, match="stopped"):
        engine.enforce_types("/tmp/nowhere", "/tmp/out.parquet", columns=[])
