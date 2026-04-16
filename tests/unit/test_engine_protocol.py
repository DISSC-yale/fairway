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


def _assert_method_matches_protocol(engine_cls, method_name):
    """Every protocol parameter name (barring **kwargs) must appear in the engine method.

    Engines may accept *additional* parameters (e.g. PySparkEngine.ingest adds
    target_file_size_mb), but they may NOT drop or rename a declared protocol
    parameter — that would silently break callers that pass it positionally
    or by keyword.
    """
    from fairway.engines.base import EngineProtocol

    protocol_fn = getattr(EngineProtocol, method_name)
    engine_fn = getattr(engine_cls, method_name, None)
    assert callable(engine_fn), f"{engine_cls.__name__} missing {method_name}"

    protocol_params = set(_signature_param_names(protocol_fn))
    engine_params = set(_signature_param_names(engine_fn))
    missing = protocol_params - engine_params
    assert not missing, (
        f"{engine_cls.__name__}.{method_name} is missing protocol parameters: {missing}"
    )


def test_protocol_defines_required_methods():
    from fairway.engines.base import EngineProtocol
    for method in _protocol_methods():
        assert hasattr(EngineProtocol, method), f"EngineProtocol missing {method}"


def test_duckdb_engine_satisfies_protocol_runtime():
    pytest.importorskip("duckdb")
    from fairway.engines.base import EngineProtocol
    from fairway.engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()
    try:
        # runtime_checkable only verifies attribute presence; we still want
        # the isinstance bridge to confirm the engine duck-types as such.
        assert isinstance(engine, EngineProtocol)
    finally:
        engine.stop()


def test_duckdb_engine_signatures_match_protocol():
    pytest.importorskip("duckdb")
    from fairway.engines.duckdb_engine import DuckDBEngine
    for method in _protocol_methods():
        _assert_method_matches_protocol(DuckDBEngine, method)


def test_pyspark_engine_class_has_protocol_methods():
    """Class-level hasattr/callable — doesn't spin up a SparkSession."""
    pytest.importorskip("pyspark")
    from fairway.engines.pyspark_engine import PySparkEngine
    for method in _protocol_methods():
        assert callable(getattr(PySparkEngine, method, None)), (
            f"PySparkEngine missing {method}"
        )


def test_pyspark_engine_signatures_match_protocol():
    pytest.importorskip("pyspark")
    from fairway.engines.pyspark_engine import PySparkEngine
    for method in _protocol_methods():
        _assert_method_matches_protocol(PySparkEngine, method)


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
