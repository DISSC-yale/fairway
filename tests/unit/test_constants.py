"""Tests for the constants module."""


def test_constants_module_has_required_keys():
    from fairway import constants
    assert hasattr(constants, 'DEFAULT_CONFIG_PATH')
    assert hasattr(constants, 'DEFAULT_SPARK_CONFIG_PATH')
    assert hasattr(constants, 'DEFAULT_MANIFEST_DIR')
    assert hasattr(constants, 'DEFAULT_CACHE_DIR')
    assert hasattr(constants, 'DEFAULT_LOG_DIR')
    assert hasattr(constants, 'FAIRWAY_SIF_ENV_VAR')
    assert hasattr(constants, 'VALID_ENGINES')


def test_valid_engines_is_frozen():
    from fairway.constants import VALID_ENGINES
    assert isinstance(VALID_ENGINES, frozenset)
    assert "duckdb" in VALID_ENGINES
    assert "pyspark" in VALID_ENGINES


def test_normalize_engine_name_handles_aliases():
    from fairway.constants import normalize_engine_name
    # 'spark' is a user-friendly alias for 'pyspark'
    assert normalize_engine_name("spark") == "pyspark"
    assert normalize_engine_name("SPARK") == "pyspark"
    # Canonical names pass through
    assert normalize_engine_name("pyspark") == "pyspark"
    assert normalize_engine_name("duckdb") == "duckdb"
    # Empty/None pass through unchanged
    assert normalize_engine_name(None) is None
    assert normalize_engine_name("") == ""
    # Unknown strings pass through so caller can validate with VALID_ENGINES
    assert normalize_engine_name("unknown_engine") == "unknown_engine"


def test_container_python_version_is_defined():
    """Dev-mode bind path depends on this constant matching the container base image."""
    from fairway import constants
    assert constants.CONTAINER_PYTHON_VERSION.startswith("python3.")
