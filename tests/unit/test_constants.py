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
