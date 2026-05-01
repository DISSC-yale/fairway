"""Tests for the engines registry (post-constants.py).

Owns the canonical engine set and alias lookup. config_loader, pipeline,
and CLI all call `normalize_engine_name` on user input before membership-
checking against `VALID_ENGINES`.
"""


def test_valid_engines_is_frozen():
    from fairway.engines import VALID_ENGINES
    assert isinstance(VALID_ENGINES, frozenset)
    assert "duckdb" in VALID_ENGINES
    assert "pyspark" in VALID_ENGINES


def test_normalize_engine_name_handles_aliases():
    from fairway.engines import normalize_engine_name
    assert normalize_engine_name("spark") == "pyspark"
    assert normalize_engine_name("SPARK") == "pyspark"
    assert normalize_engine_name("pyspark") == "pyspark"
    assert normalize_engine_name("duckdb") == "duckdb"
    assert normalize_engine_name(None) is None
    assert normalize_engine_name("") == ""
    # Unknown strings pass through so caller can validate with VALID_ENGINES.
    assert normalize_engine_name("unknown_engine") == "unknown_engine"


def test_normalize_engine_name_handles_whitespace():
    from fairway.engines import normalize_engine_name
    assert normalize_engine_name("  spark  ") == "pyspark"
    assert normalize_engine_name("\tduckdb\n") == "duckdb"
    assert normalize_engine_name("   ") == ""


def test_normalize_engine_name_rejects_non_string():
    from fairway.engines import normalize_engine_name
    assert normalize_engine_name(123) == 123
    assert normalize_engine_name(["spark"]) == ["spark"]


def test_normalize_engine_name_is_idempotent():
    from fairway.engines import normalize_engine_name
    once = normalize_engine_name("spark")
    twice = normalize_engine_name(once)
    assert once == twice == "pyspark"


# test_container_sif_identifiers_are_stable removed in v0.3 Step 2 —
# the container module and its SIF identifiers were deleted.
