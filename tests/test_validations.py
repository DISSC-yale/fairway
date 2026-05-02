"""Tests for :mod:`fairway.validations`."""
from __future__ import annotations

import duckdb
import pytest

from fairway.config import ExpectedColumns, RangeSpec, Validations
from fairway.validations import ShardValidationError, apply_validations


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def _rel(con: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyRelation:
    return con.sql(sql)


def test_min_rows_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3")
    apply_validations(rel, Validations(min_rows=2))


def test_min_rows_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS x")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(min_rows=5))
    assert ei.value.check_name == "min_rows"


def test_max_rows_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS x UNION ALL SELECT 2")
    apply_validations(rel, Validations(min_rows=1, max_rows=10))


def test_max_rows_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(min_rows=1, max_rows=2))
    assert ei.value.check_name == "max_rows"


def test_check_nulls_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 'a' AS s UNION ALL SELECT 'b'")
    apply_validations(rel, Validations(check_nulls=["s"]))


def test_check_nulls_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 'a' AS s UNION ALL SELECT NULL")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(check_nulls=["s"]))
    assert ei.value.check_name == "check_nulls"
    assert ei.value.detail["column"] == "s"


def test_expected_columns_lenient_subset_ok(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS a, 2 AS b, 3 AS c")
    apply_validations(rel, Validations(
        expected_columns=ExpectedColumns(columns=["a", "b"], strict=False),
    ))


def test_expected_columns_strict_extras_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS a, 2 AS b, 3 AS c")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(
            expected_columns=ExpectedColumns(columns=["a", "b"], strict=True),
        ))
    assert ei.value.check_name == "expected_columns"
    assert ei.value.detail.get("extra") == ["c"]


def test_expected_columns_missing_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 1 AS a")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(
            expected_columns=ExpectedColumns(columns=["a", "b"], strict=False),
        ))
    assert "b" in ei.value.detail["missing"]


def test_check_range_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 5 AS n UNION ALL SELECT 7 UNION ALL SELECT 9")
    apply_validations(rel, Validations(check_range={"n": RangeSpec(min=0, max=10)}))


def test_check_range_min_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT -5 AS n UNION ALL SELECT 7")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(check_range={"n": RangeSpec(min=0, max=10)}))
    assert ei.value.check_name == "check_range"
    assert ei.value.detail["column"] == "n"


def test_check_range_max_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 5 AS n UNION ALL SELECT 99")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(check_range={"n": RangeSpec(min=0, max=10)}))
    assert ei.value.check_name == "check_range"


def test_check_values_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 'A' AS code UNION ALL SELECT 'B'")
    apply_validations(rel, Validations(check_values={"code": ["A", "B", "C"]}))


def test_check_values_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT 'A' AS code UNION ALL SELECT 'X'")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(check_values={"code": ["A", "B"]}))
    assert ei.value.check_name == "check_values"
    assert ei.value.detail["column"] == "code"


def test_check_pattern_pass(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT '12345' AS zip UNION ALL SELECT '67890'")
    apply_validations(rel, Validations(check_pattern={"zip": r"[0-9]{5}"}))


def test_check_pattern_fail(con: duckdb.DuckDBPyConnection) -> None:
    rel = _rel(con, "SELECT '12345' AS zip UNION ALL SELECT 'abc'")
    with pytest.raises(ShardValidationError) as ei:
        apply_validations(rel, Validations(check_pattern={"zip": r"[0-9]{5}"}))
    assert ei.value.check_name == "check_pattern"
    assert ei.value.detail["column"] == "zip"


def test_shard_validation_error_attrs() -> None:
    err = ShardValidationError("foo", got=3, want_ge=10)
    assert err.check_name == "foo"
    assert err.detail == {"got": 3, "want_ge": 10}
    assert "foo" in str(err)
    assert "got=3" in str(err)
