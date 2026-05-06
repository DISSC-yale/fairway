"""Tests for schema.yaml loading, fingerprint, on_drift, apply_schema."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import duckdb
import pytest

from fairway.schema import (
    ColumnSpec,
    OnDrift,
    SchemaError,
    SchemaSpec,
    apply_schema,
    load_schema,
    schema_fingerprint,
    validate_schema_vs_config,
)


def _write_schema(table_dir: Path, body: str) -> None:
    table_dir.mkdir(parents=True, exist_ok=True)
    (table_dir / "schema.yaml").write_text(body, encoding="utf-8")


def test_fingerprint_stable_column_order():
    a = SchemaSpec(columns=[
        ColumnSpec(name="x", type="INTEGER"),
        ColumnSpec(name="y", type="DOUBLE"),
    ])
    b = SchemaSpec(columns=[
        ColumnSpec(name="y", type="DOUBLE"),
        ColumnSpec(name="x", type="INTEGER"),
    ])
    assert schema_fingerprint(a) == schema_fingerprint(b)


def test_fingerprint_excludes_on_drift():
    cols = [ColumnSpec(name="x", type="INTEGER")]
    strict = SchemaSpec(columns=cols, on_drift=OnDrift())
    lenient = SchemaSpec(columns=cols, on_drift=OnDrift.from_yaml("lenient"))
    assert schema_fingerprint(strict) == schema_fingerprint(lenient)


def test_fingerprint_changes_on_type():
    a = SchemaSpec(columns=[ColumnSpec(name="x", type="INTEGER")])
    b = SchemaSpec(columns=[ColumnSpec(name="x", type="VARCHAR")])
    assert schema_fingerprint(a) != schema_fingerprint(b)


def test_validate_fixed_width_missing_physical():
    schema = SchemaSpec(columns=[ColumnSpec(name="x", type="INTEGER", physical=None)])
    with pytest.raises(SchemaError, match="requires a `physical:` block"):
        validate_schema_vs_config(schema, "fixed_width")


def test_validate_delimited_extra_physical():
    schema = SchemaSpec(columns=[
        ColumnSpec(name="x", type="INTEGER", physical={"start": 0, "length": 3})
    ])
    with pytest.raises(SchemaError, match="forbids a `physical:` block"):
        validate_schema_vs_config(schema, "delimited")


def test_validate_record_type_filter_on_delimited():
    schema = SchemaSpec(
        columns=[ColumnSpec(name="x", type="INTEGER")],
        record_type_filter={"position": 0, "length": 1, "value": "H"},
    )
    with pytest.raises(SchemaError, match="record_type_filter"):
        validate_schema_vs_config(schema, "delimited")


def test_on_drift_from_yaml_strict():
    od = OnDrift.from_yaml("strict")
    assert od.extra == "error" and od.cast == "error" and od.missing == "null_fill"


def test_on_drift_from_yaml_lenient():
    od = OnDrift.from_yaml("lenient")
    assert od.extra == "include" and od.cast == "null" and od.missing == "null_fill"


def test_on_drift_from_yaml_partial_dict():
    od = OnDrift.from_yaml({"extra": "include"})
    assert od.extra == "include"
    assert od.cast == "error"
    assert od.missing == "null_fill"


def test_on_drift_from_yaml_invalid_axis():
    with pytest.raises(SchemaError):
        OnDrift.from_yaml({"extra": "bogus"})


def test_load_schema_empty_columns_raises(tmp_path):
    table = tmp_path / "tables" / "x"
    _write_schema(table, "on_drift: strict\ncolumns: []\n")
    with pytest.raises(SchemaError, match="no columns"):
        load_schema(table)


def test_load_schema_missing_file_raises(tmp_path):
    with pytest.raises(SchemaError, match="Missing"):
        load_schema(tmp_path / "tables" / "x")


def test_load_schema_invalid_identifier_raises(tmp_path):
    table = tmp_path / "tables" / "x"
    _write_schema(table, "columns:\n  - {name: '1bad', type: INTEGER}\n")
    with pytest.raises(SchemaError):
        load_schema(table)


def _rel(con, sql: str):
    return con.sql(sql)


def test_apply_schema_extra_strict():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT 1::INTEGER AS x, 2::INTEGER AS extra_col")
    schema = SchemaSpec(columns=[ColumnSpec(name="x", type="INTEGER")])
    with pytest.raises(SchemaError, match="extra_col"):
        apply_schema(con, rel, schema)


def test_apply_schema_extra_include():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT '1' AS x, 'hi' AS extra_col")
    schema = SchemaSpec(
        columns=[ColumnSpec(name="x", type="INTEGER")],
        on_drift=OnDrift(extra="include", missing="null_fill", cast="error"),
    )
    out, drift = apply_schema(con, rel, schema)
    assert "x" in out.columns and "extra_col" in out.columns
    assert any(e["name"] == "extra_col" for e in drift.unexpected_columns)


def test_apply_schema_missing_null_fill():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT '1' AS x")
    schema = SchemaSpec(columns=[
        ColumnSpec(name="x", type="INTEGER"),
        ColumnSpec(name="y", type="INTEGER"),
    ])
    out, drift = apply_schema(con, rel, schema)
    assert out.columns == ["x", "y"]
    assert "y" in drift.missing_columns
    row = out.fetchone()
    assert row[0] == 1 and row[1] is None


def test_apply_schema_cast_error_on_uncastable():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT 'N/A' AS x")
    schema = SchemaSpec(columns=[ColumnSpec(name="x", type="INTEGER")])
    out, _ = apply_schema(con, rel, schema)
    with pytest.raises(Exception):
        out.fetchall()


def test_apply_schema_cast_null_on_uncastable():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT 'N/A' AS x UNION ALL SELECT '5'")
    schema = SchemaSpec(
        columns=[ColumnSpec(name="x", type="INTEGER")],
        on_drift=OnDrift(extra="error", missing="null_fill", cast="null"),
    )
    out, drift = apply_schema(con, rel, schema)
    rows = sorted([r[0] for r in out.fetchall()], key=lambda v: (v is None, v))
    assert rows == [5, None]
    assert any(f["column"] == "x" and f["count"] == 1 for f in drift.cast_failures)


def test_apply_schema_strict_default_no_drift_events():
    con = duckdb.connect(":memory:")
    rel = _rel(con, "SELECT '1' AS x, '2.5' AS y")
    schema = SchemaSpec(columns=[
        ColumnSpec(name="x", type="INTEGER"),
        ColumnSpec(name="y", type="DOUBLE"),
    ])
    _, drift = apply_schema(con, rel, schema)
    assert drift.is_empty()
