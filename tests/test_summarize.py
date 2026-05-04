"""Tests for fairway.summarize (DuckDB-only, rewrite/v0.3 Step 9.6)."""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from fairway.summarize import generate_summary


def _write_fixture(parquet: Path) -> None:
    """Mixed int/float/string parquet with one NULL in ``id``."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE t (id INTEGER, amount DOUBLE, name VARCHAR)"
        )
        con.execute(
            "INSERT INTO t VALUES (1, 10.0, 'a'), (2, 20.0, 'b'), "
            "(3, 30.0, 'c'), (NULL, 40.0, 'd')"
        )
        target = str(parquet).replace("'", "''")
        con.execute(f"COPY t TO '{target}' (FORMAT PARQUET)")
    finally:
        con.close()


def _read_summary(path: Path) -> dict[str, dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row["variable"]: row for row in csv.DictReader(f)}


@pytest.fixture()
def summary_csv(tmp_path: Path) -> dict[str, dict[str, str]]:
    parquet = tmp_path / "data.parquet"
    out = tmp_path / "summary.csv"
    _write_fixture(parquet)
    generate_summary(parquet, out)
    return _read_summary(out)


def test_roundtrip_columns_and_header(summary_csv: dict[str, dict[str, str]]) -> None:
    """Summary CSV has one row per source column with the documented schema."""
    assert set(summary_csv) == {"id", "amount", "name"}
    expected_fields = {"variable", "type", "null_count", "distinct_count",
                       "min", "max", "mean", "median"}
    for row in summary_csv.values():
        assert set(row) == expected_fields


def test_numeric_column_has_min_max_mean_median(
        summary_csv: dict[str, dict[str, str]]) -> None:
    amount = summary_csv["amount"]
    assert float(amount["min"]) == 10.0
    assert float(amount["max"]) == 40.0
    assert float(amount["mean"]) == 25.0
    assert float(amount["median"]) == 25.0


def test_string_column_min_max_mean_median_blank(
        summary_csv: dict[str, dict[str, str]]) -> None:
    name = summary_csv["name"]
    assert name["min"] == ""
    assert name["max"] == ""
    assert name["mean"] == ""
    assert name["median"] == ""


def test_null_and_distinct_counts(
        summary_csv: dict[str, dict[str, str]]) -> None:
    assert int(summary_csv["id"]["null_count"]) == 1
    assert int(summary_csv["id"]["distinct_count"]) == 3
    assert int(summary_csv["amount"]["null_count"]) == 0
    assert int(summary_csv["amount"]["distinct_count"]) == 4
    assert int(summary_csv["name"]["null_count"]) == 0
    assert int(summary_csv["name"]["distinct_count"]) == 4


def test_directory_input_globs_parquet(tmp_path: Path) -> None:
    """Directory input expands to ``<dir>/**/*.parquet``."""
    data_dir = tmp_path / "landed"
    data_dir.mkdir()
    _write_fixture(data_dir / "part-0.parquet")
    out = tmp_path / "summary.csv"
    generate_summary(data_dir, out)
    rows = _read_summary(out)
    assert set(rows) == {"id", "amount", "name"}
    assert int(rows["id"]["null_count"]) == 1
