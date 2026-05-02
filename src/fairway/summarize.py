"""DuckDB-backed dataset summary (rewrite/v0.3 Step 9.6, ≤60 LOC).

One row per column: distinct/null count plus MIN/MAX/AVG/MEDIAN for numerics.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import duckdb

_NUMERIC_PREFIXES = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                     "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
                     "FLOAT", "DOUBLE", "DECIMAL")


def _is_numeric(dtype: str) -> bool:
    return any(dtype.upper().startswith(p) for p in _NUMERIC_PREFIXES)


def _source_sql(path: Path) -> str:
    glob = f"{path}/**/*.parquet" if path.is_dir() else str(path)
    return glob.replace("'", "''")


def generate_summary(parquet_path: Path | str, output_path: Path | str) -> None:
    """Write per-column summary CSV from landed parquet via DuckDB."""
    src = _source_sql(Path(parquet_path))
    con = duckdb.connect(":memory:")
    rows: list[dict[str, Any]] = []
    try:
        cols = [(r[0], r[1]) for r in con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{src}')").fetchall()]
        for name, dtype in cols:
            ident = '"' + name.replace('"', '""') + '"'
            base = f"COUNT(DISTINCT {ident}), COUNT(*) - COUNT({ident})"
            if _is_numeric(dtype):
                q = (f"SELECT {base}, MIN({ident}), MAX({ident}), "
                     f"AVG({ident}), MEDIAN({ident}) "
                     f"FROM read_parquet('{src}')")
                row = con.sql(q).fetchone() or (0, 0, None, None, None, None)
                distinct, nulls, mn, mx, mean, median = row
            else:
                q = f"SELECT {base} FROM read_parquet('{src}')"
                row = con.sql(q).fetchone() or (0, 0)
                distinct, nulls = row
                mn = mx = mean = median = None
            rows.append({"variable": name, "type": dtype,
                         "null_count": nulls, "distinct_count": distinct,
                         "min": mn, "max": mx, "mean": mean, "median": median})
    finally:
        con.close()
    fields = ["variable", "type", "null_count", "distinct_count",
              "min", "max", "mean", "median"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
