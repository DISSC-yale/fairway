"""Two-phase schema discovery for delimited tables.

Phase 1: read headers from every file → union of column names.
Phase 2: pick a coverage-guaranteed sample (≥1 file per column, up to
``--sample-files``); infer types via ``UNION ALL BY NAME``; resolve
per-column conflicts to the wider type.

Refuses fixed-width tables (no headers; manual authoring required).
"""
from __future__ import annotations

import glob as _glob
import logging
from pathlib import Path

from .config import TableConfig
from .schema import ColumnSpec, schema_path, write_schema


logger = logging.getLogger(__name__)


class DiscoverError(Exception):
    """Raised when discovery cannot run or refuses to overwrite."""


_TYPE_PRECEDENCE = (
    "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP", "VARCHAR",
)


def _wider_type(a: str, b: str) -> str:
    """Pick the more-permissive of two declared DuckDB types."""
    a_norm, b_norm = a.upper().split("(")[0], b.upper().split("(")[0]
    if a_norm == b_norm:
        return a
    try:
        ai = _TYPE_PRECEDENCE.index(a_norm)
    except ValueError:
        ai = len(_TYPE_PRECEDENCE) - 1
    try:
        bi = _TYPE_PRECEDENCE.index(b_norm)
    except ValueError:
        bi = len(_TYPE_PRECEDENCE) - 1
    return a if ai > bi else b


def _format_path_sql(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _read_headers(con, path: Path, has_header: bool, delimiter: str) -> list[str]:
    if not has_header:
        return []
    desc = con.sql(
        f"DESCRIBE SELECT * FROM read_csv_auto({_format_path_sql(path)}, "
        f"delim={_format_path_sql(delimiter)}, header=true, "
        f"sample_size=1, all_varchar=true)"
    ).fetchall()
    return [row[0] for row in desc]


def _pick_coverage_sample(
    files: list[Path],
    headers_by_file: dict[Path, list[str]],
    max_files: int,
) -> list[Path]:
    """Pick up to ``max_files`` so every column appears in ≥1 chosen file."""
    if len(files) <= max_files:
        return files
    chosen: list[Path] = []
    covered: set[str] = set()
    all_cols = {c for cols in headers_by_file.values() for c in cols}
    for f in files:
        if not (set(headers_by_file[f]) - covered):
            continue
        chosen.append(f)
        covered.update(headers_by_file[f])
        if covered == all_cols or len(chosen) >= max_files:
            break
    if len(chosen) < max_files:
        for f in files:
            if f in chosen:
                continue
            chosen.append(f)
            if len(chosen) >= max_files:
                break
    return chosen


def discover(
    config: TableConfig,
    *,
    sample_files: int = 50,
    rows_per_file: int = 1000,
    force: bool = False,
) -> Path:
    """Two-phase discovery → ``tables/<t>/schema.yaml``.

    Refuses ``source_format=fixed_width``. Refuses to overwrite a
    populated schema.yaml unless ``force=True``.
    """
    if config.source_format == "fixed_width":
        raise DiscoverError(
            "Fixed-width schemas must be authored manually — see "
            "docs/fixed_width.md. Discovery requires column headers, "
            "which fixed-width files don't have."
        )
    target = schema_path(config.table_dir)
    if target.is_file() and not force:
        try:
            import yaml as _yaml
            existing = _yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}
        if isinstance(existing, dict) and existing.get("columns"):
            raise DiscoverError(
                f"{target} already has columns; pass --force to overwrite. "
                f"Existing columns: {[c.get('name') for c in existing['columns']]}"
            )

    files = [Path(p) for p in sorted(_glob.glob(config.source_glob))]
    if not files:
        raise DiscoverError(
            f"No files matched source_glob={config.source_glob!r}"
        )

    import duckdb
    con = duckdb.connect(":memory:")
    try:
        headers_by_file: dict[Path, list[str]] = {}
        for f in files:
            try:
                headers_by_file[f] = _read_headers(con, f, config.has_header, config.delimiter)
            except Exception as exc:
                logger.warning("could not read headers from %s: %s", f, exc)
                headers_by_file[f] = []
        union_cols: list[str] = []
        seen: set[str] = set()
        for cols in headers_by_file.values():
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    union_cols.append(c)
        if not union_cols:
            raise DiscoverError("no columns found across sampled files")

        sample = _pick_coverage_sample(files, headers_by_file, sample_files)
        types: dict[str, str] = {}
        for f in sample:
            desc = con.sql(
                f"DESCRIBE SELECT * FROM read_csv_auto("
                f"{_format_path_sql(f)}, delim={_format_path_sql(config.delimiter)}, "
                f"header={'true' if config.has_header else 'false'}, "
                f"sample_size={int(rows_per_file)})"
            ).fetchall()
            for col, dtype, *_ in desc:
                cur = types.get(col)
                types[col] = dtype if cur is None else _wider_type(cur, dtype)
        for c in union_cols:
            types.setdefault(c, "VARCHAR")
    finally:
        con.close()

    columns = [ColumnSpec(name=c, type=types[c]) for c in union_cols]
    return write_schema(config.table_dir, columns)
