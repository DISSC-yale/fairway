"""Runner-side validation checks dispatched from each shard.

Each check runs as a single DuckDB aggregate against the supplied
relation; the first failure raises :class:`ShardValidationError`,
which the runner catches and persists into the shard's manifest
fragment.
"""
from __future__ import annotations

from typing import Any

from .config import RangeSpec, Validations


class ShardValidationError(RuntimeError):
    """Raised on the first failing validation check for a shard."""

    def __init__(self, check_name: str, **detail: Any) -> None:
        parts = ", ".join(f"{k}={v!r}" for k, v in detail.items())
        super().__init__(f"validation '{check_name}' failed: {parts}" if parts
                         else f"validation '{check_name}' failed")
        self.check_name = check_name
        self.detail = detail


def apply_validations(rel: Any, v: Validations) -> None:
    """Run every configured check against ``rel``; raise on first failure."""
    n_rows = int(rel.count("*").fetchone()[0])
    if n_rows < v.min_rows:
        raise ShardValidationError("min_rows", got=n_rows, want_ge=v.min_rows)
    if v.max_rows is not None and n_rows > v.max_rows:
        raise ShardValidationError("max_rows", got=n_rows, want_le=v.max_rows)
    if v.expected_columns is not None:
        _check_expected_columns(rel, v.expected_columns.columns, v.expected_columns.strict)
    for col in v.check_nulls:
        _check_nulls(rel, col)
    for col, spec in v.check_range.items():
        _check_range(rel, col, spec)
    for col, allowed in v.check_values.items():
        _check_values(rel, col, allowed)
    for col, pattern in v.check_pattern.items():
        _check_pattern(rel, col, pattern)


def _check_expected_columns(rel: Any, required: list[str], strict: bool) -> None:
    actual = set(rel.columns)
    missing = [c for c in required if c not in actual]
    if missing:
        raise ShardValidationError("expected_columns", missing=missing, actual=sorted(actual))
    if strict:
        extra = sorted(actual - set(required))
        if extra:
            raise ShardValidationError("expected_columns", extra=extra, required=required)


def _check_nulls(rel: Any, col: str) -> None:
    n = int(rel.aggregate(f'COUNT(*) FILTER (WHERE "{col}" IS NULL)').fetchone()[0])
    if n > 0:
        raise ShardValidationError("check_nulls", column=col, null_count=n)


def _check_range(rel: Any, col: str, spec: RangeSpec) -> None:
    row = rel.aggregate(f'MIN("{col}"), MAX("{col}")').fetchone()
    lo, hi = row[0], row[1]
    if spec.min is not None and lo is not None and lo < spec.min:
        raise ShardValidationError("check_range", column=col, min=lo, want_ge=spec.min)
    if spec.max is not None and hi is not None and hi > spec.max:
        raise ShardValidationError("check_range", column=col, max=hi, want_le=spec.max)


def _check_values(rel: Any, col: str, allowed: list[Any]) -> None:
    if not allowed:
        return
    in_clause = ", ".join(_sql_literal(v) for v in allowed)
    bad = rel.filter(f'"{col}" IS NOT NULL AND "{col}" NOT IN ({in_clause})') \
             .limit(1).fetchall()
    if bad:
        raise ShardValidationError("check_values", column=col, sample=bad[0][0],
                                   allowed=allowed)


def _check_pattern(rel: Any, col: str, pattern: str) -> None:
    pat = pattern.replace("'", "''")
    bad = rel.filter(
        f'"{col}" IS NOT NULL AND NOT regexp_full_match("{col}", \'{pat}\')'
    ).limit(1).fetchall()
    if bad:
        raise ShardValidationError("check_pattern", column=col, sample=bad[0][0],
                                   pattern=pattern)


def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"
