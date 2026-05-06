"""Schema-as-required artifact for fairway v0.3.

Loads ``tables/<t>/schema.yaml``, cross-validates against ``config.yaml``,
computes a stable per-table fingerprint, and applies CASTs at ingest
time with three configurable ``on_drift`` axes (extra/missing/cast).
Subsumes the legacy ``fixed_width.py`` column-spec model.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml


logger = logging.getLogger(__name__)


_VALID_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class SchemaError(Exception):
    """Raised on missing schema, invalid columns, or drift in strict mode."""


@dataclass(frozen=True)
class OnDrift:
    extra: Literal["error", "include"] = "error"
    missing: Literal["null_fill"] = "null_fill"
    cast: Literal["error", "null"] = "error"

    def is_strict_default(self) -> bool:
        return self.extra == "error" and self.cast == "error"

    @classmethod
    def from_yaml(cls, value: Any) -> "OnDrift":
        if value is None or value == "strict":
            return cls()
        if value == "lenient":
            return cls(extra="include", missing="null_fill", cast="null")
        if not isinstance(value, dict):
            raise SchemaError(
                f"on_drift must be 'strict', 'lenient', or a per-axis mapping; got {value!r}"
            )
        extra = value.get("extra", "error")
        cast = value.get("cast", "error")
        missing = value.get("missing", "null_fill")
        if extra not in ("error", "include"):
            raise SchemaError(f"on_drift.extra must be 'error' or 'include', got {extra!r}")
        if cast not in ("error", "null"):
            raise SchemaError(f"on_drift.cast must be 'error' or 'null', got {cast!r}")
        if missing != "null_fill":
            raise SchemaError(
                f"on_drift.missing must be 'null_fill' (only supported value), got {missing!r}"
            )
        return cls(extra=extra, missing="null_fill", cast=cast)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    physical: dict[str, Any] | None = None


@dataclass(frozen=True)
class SchemaSpec:
    columns: list[ColumnSpec]
    on_drift: OnDrift = field(default_factory=OnDrift)
    record_type_filter: dict[str, Any] | None = None


def schema_path(table_dir: Path) -> Path:
    return table_dir / "schema.yaml"


def load_schema(table_dir: Path) -> SchemaSpec:
    """Load + parse ``tables/<t>/schema.yaml``. Empty columns is an error."""
    path = schema_path(table_dir)
    if not path.is_file():
        raise SchemaError(
            f"Missing {path}. Run `fairway discover {table_dir.name}` to generate it."
        )
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise SchemaError(f"{path}: top-level YAML must be a mapping")
    cols_raw = raw.get("columns") or []
    if not cols_raw:
        raise SchemaError(
            f"{path} has no columns — run `fairway discover {table_dir.name}` "
            f"or add columns manually."
        )
    columns: list[ColumnSpec] = []
    for i, c in enumerate(cols_raw):
        if not isinstance(c, dict):
            raise SchemaError(f"{path}: columns[{i}] must be a mapping")
        name = c.get("name")
        ctype = c.get("type")
        if not isinstance(name, str) or not _VALID_IDENT.match(name):
            raise SchemaError(f"{path}: columns[{i}].name {name!r} is not a valid identifier")
        if not isinstance(ctype, str) or not ctype.strip():
            raise SchemaError(f"{path}: columns[{i}].type is required")
        physical = c.get("physical")
        if physical is not None and not isinstance(physical, dict):
            raise SchemaError(f"{path}: columns[{i}].physical must be a mapping")
        columns.append(ColumnSpec(name=name, type=ctype.strip(), physical=physical))
    on_drift = OnDrift.from_yaml(raw.get("on_drift"))
    rtf = raw.get("record_type_filter")
    if rtf is not None and not isinstance(rtf, dict):
        raise SchemaError(f"{path}: record_type_filter must be a mapping")
    return SchemaSpec(columns=columns, on_drift=on_drift, record_type_filter=rtf)


def write_schema(
    table_dir: Path,
    columns: list[ColumnSpec],
    on_drift: str = "strict",
) -> Path:
    """Render a schema.yaml file. Used by ``fairway discover``."""
    path = schema_path(table_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = ["on_drift: " + on_drift, "columns:"]
    for c in columns:
        body.append(f"  - {{name: {c.name}, type: {c.type}}}")
    path.write_text("\n".join(body) + "\n", encoding="utf-8")
    return path


def validate_schema_vs_config(schema: SchemaSpec, source_format: str) -> None:
    """Cross-check ``physical`` blocks vs ``source_format``."""
    if source_format == "fixed_width":
        for c in schema.columns:
            if not c.physical:
                raise SchemaError(
                    f"column {c.name!r}: source_format=fixed_width requires a `physical:` block"
                )
            if "start" not in c.physical or "length" not in c.physical:
                raise SchemaError(
                    f"column {c.name!r}: physical block needs `start` and `length`"
                )
    elif source_format == "delimited":
        for c in schema.columns:
            if c.physical:
                raise SchemaError(
                    f"column {c.name!r}: source_format=delimited forbids a `physical:` block"
                )
        if schema.record_type_filter is not None:
            raise SchemaError(
                "record_type_filter is only allowed for source_format=fixed_width"
            )
    else:
        raise SchemaError(f"unknown source_format: {source_format!r}")


def schema_fingerprint(schema: SchemaSpec) -> str:
    """16-char hex SHA256. Excludes ``on_drift`` so toggling it doesn't reprocess."""
    payload = {
        "columns": sorted(
            ({"name": c.name, "type": c.type, "physical": c.physical}
             for c in schema.columns),
            key=lambda c: c["name"],
        ),
        "record_type_filter": schema.record_type_filter,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


@dataclass(frozen=True)
class DriftEvents:
    unexpected_columns: list[dict[str, Any]] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    cast_failures: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "unexpected_columns": list(self.unexpected_columns),
            "missing_columns": list(self.missing_columns),
            "cast_failures": list(self.cast_failures),
        }

    def is_empty(self) -> bool:
        return (not self.unexpected_columns and not self.missing_columns
                and not self.cast_failures)


def apply_schema(
    con: Any,
    rel: Any,
    schema: SchemaSpec,
) -> tuple[Any, DriftEvents]:
    """Apply schema CASTs, recording drift events according to ``on_drift``.

    Returns ``(relation, drift_events)``. ``rel`` carries source-typed
    columns (typically VARCHAR via ``all_varchar=true``); the returned
    relation carries declared types. Cast failures are detected by
    comparing ``CAST`` vs ``TRY_CAST`` row counts when ``cast == "null"``;
    in strict mode (``cast == "error"``), an uncastable value raises.
    """
    on = schema.on_drift
    source_cols = list(rel.columns)
    declared = {c.name: c for c in schema.columns}
    extras = [c for c in source_cols if c not in declared]
    missing = [c.name for c in schema.columns if c.name not in source_cols]
    drift = DriftEvents(
        unexpected_columns=[],
        missing_columns=list(missing),
        cast_failures=[],
    )
    if extras:
        if on.extra == "error":
            raise SchemaError(
                f"unexpected source columns not in schema.yaml: {extras!r}"
            )
        for name in extras:
            drift.unexpected_columns.append({"name": name})

    cast_op = "TRY_CAST" if on.cast == "null" else "CAST"
    select_parts: list[str] = []
    for c in schema.columns:
        ident = '"' + c.name.replace('"', '""') + '"'
        if c.name in source_cols:
            select_parts.append(f"{cast_op}({ident} AS {c.type}) AS {ident}")
        else:
            select_parts.append(f"CAST(NULL AS {c.type}) AS {ident}")
    if on.extra == "include":
        for name in extras:
            ident = '"' + name.replace('"', '""') + '"'
            select_parts.append(f"{ident}")

    if on.cast == "null":
        for c in schema.columns:
            if c.name not in source_cols:
                continue
            ident = '"' + c.name.replace('"', '""') + '"'
            try:
                fail_count = int(rel.aggregate(
                    f"SUM(CASE WHEN {ident} IS NOT NULL "
                    f"AND TRY_CAST({ident} AS {c.type}) IS NULL THEN 1 ELSE 0 END)"
                ).fetchone()[0] or 0)
            except Exception:
                fail_count = 0
            if fail_count > 0:
                drift.cast_failures.append({"column": c.name, "count": fail_count})

    out = rel.project(", ".join(select_parts))
    return out, drift
