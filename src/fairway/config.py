"""Frozen Config dataclass + YAML loader for fairway v0.3.

Replaces the 832-LOC ``config_loader.py`` with a minimal, strongly typed
surface. Legacy v0.2 fields removed in v0.3 raise :class:`ConfigError`
rather than being silently accepted. ``IngestCtx``/``TransformCtx`` are
the frozen contexts handed to user transforms in Stage 1 / Stage 2.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml


class ConfigError(Exception):
    """Raised when a YAML config is invalid or uses removed v0.2 fields."""


# Back-compat alias for legacy test imports.
ConfigValidationError = ConfigError


@dataclass(frozen=True)
class RangeSpec:
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True)
class ExpectedColumns:
    columns: list[str]
    strict: bool = False


@dataclass(frozen=True)
class Validations:
    min_rows: int = 1
    max_rows: int | None = None
    check_nulls: list[str] = field(default_factory=list)
    expected_columns: ExpectedColumns | None = None
    check_range: dict[str, RangeSpec] = field(default_factory=dict)
    check_values: dict[str, list[Any]] = field(default_factory=dict)
    check_pattern: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Config:
    # Required
    dataset_name: str
    python: Path
    storage_root: Path
    source_glob: str
    naming_pattern: str
    partition_by: list[str]
    # Path overrides
    storage_raw: Path | None = None
    storage_processed: Path | None = None
    storage_curated: Path | None = None
    layer: Literal["raw", "processed", "curated"] = "raw"
    # Source & partitioning
    source_format: Literal["delimited", "fixed_width"] = "delimited"
    sub_partition_by: list[str] = field(default_factory=list)
    # Reading
    delimiter: str = "\t"
    has_header: bool = True
    encoding: str = "utf-8"
    encoding_fallback: str = "latin-1"
    allow_encoding_fallback: bool = False
    date_columns: dict[str, str] = field(default_factory=dict)
    # Type A
    apply_type_a: bool = True
    # Zip
    unzip: bool = False
    zip_inner_pattern: str = "*.csv|*.tsv|*.txt"
    zip_password_file: Path | None = None
    # Validations
    validations: Validations = field(default_factory=Validations)
    # Performance / output
    sort_by: list[str] | None = None
    row_group_size: int = 122880
    # Slurm
    slurm_account: str | None = None
    slurm_partition: str | None = None
    slurm_chunk_size: int = 4000
    slurm_concurrency: int = 64
    slurm_mem: str = "64G"
    slurm_cpus_per_task: int = 8
    slurm_time: str = "2:00:00"
    # Runtime
    scratch_dir: Path | None = None


# Legacy v0.2 top-level fields, with the explicit error message that fires
# when they are encountered. Silent drops are forbidden — typos and stale
# configs must surface loudly.
_REMOVED_FIELDS: dict[str, str] = {
    "engine": "Field `engine` is no longer supported (DuckDB only since v0.3)",
    "enrichment": "Field `enrichment` removed; use fairway enrich subcommand on landed parquet",
    "enrichments": "Field `enrichment` removed; use fairway enrich subcommand on landed parquet",
    "performance": "Field `performance` removed; use sort_by/row_group_size",
    "container": "Field `container` removed; no Apptainer in v0.3",
    "transformations": "Field `transformations` removed; transforms now live in <dataset>.py",
    "schema": "Field `schema` removed; lossless landing via all_varchar=true",
}

_REQUIRED_FIELDS: tuple[str, ...] = (
    "dataset_name", "python", "storage_root",
    "source_glob", "naming_pattern", "partition_by",
)


def _path(value: Any) -> Path | None:
    return None if value is None else Path(value)


def _parse_validations(raw: Any) -> Validations:
    if raw is None:
        return Validations()
    if not isinstance(raw, dict):
        raise ConfigError(f"`validations` must be a mapping, got {type(raw).__name__}")
    expected: ExpectedColumns | None = None
    expected_raw = raw.get("expected_columns")
    if expected_raw is not None:
        if not isinstance(expected_raw, dict) or "columns" not in expected_raw:
            raise ConfigError("`validations.expected_columns` must be a mapping with `columns`")
        expected = ExpectedColumns(
            columns=list(expected_raw["columns"]),
            strict=bool(expected_raw.get("strict", False)),
        )
    check_range = {
        col: RangeSpec(min=spec.get("min"), max=spec.get("max"))
        for col, spec in (raw.get("check_range") or {}).items()
    }
    return Validations(
        min_rows=int(raw.get("min_rows", 1)),
        max_rows=raw.get("max_rows"),
        check_nulls=list(raw.get("check_nulls", [])),
        expected_columns=expected,
        check_range=check_range,
        check_values=dict(raw.get("check_values", {})),
        check_pattern=dict(raw.get("check_pattern", {})),
    )


def load_config(path: Path | str) -> Config:
    """Parse a YAML config file into a frozen :class:`Config`."""
    with Path(path).open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Top-level YAML must be a mapping, got {type(raw).__name__}")
    for legacy in _REMOVED_FIELDS:
        if legacy in raw:
            raise ConfigError(_REMOVED_FIELDS[legacy])
    missing = [f for f in _REQUIRED_FIELDS if raw.get(f) in (None, "")]
    if missing:
        raise ConfigError(f"Missing required field(s): {', '.join(missing)}")
    partition_by = list(raw["partition_by"])
    naming_pattern = str(raw["naming_pattern"])
    groups = set(re.findall(r"\?P<([^>]+)>", naming_pattern))
    if set(partition_by) != groups:
        raise ConfigError(
            f"naming_pattern named groups {sorted(groups)} must equal "
            f"partition_by {sorted(partition_by)}"
        )
    return Config(
        dataset_name=str(raw["dataset_name"]),
        python=Path(raw["python"]),
        storage_root=Path(raw["storage_root"]),
        source_glob=str(raw["source_glob"]),
        naming_pattern=naming_pattern,
        partition_by=partition_by,
        storage_raw=_path(raw.get("storage_raw")),
        storage_processed=_path(raw.get("storage_processed")),
        storage_curated=_path(raw.get("storage_curated")),
        layer=raw.get("layer", "raw"),
        source_format=raw.get("source_format", "delimited"),
        sub_partition_by=list(raw.get("sub_partition_by", [])),
        delimiter=str(raw.get("delimiter", "\t")),
        has_header=bool(raw.get("has_header", True)),
        encoding=str(raw.get("encoding", "utf-8")),
        encoding_fallback=str(raw.get("encoding_fallback", "latin-1")),
        allow_encoding_fallback=bool(raw.get("allow_encoding_fallback", False)),
        date_columns=dict(raw.get("date_columns", {})),
        apply_type_a=bool(raw.get("apply_type_a", True)),
        unzip=bool(raw.get("unzip", False)),
        zip_inner_pattern=str(raw.get("zip_inner_pattern", "*.csv|*.tsv|*.txt")),
        zip_password_file=_path(raw.get("zip_password_file")),
        validations=_parse_validations(raw.get("validations")),
        sort_by=list(raw["sort_by"]) if raw.get("sort_by") else None,
        row_group_size=int(raw.get("row_group_size", 122880)),
        slurm_account=raw.get("slurm_account"),
        slurm_partition=raw.get("slurm_partition"),
        slurm_chunk_size=int(raw.get("slurm_chunk_size", 4000)),
        slurm_concurrency=int(raw.get("slurm_concurrency", 64)),
        slurm_mem=str(raw.get("slurm_mem", "64G")),
        slurm_cpus_per_task=int(raw.get("slurm_cpus_per_task", 8)),
        slurm_time=str(raw.get("slurm_time", "2:00:00")),
        scratch_dir=_path(raw.get("scratch_dir")),
    )


# Legacy import shims so pre-rewrite tests collect (they raise on use).
class TableConfig:  # pragma: no cover - legacy import shim
    def __init__(self, *_a: Any, **_kw: Any) -> None:
        raise ConfigError("TableConfig was removed in v0.3")

    @classmethod
    def from_dict(cls, _d: dict) -> "TableConfig":
        raise ConfigError("TableConfig was removed in v0.3")


def validate_config_paths(_config: Any) -> None:  # pragma: no cover - legacy shim
    raise ConfigError("validate_config_paths was removed in v0.3")
