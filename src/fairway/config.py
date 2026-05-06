"""Project + per-table config loader for fairway v0.3.

Walks up from CWD for ``fairway.yaml``; merges hardcoded defaults <
``fairway.yaml`` < ``tables/<t>/config.yaml`` < CLI overrides into a
single :class:`TableConfig`. Removed legacy v0.2 fields raise
:class:`ConfigError` rather than being silently accepted.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import click
import yaml


logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised on missing files, missing required fields, or invalid values."""


PROJECT_FILE = "fairway.yaml"


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
class TableConfig:
    # identity
    table: str
    project_root: Path
    table_dir: Path
    # source
    source_glob: str
    naming_pattern: str
    partition_by: list[str]
    shard_by: list[str]
    source_format: Literal["delimited", "fixed_width"] = "delimited"
    delimiter: str = ","
    has_header: bool = True
    date_columns: dict[str, str] = field(default_factory=dict)
    apply_type_a: bool = False
    validations: Validations = field(default_factory=Validations)
    # storage (resolved absolute)
    storage_processed: Path = Path("data/processed")
    storage_curated: Path = Path("data/curated")
    scratch_dir: Path | None = None
    row_group_size: int = 122880
    # encoding
    encoding: str = "utf-8"
    encoding_fallback: str = "latin-1"
    allow_encoding_fallback: bool = False
    # zip / sort
    unzip: bool = False
    zip_inner_pattern: str = "*.csv|*.tsv|*.txt"
    zip_password_file: Path | None = None
    sort_by: list[str] | None = None
    # Slurm
    slurm_account: str | None = None
    slurm_partition: str | None = None
    slurm_chunk_size: int = 4000
    slurm_concurrency: int = 64
    slurm_mem: str = "64G"
    slurm_cpus_per_task: int = 8
    slurm_time: str = "2:00:00"


_PROJECT_KEYS: tuple[str, ...] = (
    "storage_root", "storage_processed", "storage_curated", "scratch_dir",
    "encoding", "encoding_fallback", "allow_encoding_fallback",
    "row_group_size",
    "slurm_account", "slurm_partition", "slurm_chunk_size",
    "slurm_concurrency", "slurm_mem", "slurm_cpus_per_task", "slurm_time",
)


_REMOVED_FIELDS: dict[str, str] = {
    "engine": "Field `engine` is no longer supported (DuckDB only since v0.3)",
    "enrichment": "Field `enrichment` removed; transforms live in tables/<t>/transform.py",
    "enrichments": "Field `enrichment` removed; transforms live in tables/<t>/transform.py",
    "performance": "Field `performance` removed; use sort_by/row_group_size",
    "container": "Field `container` removed; no Apptainer in v0.3",
    "transformations": "Field `transformations` removed; transforms live in tables/<t>/transform.py",
    "schema": "Field `schema` belongs in tables/<t>/schema.yaml, not config.yaml",
    "dataset_name": "Field `dataset_name` removed; folder name is the table name",
    "python": "Field `python` removed; transform.py is auto-loaded next to config.yaml",
    "layer": "Field `layer` removed; v0.3 writes to `processed` only",
    "storage_raw": "Field `storage_raw` removed; raw is no longer a managed layer",
    "sub_partition_by": "Field `sub_partition_by` removed; use shard_by + partition_by",
}


_TABLE_REQUIRED: tuple[str, ...] = (
    "source_glob", "naming_pattern", "partition_by",
)


def find_project_root(start: Path | None = None) -> Path:
    """Walk up from ``start`` (default CWD) until ``fairway.yaml`` is found.

    Raises :class:`ConfigError` if not found before filesystem root.
    """
    here = Path(start or Path.cwd()).resolve()
    candidates = [here, *here.parents]
    for cand in candidates:
        logger.debug("checking %s for %s", cand, PROJECT_FILE)
        if (cand / PROJECT_FILE).is_file():
            return cand
    raise ConfigError(
        f"No `{PROJECT_FILE}` found in . or ancestors. "
        f"Run `fairway init <dir>` to create a project."
    )


def load_project_config(project_root: Path) -> dict[str, Any]:
    """Load and return ``fairway.yaml`` as a raw dict."""
    path = project_root / PROJECT_FILE
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top-level YAML must be a mapping")
    if "apply_type_a" in raw:
        raise ConfigError(
            f"{path}: `apply_type_a` is per-table only — move it to tables/<t>/config.yaml"
        )
    if "storage_raw" in raw:
        raise ConfigError(f"{path}: `storage_raw` is removed in v0.3")
    return raw


def load_table_config(project_root: Path, table: str) -> dict[str, Any]:
    """Load ``tables/<table>/config.yaml`` as a raw dict."""
    table_dir = project_root / "tables" / table
    path = table_dir / "config.yaml"
    if not path.is_file():
        raise ConfigError(
            f"Table {table!r} not found: missing {path}. "
            f"Run `fairway init . --table {table}` to scaffold it."
        )
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top-level YAML must be a mapping")
    return raw


def _path_or_none(value: Any) -> Path | None:
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


def _resolve_storage(
    project_root: Path, project: dict, table: dict, key: str, default: str,
) -> Path:
    """Per-table override beats project default beats hardcoded default."""
    raw = table.get(key, project.get(key, default))
    p = Path(raw)
    return p if p.is_absolute() else (project_root / p).resolve()


def resolve_config(
    project_root: Path | None = None,
    table: str | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> TableConfig:
    """Merge defaults < ``fairway.yaml`` < ``tables/<t>/config.yaml`` < CLI.

    Always echoes the resolved ``project_root`` so accidental walk-up
    matches surface immediately.
    """
    if table is None:
        raise ConfigError("table name is required")
    root = (project_root or find_project_root()).resolve()
    click.echo(f"Project root: {root}")
    project = load_project_config(root)
    table_raw = load_table_config(root, table)
    overrides = cli_overrides or {}

    for legacy, msg in _REMOVED_FIELDS.items():
        if legacy in table_raw:
            raise ConfigError(f"tables/{table}/config.yaml: {msg}")

    missing = [f for f in _TABLE_REQUIRED if table_raw.get(f) in (None, "")]
    if missing:
        raise ConfigError(
            f"tables/{table}/config.yaml: missing required field(s): "
            f"{', '.join(missing)}"
        )

    partition_by = list(table_raw["partition_by"])
    naming_pattern = str(table_raw["naming_pattern"])
    groups = set(re.findall(r"\?P<([^>]+)>", naming_pattern))
    if set(partition_by) != groups:
        raise ConfigError(
            f"naming_pattern named groups {sorted(groups)} must equal "
            f"partition_by {sorted(partition_by)}"
        )

    raw_shard_by = table_raw.get("shard_by", partition_by)
    if raw_shard_by is None:
        raw_shard_by = []
    shard_by = list(raw_shard_by)

    storage_processed = _resolve_storage(root, project, table_raw,
                                         "storage_processed", "data/processed")
    storage_curated = _resolve_storage(root, project, table_raw,
                                       "storage_curated", "data/curated")

    def pick(key: str, default: Any) -> Any:
        if key in overrides:
            return overrides[key]
        if key in table_raw:
            return table_raw[key]
        if key in project:
            return project[key]
        return default

    table_dir = root / "tables" / table
    raw_glob = str(table_raw["source_glob"])
    glob_path = Path(raw_glob)
    source_glob = raw_glob if glob_path.is_absolute() else str(root / glob_path)
    return TableConfig(
        table=table,
        project_root=root,
        table_dir=table_dir,
        source_glob=source_glob,
        naming_pattern=naming_pattern,
        partition_by=partition_by,
        shard_by=shard_by,
        source_format=pick("source_format", "delimited"),
        delimiter=str(pick("delimiter", ",")),
        has_header=bool(pick("has_header", True)),
        date_columns=dict(pick("date_columns", {})),
        apply_type_a=bool(table_raw.get("apply_type_a", False)),
        validations=_parse_validations(table_raw.get("validations")),
        storage_processed=storage_processed,
        storage_curated=storage_curated,
        scratch_dir=_path_or_none(pick("scratch_dir", None)),
        row_group_size=int(pick("row_group_size", 122880)),
        encoding=str(pick("encoding", "utf-8")),
        encoding_fallback=str(pick("encoding_fallback", "latin-1")),
        allow_encoding_fallback=bool(pick("allow_encoding_fallback", False)),
        unzip=bool(pick("unzip", False)),
        zip_inner_pattern=str(pick("zip_inner_pattern", "*.csv|*.tsv|*.txt")),
        zip_password_file=_path_or_none(pick("zip_password_file", None)),
        sort_by=list(table_raw["sort_by"]) if table_raw.get("sort_by") else None,
        slurm_account=pick("slurm_account", None),
        slurm_partition=pick("slurm_partition", None),
        slurm_chunk_size=int(pick("slurm_chunk_size", 4000)),
        slurm_concurrency=int(pick("slurm_concurrency", 64)),
        slurm_mem=str(pick("slurm_mem", "64G")),
        slurm_cpus_per_task=int(pick("slurm_cpus_per_task", 8)),
        slurm_time=str(pick("slurm_time", "2:00:00")),
    )
