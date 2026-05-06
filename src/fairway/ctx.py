"""Shard-execution context handed to user transforms."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import TableConfig


@dataclass(frozen=True)
class IngestCtx:
    config: TableConfig
    input_paths: list[Path]
    output_root: Path                       # storage_processed/<table>
    partition_values: dict[str, str]        # {key: value} for the SHARD level
    leaves: dict[str, list[Path]]           # {leaf_path: [files...]} for this shard
    shard_id: str
    scratch_dir: Path
    input_view: str = "fairway_input"       # name of the DuckDB view bound to inputs
    extra: dict[str, Any] = field(default_factory=dict)
