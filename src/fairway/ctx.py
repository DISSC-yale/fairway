"""Shard-execution contexts handed to user transforms.

Stage 1 (ingest) uses :class:`IngestCtx`; Stage 2 (transform) uses
:class:`TransformCtx`. Both are frozen dataclasses with the same shape
modulo the config field — the type difference distinguishes the two
stages at the user-callable boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import Config


@dataclass(frozen=True)
class TransformConfig:
    """Stage 2 config stub — fully built in later steps."""

    dataset_name: str = ""
    python: Path | None = None


@dataclass(frozen=True)
class IngestCtx:
    config: Config
    input_paths: list[Path]
    output_path: Path
    partition_values: dict[str, str]
    shard_id: str
    scratch_dir: Path


@dataclass(frozen=True)
class TransformCtx:
    transform_config: TransformConfig
    input_paths: list[Path]
    output_path: Path
    partition_values: dict[str, str]
    shard_id: str
    scratch_dir: Path
