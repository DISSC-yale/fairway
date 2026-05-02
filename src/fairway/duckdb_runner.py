"""DuckDB shard runner — skeleton for Step 7.1.

Defines the public dataclasses returned by :func:`run_shard` and the
function signature itself. The full ingest pipeline (PRAGMA setup,
unzip, transform load, type A, partition cols, validations, parquet
write, fragment write) is wired in Step 7.3.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-only
    from .ctx import IngestCtx


@dataclass(frozen=True)
class ColumnSpec:
    """One column of a parquet output schema."""

    name: str
    dtype: str


@dataclass(frozen=True)
class ShardResult:
    """Result of running a single shard through the runner."""

    row_count: int
    schema_fingerprint: str
    schema: list[ColumnSpec]
    output_paths: list[Path]
    duration_seconds: float
    encoding_used: str


def run_shard(con: Any, ctx: "IngestCtx") -> ShardResult:
    """Execute a single shard end-to-end and return a :class:`ShardResult`.

    The full body — PRAGMA setup, optional unzip, plugin load + execution,
    Type A rename, partition columns, validations, sort, parquet write,
    schema fingerprint, manifest fragment, and scratch cleanup — is wired
    in Step 7.3. This skeleton exists so importers can resolve the symbols
    in Step 7.1.
    """
    raise NotImplementedError("run_shard is wired in Step 7.3")
