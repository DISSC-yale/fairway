"""Pipeline orchestrator (rewrite/v0.3 Step 8.1 skeleton).

Single-path orchestrator: enumerate shards from :mod:`fairway.batcher`,
build an :class:`~fairway.ctx.IngestCtx` per shard, open a DuckDB
in-memory connection, call :func:`fairway.duckdb_runner.run_shard`, and
collect :class:`~fairway.duckdb_runner.ShardResult` objects.

There is no engine selector and no batch-strategy dispatch — DuckDB is
the only execution engine in v0.3. The Slurm-array entry point passes
``shard_index=N`` to process exactly one shard; sequential local runs
(used for tests and small datasets) pass ``shard_index=None``.
"""
from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import manifest
from .batcher import PartitionBatcher
from .config import Config
from .ctx import IngestCtx
from .duckdb_runner import ShardResult, _layer_root, run_shard


@dataclass(frozen=True)
class PipelineResult:
    """Aggregated outcome of a :func:`run_pipeline` invocation."""

    total_shards: int
    ok_count: int
    error_count: int
    shard_results: list[ShardResult] = field(default_factory=list)


@dataclass(frozen=True)
class _ShardSpec:
    """Internal: a single shard's identity + inputs prior to context build."""

    shard_id: str
    input_paths: list[Path]
    partition_values: dict[str, str]


def _resolve_scratch_dir(config: Config) -> Path:
    """Pick a scratch directory: explicit config override, else env, else /tmp."""
    if config.scratch_dir is not None:
        return Path(config.scratch_dir)
    env = os.environ.get("SLURM_TMPDIR") or os.environ.get("TMPDIR") or "/tmp"
    return Path(env)


def _enumerate_shards(config: Config) -> list[_ShardSpec]:
    """Discover input files and group them into deterministic shards.

    Files whose basename does not match ``config.naming_pattern`` are
    silently skipped at this layer. Step 9.3 introduces the pre-scan
    validation gate (:func:`batcher.expand_and_validate`) that surfaces
    those mismatches with ``--allow-skip`` semantics.
    """
    files = sorted(glob.glob(config.source_glob))
    grouped = PartitionBatcher.group_files(
        files, config.naming_pattern, config.partition_by
    )
    specs: list[_ShardSpec] = []
    for values, paths in grouped.items():
        if values is None:
            continue
        partition_values = dict(zip(config.partition_by, values))
        shard_id = manifest.compute_task_id(partition_values)
        specs.append(
            _ShardSpec(
                shard_id=shard_id,
                input_paths=[Path(p) for p in sorted(paths)],
                partition_values=partition_values,
            )
        )
    specs.sort(key=lambda s: s.shard_id)
    return specs


def _build_ctx(config: Config, spec: _ShardSpec) -> IngestCtx:
    """Materialize a frozen :class:`IngestCtx` for one shard.

    ``output_path`` is the dataset's layer root (``storage_root/layer/
    dataset_name`` with overrides honored); the runner's ``write_parquet``
    call lays Hive ``__<key>=<val>`` subdirectories underneath it.
    """
    return IngestCtx(
        config=config,
        input_paths=spec.input_paths,
        output_path=_layer_root(config),
        partition_values=spec.partition_values,
        shard_id=spec.shard_id,
        scratch_dir=_resolve_scratch_dir(config),
    )


def _run_one(config: Config, spec: _ShardSpec) -> ShardResult:
    """Open an in-memory DuckDB connection and ingest one shard."""
    con = duckdb.connect(":memory:")
    try:
        return run_shard(con, _build_ctx(config, spec))
    finally:
        con.close()


def run_pipeline(
    config: Config, shard_index: int | None = None
) -> PipelineResult:
    """Drive Stage-1 ingest for one Slurm-array task or every shard.

    Parameters
    ----------
    config:
        Parsed :class:`~fairway.config.Config`.
    shard_index:
        ``None`` runs every shard sequentially in this process (used by
        tests and very small datasets). An ``int`` runs only the shard at
        that index in the deterministically sorted shard list — the Slurm
        array entry point passes its ``$SLURM_ARRAY_TASK_ID`` here.

    Notes
    -----
    Shard-level failures are absorbed by
    :func:`fairway.duckdb_runner.run_shard`; they appear as
    ``ShardResult(status="error")`` in :attr:`PipelineResult.shard_results`
    and are tallied in :attr:`PipelineResult.error_count`. The pipeline
    itself never re-raises a per-shard exception.
    """
    specs = _enumerate_shards(config)
    if shard_index is not None:
        if shard_index < 0 or shard_index >= len(specs):
            raise IndexError(
                f"shard_index {shard_index} out of range "
                f"(0..{len(specs) - 1 if specs else -1})"
            )
        results = [_run_one(config, specs[shard_index])]
    else:
        results = [_run_one(config, s) for s in specs]
    ok = sum(1 for r in results if r.status == "ok")
    err = sum(1 for r in results if r.status == "error")
    return PipelineResult(
        total_shards=len(specs),
        ok_count=ok,
        error_count=err,
        shard_results=results,
    )


# --- Transitional v0.2 shim ---------------------------------------------------
# A handful of pre-rewrite tests still ``from fairway.pipeline import
# IngestionPipeline`` at module scope; without an importable symbol the
# collect gate breaks. The class is intentionally non-functional — any
# instantiation surfaces a clear ``NotImplementedError`` pointing at the
# replacement entry point. Removed when the pre-rewrite test modules are
# culled in the Step 9 / Step 11 cleanup waves.
class IngestionPipeline:  # pragma: no cover - shim
    """Deprecated v0.2 shim. New code: :func:`run_pipeline`."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise NotImplementedError(
            "IngestionPipeline removed in v0.3 rewrite — use "
            "fairway.pipeline.run_pipeline(config) instead"
        )

