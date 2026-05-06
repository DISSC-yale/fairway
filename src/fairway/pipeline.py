"""Pipeline orchestrator — single-machine and per-task entry points.

``run_inline`` is the single-machine path used by ``fairway run``: it
clears ``last_run``, executes every (non-skippable) shard sequentially,
and finalizes the manifest at the end. ``run_one_shard`` is the
per-task entry point used by Slurm array workers (``fairway _shard``).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from . import manifest as _manifest
from .batcher import Shard, enumerate_shards, validate_shard_by
from .config import TableConfig
from .duckdb_runner import ShardResult, run_shard
from .schema import (
    SchemaSpec,
    schema_fingerprint,
    validate_schema_vs_config,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    total_shards: int
    submitted: int
    skipped: int
    shard_results: list[ShardResult] = field(default_factory=list)


def _resumable_shards(
    shards: list[Shard],
    table_dir: Path,
    schema_fp: str,
    *,
    force: bool,
) -> tuple[list[Shard], list[Shard]]:
    """Split shards into ``(to_run, skipped)`` based on per-leaf manifest state."""
    if force:
        return list(shards), []
    manifest = _manifest.load_manifest(table_dir)
    to_run: list[Shard] = []
    skipped: list[Shard] = []
    for shard in shards:
        all_valid = True
        for leaf, files in shard.leaves.items():
            sources = [str(p) for p in files]
            try:
                hashes = [_manifest.source_hash(p) for p in files]
            except OSError:
                all_valid = False
                break
            if not _manifest.is_leaf_valid(
                manifest, "processed", leaf, sources, hashes, schema_fp,
            ):
                all_valid = False
                break
        if all_valid:
            skipped.append(shard)
        else:
            to_run.append(shard)
    return to_run, skipped


def run_inline(
    config: TableConfig,
    schema: SchemaSpec,
    *,
    force: bool = False,
) -> PipelineResult:
    """Single-machine ingest: enumerate, run sequentially, finalize."""
    validate_shard_by(config)
    validate_schema_vs_config(schema, config.source_format)
    shards, unmatched = enumerate_shards(config)
    if unmatched:
        names = "\n  ".join(str(p) for p in unmatched)
        raise RuntimeError(
            f"{len(unmatched)} input file(s) did not match naming_pattern:\n  {names}"
        )

    schema_fp = schema_fingerprint(schema)
    manifest = _manifest.load_manifest(config.table_dir)
    _manifest.clear_last_run(manifest)
    _manifest.save_manifest(manifest, config.table_dir)

    to_run, skipped = _resumable_shards(shards, config.table_dir, schema_fp, force=force)
    results: list[ShardResult] = []
    con = duckdb.connect(":memory:")
    try:
        for shard in to_run:
            results.append(run_shard(con, config, schema, shard))
    finally:
        con.close()

    _manifest.finalize(
        config.table_dir,
        layer="processed",
        shard_by=list(config.shard_by),
    )
    return PipelineResult(
        total_shards=len(shards),
        submitted=len(to_run),
        skipped=len(skipped),
        shard_results=results,
    )


def run_shard_direct(
    config: TableConfig,
    schema: SchemaSpec,
    shard: Shard,
) -> ShardResult:
    """Worker entry point: execute the *given* shard, without re-enumeration.

    This is the correct path for Slurm array workers: ``shards.json`` is the
    pre-computed (and possibly resume-filtered) subset; re-globbing the source
    tree from the worker would let `to_run[k]` and `enumerate_shards()[k]`
    diverge once any shard is skipped, silently processing the wrong leaves.
    """
    validate_shard_by(config)
    validate_schema_vs_config(schema, config.source_format)
    con = duckdb.connect(":memory:")
    try:
        return run_shard(con, config, schema, shard)
    finally:
        con.close()
