"""Per-shard DuckDB runner.

Pipeline order (one shard):

1. Bind a DuckDB view over the shard's input files.
2. Apply optional ``transform.py`` (identity if absent).
3. Apply Type A rename if ``config.apply_type_a``.
4. Apply ``schema.yaml`` CASTs and ``on_drift`` enforcement.
5. Run validations.
6. Sort if ``config.sort_by`` set.
7. ``COPY ... PARTITION_BY`` to ``storage_processed/<table>/``.
8. Write a leaf-keyed manifest fragment.

Shard-level errors are caught and produce a ``failed_shards`` fragment;
per-leaf errors set ``status="error"`` for that leaf only.
"""
from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import defaults, manifest, transform_loader
from .batcher import Shard
from .config import TableConfig
from .ctx import IngestCtx
from .schema import (
    DriftEvents,
    SchemaSpec,
    apply_schema,
    schema_fingerprint,
)
from .validations import apply_validations


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ShardResult:
    shard_id: str
    leaves_ok: list[str] = field(default_factory=list)
    leaves_error: list[str] = field(default_factory=list)
    failed: dict[str, Any] | None = None
    duration_seconds: float = 0.0


def _layer_root(config: TableConfig) -> Path:
    return Path(config.storage_processed) / config.table


def _format_path_sql(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _configure(con: Any, config: TableConfig, scratch_dir: Path) -> None:
    con.execute(f"PRAGMA threads={int(config.slurm_cpus_per_task)}")
    mem = config.slurm_mem.replace("'", "''")
    con.execute(f"PRAGMA memory_limit='{mem}'")
    con.execute(f"PRAGMA temp_directory='{str(scratch_dir).replace(chr(39), chr(39)*2)}'")


def _resolve_scratch(config: TableConfig) -> Path:
    if config.scratch_dir is not None:
        return Path(config.scratch_dir)
    import os
    env = os.environ.get("SLURM_TMPDIR") or os.environ.get("TMPDIR") or "/tmp"
    return Path(env)


def build_ctx(config: TableConfig, shard: Shard) -> IngestCtx:
    return IngestCtx(
        config=config,
        input_paths=shard.input_paths,
        output_root=_layer_root(config),
        partition_values=dict(shard.shard_values),
        leaves={leaf: list(files) for leaf, files in shard.leaves.items()},
        shard_id=shard.shard_id,
        scratch_dir=_resolve_scratch(config),
    )


def _hash_inputs(paths: list[Path]) -> list[str]:
    out: list[str] = []
    for p in paths:
        try:
            out.append(manifest.source_hash(p))
        except OSError:
            out.append("")
    return out


def run_shard(
    con: Any,
    config: TableConfig,
    schema: SchemaSpec,
    shard: Shard,
) -> ShardResult:
    """Execute one shard end-to-end. Always writes a fragment."""
    started = time.monotonic()
    ctx = build_ctx(config, shard)
    layer_root = ctx.output_root
    layer_root.mkdir(parents=True, exist_ok=True)
    schema_fp = schema_fingerprint(schema)
    leaves_payload: dict[str, dict[str, Any]] = {}
    leaves_ok: list[str] = []
    leaves_error: list[str] = []
    fail_record: dict[str, Any] | None = None
    extracted_dirs: list[Path] = []

    try:
        _configure(con, config, ctx.scratch_dir)

        active_inputs_by_leaf: dict[str, list[Path]] = {}
        if config.unzip:
            for leaf, files in ctx.leaves.items():
                extracted = defaults.unzip_inputs(
                    zip_paths=files,
                    scratch_dir=ctx.scratch_dir,
                    inner_pattern=config.zip_inner_pattern,
                    password_file=config.zip_password_file,
                )
                extracted_dirs.extend(sorted({p.parent for p in extracted}))
                active_inputs_by_leaf[leaf] = extracted
        else:
            active_inputs_by_leaf = {leaf: list(files) for leaf, files in ctx.leaves.items()}

        user_transform = transform_loader.load_transform(config.table_dir)

        for leaf, files in active_inputs_by_leaf.items():
            source_files = [str(p) for p in ctx.leaves[leaf]]
            source_hashes = _hash_inputs(ctx.leaves[leaf])
            try:
                encoding_used = defaults.bind_source_view(con, files, config, ctx.input_view)
                if user_transform is not None:
                    rel = user_transform(con, ctx)
                else:
                    rel = con.sql(f"SELECT * FROM {ctx.input_view}")
                if config.apply_type_a:
                    rel = defaults.apply_type_a(rel)
                rel, drift = apply_schema(con, rel, schema)
                apply_validations(rel, config.validations)
                if config.sort_by:
                    rel = rel.order(", ".join(config.sort_by))
                leaf_dir = layer_root / leaf
                leaf_dir.mkdir(parents=True, exist_ok=True)
                leaf_file = leaf_dir / "data.parquet"
                rel.write_parquet(
                    str(leaf_file),
                    compression="zstd",
                    row_group_size=config.row_group_size,
                )
                row_count = int(con.sql(
                    f"SELECT COUNT(*) FROM read_parquet({_format_path_sql(str(leaf_file))})"
                ).fetchone()[0])
                drift_events = None if drift.is_empty() else drift.to_dict()
                payload: dict[str, Any] = {
                    "source_files": source_files,
                    "source_hashes": source_hashes,
                    "schema_fingerprint": schema_fp,
                    "row_count": row_count,
                    "status": "ok",
                    "ingest_ts": manifest.utc_now_iso(),
                }
                if drift_events:
                    payload["drift_events"] = drift_events
                if encoding_used != config.encoding:
                    payload["encoding_used"] = encoding_used
                leaves_payload[leaf] = payload
                leaves_ok.append(leaf)
            except Exception as exc:
                logger.exception("leaf %s failed", leaf)
                leaves_payload[leaf] = {
                    "source_files": source_files,
                    "source_hashes": source_hashes,
                    "schema_fingerprint": schema_fp,
                    "row_count": 0,
                    "status": "error",
                    "ingest_ts": manifest.utc_now_iso(),
                }
                leaves_error.append(leaf)
                if fail_record is None:
                    fail_record = {
                        "shard_id": shard.shard_id,
                        "error_message": str(exc),
                        "failed_at": manifest.utc_now_iso(),
                        "leaves_attempted": [],
                    }
                fail_record["leaves_attempted"].append(leaf)
    except Exception as exc:
        logger.exception("shard %s failed at setup", shard.shard_id)
        fail_record = {
            "shard_id": shard.shard_id,
            "error_message": str(exc),
            "failed_at": manifest.utc_now_iso(),
            "leaves_attempted": list(ctx.leaves),
        }
    finally:
        for d in extracted_dirs:
            shutil.rmtree(d, ignore_errors=True)

    successful_leaves = {k: v for k, v in leaves_payload.items() if v["status"] == "ok"}
    manifest.write_fragment(
        config.table_dir,
        shard.shard_id,
        layer="processed",
        leaves=successful_leaves,
        failed=fail_record,
        shard_by=list(config.shard_by),
    )
    return ShardResult(
        shard_id=shard.shard_id,
        leaves_ok=sorted(leaves_ok),
        leaves_error=sorted(leaves_error),
        failed=fail_record,
        duration_seconds=time.monotonic() - started,
    )
