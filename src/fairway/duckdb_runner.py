"""DuckDB shard runner — Step 7.3 full pipeline integration.

Glues together connection setup, optional unzip, plugin load, the user
transform, Type A rename, partition-column injection, validations,
sort, parquet write, schema fingerprint, manifest fragment write, and
scratch cleanup. All exceptions are caught: a failure produces an
``error`` fragment plus a :class:`ShardResult` with ``status='error'``
— the Slurm task itself always exits 0; the manifest is the truth.
"""
from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import defaults, manifest, transform_loader, validations

if TYPE_CHECKING:  # pragma: no cover — import-only
    from .ctx import IngestCtx
    from .config import Config


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
    status: str = "ok"
    error: str | None = None


def _layer_root(cfg: "Config") -> Path:
    """Dataset's storage layer root (where ``_fragments/`` lives)."""
    overrides = {
        "raw": cfg.storage_raw,
        "processed": cfg.storage_processed,
        "curated": cfg.storage_curated,
    }
    base = overrides.get(cfg.layer) or (cfg.storage_root / cfg.layer)
    return Path(base) / cfg.dataset_name


def _configure(con: Any, ctx: "IngestCtx") -> None:
    cfg = ctx.config
    con.execute(f"PRAGMA threads={int(cfg.slurm_cpus_per_task)}")
    mem = cfg.slurm_mem.replace("'", "''")
    con.execute(f"PRAGMA memory_limit='{mem}'")
    tmp = str(ctx.scratch_dir).replace("'", "''")
    con.execute(f"PRAGMA temp_directory='{tmp}'")


def _describe_shard_output(
    con: Any,
    output_root: Path,
    partition_values: dict[str, str],
    partition_by: list[str],
) -> list[dict[str, str]]:
    """DESCRIBE only the parquet leaf written by THIS shard.

    Globbing the dataset root would let DuckDB intersect schemas across
    sibling shards (non-deterministic under Slurm-array concurrency). We
    join ``__<key>=<value>`` directories onto ``output_root`` in the
    same order as ``config.partition_by`` (DuckDB's ``write_parquet``
    Hive order) and glob only that leaf — sub-partition levels are
    picked up by the recursive ``**``. ``hive_partitioning=true``
    reconstitutes partition columns in the schema view.
    """
    leaf = output_root
    for k in partition_by:
        leaf = leaf / f"__{k}={partition_values[k]}"
    pattern = str(leaf).replace("'", "''") + "/**/*.parquet"
    rows = con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"
    ).fetchall()
    return [{"name": r[0], "dtype": r[1]} for r in rows]


def run_shard(con: Any, ctx: "IngestCtx") -> ShardResult:
    """Execute a single shard end-to-end and return a :class:`ShardResult`.

    Pipeline order matches PLAN.md Step 7.3 verbatim. Any exception is
    captured, persisted as an ``error`` fragment, and surfaced via the
    returned :class:`ShardResult` — never re-raised.
    """
    started = time.monotonic()
    cfg = ctx.config
    sources = [str(p) for p in ctx.input_paths]
    source_hashes: list[str] = []
    for p in ctx.input_paths:
        try:
            source_hashes.append(manifest.source_hash(p))
        except OSError:
            source_hashes.append("")
    encoding_used = cfg.encoding
    extracted_dirs: list[Path] = []
    layer_root = _layer_root(cfg)

    def _error_result(exc: BaseException) -> ShardResult:
        frag = manifest.build_fragment(
            partition_values=ctx.partition_values,
            source_files=sources,
            source_hashes=source_hashes,
            schema=[],
            row_count=0,
            status="error",
            error=str(exc),
            encoding_used=encoding_used,
        )
        try:
            manifest.write_fragment(frag, root_dir=layer_root)
        except OSError:
            pass
        return ShardResult(
            row_count=0,
            schema_fingerprint=frag["schema_fingerprint"],
            schema=[],
            output_paths=[],
            duration_seconds=time.monotonic() - started,
            encoding_used=encoding_used,
            status="error",
            error=str(exc),
        )

    try:
        _configure(con, ctx)

        active_ctx = ctx
        if cfg.unzip:
            extracted = defaults.unzip_inputs(
                zip_paths=ctx.input_paths,
                scratch_dir=ctx.scratch_dir,
                inner_pattern=cfg.zip_inner_pattern,
                password_file=cfg.zip_password_file,
            )
            extracted_dirs = sorted({p.parent for p in extracted})
            active_ctx = replace(ctx, input_paths=extracted)

        user_transform = transform_loader.load_transform(cfg.python)
        rel = user_transform(con, active_ctx)
        if cfg.apply_type_a:
            rel = defaults.apply_type_a(rel)
        for k in sorted(active_ctx.partition_values):
            v = str(active_ctx.partition_values[k]).replace("'", "''")
            rel = rel.project(f"*, '{v}' AS __{k}")
        validations.apply_validations(rel, cfg.validations)
        if cfg.sort_by:
            rel = rel.order(", ".join(cfg.sort_by))

        ctx.output_path.parent.mkdir(parents=True, exist_ok=True)
        partition_keys = [f"__{k}" for k in cfg.partition_by] + list(cfg.sub_partition_by)
        rel.write_parquet(
            str(ctx.output_path),
            compression="zstd",
            row_group_size=cfg.row_group_size,
            partition_by=partition_keys or None,
            overwrite=True,
        )

        schema = _describe_shard_output(
            con, ctx.output_path, active_ctx.partition_values, list(cfg.partition_by)
        )
        fp = manifest.schema_fingerprint(schema)
        row_count = int(rel.count("*").fetchone()[0])
        fragment = manifest.build_fragment(
            partition_values=ctx.partition_values,
            source_files=sources,
            source_hashes=source_hashes,
            schema=schema,
            row_count=row_count,
            status="ok",
            error=None,
            encoding_used=encoding_used,
        )
        manifest.write_fragment(fragment, root_dir=layer_root)

        output_paths = sorted(ctx.output_path.rglob("*.parquet"))
        return ShardResult(
            row_count=row_count,
            schema_fingerprint=fp,
            schema=[ColumnSpec(name=c["name"], dtype=c["dtype"]) for c in schema],
            output_paths=output_paths,
            duration_seconds=time.monotonic() - started,
            encoding_used=encoding_used,
        )
    except Exception as exc:
        return _error_result(exc)
    finally:
        for d in extracted_dirs:
            shutil.rmtree(d, ignore_errors=True)
