"""Slurm submission helpers for ``fairway submit``.

Two jobs per submit:

1. The array job (one task per shard) running ``fairway _shard``.
2. A dependency job (``afterany``) running ``fairway finalize``, so the
   per-table manifest is merged exactly once after the array completes
   (regardless of any task failures).

Idempotent resume reads ``tables/<t>/manifest.json`` per leaf — never
the fragment dir.
"""
from __future__ import annotations

import json
import logging
import os
import re
from importlib import resources
from pathlib import Path
from typing import Any

from .batcher import Shard, enumerate_shards, validate_shard_by
from .config import TableConfig
from .pipeline import _resumable_shards
from .schema import (
    SchemaSpec,
    schema_fingerprint,
    validate_schema_vs_config,
)


logger = logging.getLogger(__name__)


def _load_template() -> str:
    return (resources.files("fairway.slurm_templates")
            .joinpath("job_array.sh").read_text(encoding="utf-8"))


_TEMPLATE = _load_template()


def render_array_script(
    config: TableConfig,
    n_shards: int,
    shards_file: Path,
) -> str:
    """Render the per-array sbatch script."""
    return _TEMPLATE.format(
        table=config.table,
        account=(f"#SBATCH --account={config.slurm_account}\n"
                 if config.slurm_account else ""),
        partition=(f"#SBATCH --partition={config.slurm_partition}\n"
                   if config.slurm_partition else ""),
        last=max(n_shards - 1, 0),
        conc=config.slurm_concurrency,
        time=config.slurm_time,
        mem=config.slurm_mem,
        cpus=config.slurm_cpus_per_task,
        root=config.project_root,
        shards=shards_file.resolve(),
    )


def render_finalize_script(config: TableConfig) -> str:
    """Render a single-task sbatch script that runs ``fairway finalize``."""
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name=fairway-finalize-{config.table}",
    ]
    if config.slurm_account:
        lines.append(f"#SBATCH --account={config.slurm_account}")
    if config.slurm_partition:
        lines.append(f"#SBATCH --partition={config.slurm_partition}")
    lines.extend([
        "#SBATCH --time=0:30:00",
        "#SBATCH --mem=4G",
        "#SBATCH --cpus-per-task=1",
        f"#SBATCH --output=logs/fairway-finalize-{config.table}-%j.out",
        "set -euo pipefail",
        "mkdir -p logs",
        f'cd "{config.project_root}"',
        "source .venv/bin/activate",
        f"exec python -m fairway finalize {config.table}",
        "",
    ])
    return "\n".join(lines)


def shards_payload(
    config: TableConfig, shards: list[Shard],
) -> dict[str, Any]:
    """JSON payload consumed by ``fairway _shard`` workers."""
    return {
        "table": config.table,
        "project_root": str(config.project_root),
        "shards": [
            {
                "shard_id": s.shard_id,
                "shard_values": dict(s.shard_values),
                "leaves": {
                    leaf: [str(p) for p in files]
                    for leaf, files in s.leaves.items()
                },
            }
            for s in shards
        ],
    }


def write_artifacts(
    config: TableConfig, shards: list[Shard],
) -> tuple[Path, Path, Path]:
    """Render shards.json + array sbatch + finalize sbatch.

    Returns ``(shards_file, array_script, finalize_script)``. Files land
    under ``<project_root>/build/sbatch/<table>/``.
    """
    out_dir = config.project_root / "build" / "sbatch" / config.table
    out_dir.mkdir(parents=True, exist_ok=True)
    shards_file = out_dir / "shards.json"
    shards_file.write_text(
        json.dumps(shards_payload(config, shards), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    array_script = out_dir / "array.sh"
    array_script.write_text(render_array_script(config, len(shards), shards_file),
                            encoding="utf-8")
    array_script.chmod(0o755)
    finalize_script = out_dir / "finalize.sh"
    finalize_script.write_text(render_finalize_script(config), encoding="utf-8")
    finalize_script.chmod(0o755)
    return shards_file, array_script, finalize_script


def parse_array_job_id(stdout: str) -> str:
    m = re.search(r"Submitted batch job (\d+)", stdout)
    if not m:
        raise ValueError(f"Could not parse array job id from: {stdout!r}")
    return m.group(1)


def filter_resumable_shards(
    shards: list[Shard],
    table_dir: Path,
    schema_fp: str,
    *,
    force: bool = False,
) -> tuple[list[Shard], list[Shard]]:
    """Idempotent-resume gate consulted at submit time.

    Reads ``tables/<t>/manifest.json`` only — never the fragment dir.
    """
    return _resumable_shards(shards, table_dir, schema_fp, force=force)


def plan_submission(
    config: TableConfig,
    schema: SchemaSpec,
    *,
    force: bool = False,
) -> tuple[list[Shard], list[Shard], list[Path]]:
    """Validation + enumeration + resume filter — no Slurm calls.

    Returns ``(to_run, skipped, unmatched)``.
    """
    validate_shard_by(config)
    validate_schema_vs_config(schema, config.source_format)
    shards, unmatched = enumerate_shards(config)
    schema_fp = schema_fingerprint(schema)
    to_run, skipped = filter_resumable_shards(
        shards, config.table_dir, schema_fp, force=force,
    )
    return to_run, skipped, unmatched


def run_sbatch(script: Path) -> str:
    """Invoke ``sbatch <script>``; return the parsed job id."""
    import subprocess
    proc = subprocess.run(
        ["sbatch", str(script)], check=True, capture_output=True, text=True,
    )
    return parse_array_job_id(proc.stdout)


def run_sbatch_dependency(script: Path, after_jid: str) -> str:
    """Submit ``script`` with ``--dependency=afterany:<after_jid>``."""
    import subprocess
    proc = subprocess.run(
        ["sbatch", f"--dependency=afterany:{after_jid}", str(script)],
        check=True, capture_output=True, text=True,
    )
    return parse_array_job_id(proc.stdout)


def format_submission_summary(
    n_shards: int, n_skipped: int, array_jid: str, finalize_jid: str,
) -> str:
    return (
        f"Submitted {n_shards} shard(s) (skipped {n_skipped} up-to-date).\n"
        f"  array job:    {array_jid}\n"
        f"  finalize job: {finalize_jid} (afterany:{array_jid})"
    )


def handle_unmatched(unmatched: list[Path]) -> None:
    """Hard-error on any unmatched files (no --allow-skip in v0.3 surface)."""
    if not unmatched:
        return
    import click
    import sys
    click.echo(
        f"Error: {len(unmatched)} file(s) in source_glob did not match naming_pattern:",
        err=True,
    )
    for p in unmatched:
        click.echo(f"  {p}", err=True)
    sys.exit(2)
