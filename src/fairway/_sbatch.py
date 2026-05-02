"""sbatch script + shards-manifest helpers for ``fairway submit``.

The Step 10 work (`slurm_templates/job_array.sh`) will replace ``_TEMPLATE``
with a file-on-disk loaded via :mod:`importlib.resources`. Until then the
inline template here keeps Step 9.2 bounded to one new module plus the
``submit`` body in :mod:`fairway.cli`. Variable substitution uses plain
``str.format`` — no Jinja, no DSL (locked decision in PLAN.md).
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import click

from . import manifest
from .batcher import ShardSpec
from .config import Config

_TEMPLATE = """\
#!/bin/bash
#SBATCH --job-name=fairway-{dataset}
{account}{partition}#SBATCH --array=0-{last}%{conc}
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --output=logs/fairway-{dataset}-%A_%a.out
set -euo pipefail
mkdir -p logs
cd {root}
source .venv/bin/activate
exec python -m fairway run --shards-file {shards} --shard-index $SLURM_ARRAY_TASK_ID
"""


def render_script(
    config: Config,
    n_shards: int,
    shards_file: Path,
    *,
    project_root: Path | None = None,
) -> str:
    """Render the per-array sbatch script.

    ``slurm_account`` / ``slurm_partition`` directives are emitted only
    when set; this lets tests render scripts without pinning a real site
    config while still matching real-cluster requirements when operators
    populate them.
    """
    return _TEMPLATE.format(
        dataset=config.dataset_name,
        account=(f"#SBATCH --account={config.slurm_account}\n"
                 if config.slurm_account else ""),
        partition=(f"#SBATCH --partition={config.slurm_partition}\n"
                   if config.slurm_partition else ""),
        last=max(n_shards - 1, 0),
        conc=config.slurm_concurrency,
        time=config.slurm_time,
        mem=config.slurm_mem,
        cpus=config.slurm_cpus_per_task,
        root=Path(project_root) if project_root else Path.cwd(),
        shards=shards_file.resolve(),
    )


def parse_array_job_id(stdout: str) -> str:
    """Extract the array job id from ``sbatch`` stdout."""
    m = re.search(r"Submitted batch job (\d+)", stdout)
    if not m:
        raise ValueError(f"Could not parse array job id from: {stdout!r}")
    return m.group(1)


def shards_payload(specs: list, config_yaml: Path) -> dict[str, Any]:
    """Build the ``shards.json`` payload (deterministic by ``shard_id``)."""
    shards = sorted(
        [{"shard_id": s.shard_id,
          "input_paths": [str(p) for p in s.input_paths],
          "partition_values": dict(s.partition_values)} for s in specs],
        key=lambda d: d["shard_id"])
    return {"config": str(config_yaml.resolve()), "shards": shards}


def write_artifacts(
    config: Config, shards: list[ShardSpec], config_yaml: Path,
) -> tuple[Path, Path]:
    """Render ``shards.json`` + sbatch script under ``build/sbatch/``.

    Returns ``(shards_path, script_path)``. The script is marked executable.
    """
    out_dir = Path("build/sbatch")
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = shards_payload(shards, config_yaml)
    shards_path = out_dir / f"{config.dataset_name}.shards.json"
    shards_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    script = out_dir / f"{config.dataset_name}.sh"
    script.write_text(
        render_script(config, len(payload["shards"]), shards_path),
        encoding="utf-8")
    script.chmod(0o755)
    return shards_path, script


def _shard_resumable(shard: ShardSpec, layer_root: Path) -> bool:
    """Step 9.4 hash-comparison gate: True iff shard's existing fragment is
    ``status="ok"`` and every recorded ``source_hash`` matches the current
    on-disk file hash via :func:`fairway.manifest.source_hash`."""
    frag_path = manifest.fragment_path(layer_root, shard.shard_id)
    if not frag_path.is_file():
        return False
    try:
        frag = json.loads(frag_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    if frag.get("status") != "ok":
        return False
    recorded_files = list(frag.get("source_files") or [])
    recorded_hashes = list(frag.get("source_hashes") or [])
    current_paths = [str(p) for p in shard.input_paths]
    if recorded_files != current_paths:
        return False
    if len(recorded_hashes) != len(current_paths):
        return False
    for path, recorded in zip(current_paths, recorded_hashes):
        try:
            if manifest.source_hash(path) != recorded:
                return False
        except OSError:
            return False
    return True


def filter_resumable_shards(
    matched_shards: list[ShardSpec], layer_root: Path, force: bool,
) -> tuple[list[ShardSpec], list[ShardSpec]]:
    """Idempotent-resume filter (Step 9.4).

    Returns ``(shards_to_submit, shards_skipped_existing_ok)``. With
    ``force=True`` every matched shard is re-submitted (skipped list empty).
    Otherwise a shard is skipped iff a fragment exists at
    ``layer_root/_fragments/<shard_id>.json`` with ``status="ok"`` AND every
    recorded ``source_hash`` matches the current file hash (in order matching
    ``fragment.source_files``). Anything else — missing fragment, status
    ``"error"``, hash drift, file-list drift, corrupt JSON — re-submits.
    """
    if force:
        return list(matched_shards), []
    to_submit: list[ShardSpec] = []
    skipped: list[ShardSpec] = []
    for shard in matched_shards:
        if _shard_resumable(shard, layer_root):
            skipped.append(shard)
        else:
            to_submit.append(shard)
    return to_submit, skipped


def _atomic_write_json(target: Path, payload: Any) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True),
                   encoding="utf-8")
    os.rename(tmp, target)


def write_submission_record(
    layer_root: Path, *, sbatch_script: Path, array_job_id: str,
    n_shards: int, ts: str, chunk_index: int = 0,
) -> Path:
    """Atomically write a submission record under ``manifest/_submissions``."""
    record = {"submitted_at": ts, "chunk_index": chunk_index,
              "sbatch_script": str(sbatch_script),
              "array_job_id": array_job_id, "n_shards": n_shards}
    target = (layer_root / "manifest" / "_submissions"
              / f"{ts.replace(':', '-')}.json")
    _atomic_write_json(target, record)
    return target


def write_skipped_log(
    layer_root: Path, unmatched: list[Path], ts: str,
) -> Path:
    """Atomically write the Step 9.3 ``--allow-skip`` log."""
    payload = [{"file": str(p), "reason": "naming_pattern mismatch"}
               for p in unmatched]
    target = (layer_root / "manifest" / "_skipped"
              / f"{ts.replace(':', '-')}.json")
    _atomic_write_json(target, payload)
    return target


def handle_unmatched(
    unmatched: list[Path], layer_root: Path,
    *, allow_skip: bool, dry_run: bool,
) -> None:
    """Step 9.3 pre-scan: hard-error, dry-run-print, or write _skipped log."""
    if not unmatched:
        return
    if not allow_skip:
        click.echo(f"Error: {len(unmatched)} file(s) in source_glob did not "
                   "match naming_pattern:", err=True)
        for p in unmatched:
            click.echo(f"  {p}", err=True)
        sys.exit(2)
    click.echo(f"{'Would skip' if dry_run else 'Skipping'} {len(unmatched)} "
               "file(s) (naming_pattern mismatch):")
    for p in unmatched:
        click.echo(f"  {p}")
    if not dry_run:
        write_skipped_log(layer_root, unmatched, manifest.utc_now_iso())
