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
from pathlib import Path
from typing import Any

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
