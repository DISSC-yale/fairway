"""sbatch script + shards-manifest helpers for ``fairway submit``.

The job-array sbatch template lives at ``slurm_templates/job_array.sh`` and is
loaded via :mod:`importlib.resources` so it ships in the wheel via
``[tool.setuptools.package-data]``. Variable substitution uses plain
``str.format`` — no Jinja, no DSL (locked decision in PLAN.md).
"""
from __future__ import annotations

import json
import os
import re
import sys
from importlib import resources
from pathlib import Path
from typing import Any

import click

from . import manifest
from .batcher import ShardSpec
from .config import Config


def _load_template() -> str:
    """Read ``slurm_templates/job_array.sh`` from package data."""
    return (resources.files("fairway.slurm_templates")
            .joinpath("job_array.sh").read_text(encoding="utf-8"))


_TEMPLATE = _load_template()


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
    *, chunk_index: int | None = None,
) -> tuple[Path, Path]:
    """Render ``shards.json`` + sbatch script under ``build/sbatch/``.

    When ``chunk_index`` is ``None`` (single-chunk path), files are named
    ``<dataset>.shards.json`` / ``<dataset>.sh``. When ``chunk_index`` is an
    int (Step 9.5 chunked path), names embed ``-chunk-<i>``. Returns
    ``(shards_path, script_path)``; the script is marked executable.
    """
    out_dir = Path("build/sbatch")
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = shards_payload(shards, config_yaml)
    suffix = f"-chunk-{chunk_index}" if chunk_index is not None else ""
    shards_path = out_dir / f"{config.dataset_name}{suffix}.shards.json"
    shards_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    script = out_dir / f"{config.dataset_name}{suffix}.sh"
    script.write_text(
        render_script(config, len(payload["shards"]), shards_path),
        encoding="utf-8")
    script.chmod(0o755)
    return shards_path, script


def chunk_shards(
    shards: list[ShardSpec], chunk_size: int,
) -> list[list[ShardSpec]]:
    """Split ``shards`` into contiguous slices of ``<= chunk_size`` (Step 9.5).

    Single chunk when ``len(shards) <= chunk_size`` — caller can detect by
    checking ``len(result) == 1``. Raises ``ValueError`` for non-positive
    ``chunk_size``.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    if not shards:
        return []
    return [shards[i:i + chunk_size]
            for i in range(0, len(shards), chunk_size)]


def write_chunk_artifacts(
    config: Config, chunks: list[list[ShardSpec]], config_yaml: Path,
) -> list[Path]:
    """Render artifacts for each chunk; return script paths in chunk-index order.

    Single-chunk case (``len(chunks) == 1``) keeps the un-suffixed legacy
    filenames; multi-chunk case uses ``<dataset>-chunk-<i>.{shards.json,sh}``.
    """
    is_chunked = len(chunks) > 1
    scripts: list[Path] = []
    for i, chunk in enumerate(chunks):
        _, script = write_artifacts(
            config, chunk, config_yaml,
            chunk_index=i if is_chunked else None)
        scripts.append(script)
    return scripts


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
    n_shards: int, ts: str, chunk_index: int = 0, total_chunks: int = 1,
) -> Path:
    """Atomically write a submission record under ``manifest/_submissions``.

    Single-chunk submissions (``total_chunks == 1``) keep the legacy
    ``<ts>.json`` filename so the existing one-record-per-submit invariant
    holds. Chunked submissions (``total_chunks > 1``) embed ``-chunk-<i>`` in
    the filename so all chunks of one submission can coexist while sharing
    the same ``submitted_at`` field for aggregation in :func:`read_recent_submission_records`.
    """
    record = {"submitted_at": ts, "chunk_index": chunk_index,
              "sbatch_script": str(sbatch_script),
              "array_job_id": array_job_id, "n_shards": n_shards}
    base = ts.replace(":", "-")
    name = f"{base}-chunk-{chunk_index}.json" if total_chunks > 1 else f"{base}.json"
    target = layer_root / "manifest" / "_submissions" / name
    _atomic_write_json(target, record)
    return target


def read_recent_submission_records(layer_root: Path) -> list[dict[str, Any]]:
    """Return all submission records sharing the most recent ``submitted_at``.

    Used by ``fairway status`` to aggregate across chunks of one submission
    (Step 9.5). Sorted by ``chunk_index``. Empty list if none exist.
    """
    sub_dir = layer_root / "manifest" / "_submissions"
    if not sub_dir.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for f in sorted(sub_dir.glob("*.json")):
        try:
            records.append(json.loads(f.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            continue
    if not records:
        return []
    most_recent_ts = max(r.get("submitted_at", "") for r in records)
    same = [r for r in records if r.get("submitted_at") == most_recent_ts]
    return sorted(same, key=lambda r: r.get("chunk_index", 0))


def format_status_summary(
    layer_root: Path,
) -> tuple[list[str], list[str]]:
    """Build the human-readable lines + job-ids-to-squeue for ``fairway status``.

    Returns ``(lines, job_ids)``. ``job_ids`` is non-empty only when the most
    recent submission was chunked (>1 records); the caller is responsible for
    invoking ``squeue`` and feeding outputs back to :func:`aggregate_squeue_states`.
    """
    records = read_recent_submission_records(layer_root)
    if not records:
        sub_dir = layer_root / "manifest" / "_submissions"
        return [f"No submission record under {sub_dir}"], []
    if len(records) == 1:
        rec = records[0]
        lines = [f"Most recent submission: {rec.get('submitted_at')}"]
        for k in ("array_job_id", "n_shards", "sbatch_script"):
            if k in rec:
                lines.append(f"  {k}: {rec[k]}")
        return lines, []
    total = sum(int(r.get("n_shards", 0)) for r in records)
    ids = [str(r.get("array_job_id", "?")) for r in records]
    return [f"Most recent submission: {len(records)} chunks at "
            f"{records[0].get('submitted_at')} (total {total} shards). "
            f"Job IDs: {', '.join(ids)}."], ids


def aggregate_squeue_states(squeue_outputs: list[str]) -> dict[str, int]:
    """Tally Slurm task states across multiple ``squeue -h ... -o '%T'`` outputs."""
    counts: dict[str, int] = {}
    for output in squeue_outputs:
        for line in output.splitlines():
            state = line.strip()
            if state:
                counts[state] = counts.get(state, 0) + 1
    return counts


def format_submission_summary(
    n_shards: int, n_skipped: int, n_chunks: int, job_ids: list[str],
) -> str:
    """Render the operator-facing summary line for ``fairway submit``."""
    if n_chunks > 1:
        return (f"Submitting {n_chunks} chunks (total {n_shards} shards, "
                f"skipped {n_skipped} existing-ok shards). "
                f"Job IDs: {', '.join(job_ids)}.")
    return (f"Submitting {n_shards} shards (skipped {n_skipped} "
            f"existing-ok shards). Submit array {job_ids[0]}.")


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
