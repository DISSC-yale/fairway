"""fairway CLI — rewrite/v0.3 Step 9.1 skeleton (≤ 280 LOC after 9.5)."""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import click

from . import _init_templates as _t
from . import manifest as _manifest
from .config import load_config


@click.group()
def main() -> None:
    """fairway — shuffle-free Slurm-array data ingestion."""


def _scaffold_project(root: Path) -> None:
    for sub in ("datasets", "transforms/raw_to_processed",
                "transforms/processed_to_curated", "data", "build"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (root / ".gitignore").write_text(_t.GITIGNORE, encoding="utf-8")
    (root / "README.md").write_text(_t.README.format(name=root.name), encoding="utf-8")


def _scaffold_dataset(root: Path, name: str) -> None:
    (root / "datasets").mkdir(parents=True, exist_ok=True)
    for ext, tmpl in (("yaml", _t.DATASET_YAML), ("py", _t.DATASET_PY)):
        (root / "datasets" / f"{name}.{ext}").write_text(
            tmpl.format(name=name), encoding="utf-8")


@main.command()
@click.argument("project_dir", type=click.Path(file_okay=False))
@click.option("--dataset", "dataset_name", help="Add a dataset scaffold.")
@click.option("--init-project", is_flag=True,
              help="Force project scaffold even if directory looks initialized.")
@click.option("--force", is_flag=True,
              help="Overwrite a non-empty <project_dir> on first init.")
def init(project_dir: str, dataset_name: str | None,
         init_project: bool, force: bool) -> None:
    """Scaffold a fairway project (and optionally a dataset)."""
    root = Path(project_dir)
    initialized = root.exists() and (root / "datasets").is_dir()
    is_first = not initialized or init_project
    if (is_first and root.exists() and any(root.iterdir())
            and not force and not init_project):
        raise click.ClickException(
            f"Path {project_dir!r} exists and is not empty. "
            "Re-run with --force to overwrite, or pass --dataset <name>."
        )
    if is_first:
        _scaffold_project(root)
    if dataset_name:
        _scaffold_dataset(root, dataset_name)
    click.echo(f"Initialized {project_dir}")


def _build_ctx_from_shard(config, shard):
    """Mirror ``pipeline._build_ctx`` for an entry from the shards file."""
    import os
    from .ctx import IngestCtx
    from .duckdb_runner import _layer_root
    scratch = (Path(config.scratch_dir) if config.scratch_dir is not None
               else Path(os.environ.get("SLURM_TMPDIR")
                         or os.environ.get("TMPDIR") or "/tmp"))
    return IngestCtx(
        config=config,
        input_paths=[Path(p) for p in shard["input_paths"]],
        output_path=_layer_root(config),
        partition_values=dict(shard.get("partition_values") or {}),
        shard_id=str(shard["shard_id"]),
        scratch_dir=scratch)


@main.command()
@click.option("--shards-file", required=True,
              type=click.Path(exists=True, dir_okay=False))
@click.option("--shard-index", required=True, type=int)
def run(shards_file: str, shard_index: int) -> None:
    """Run one shard by index from the shards manifest. Always exits 0 on
    shard error (manifest fragment is the truth); pipeline-level errors
    (config parse, plugin load, missing file) DO exit non-zero.

    Shards file shape (Step 9.2 may refine):
    ``{"config": "<path/to/config.yaml>", "shards": [<entry>, ...]}``.
    """
    import duckdb
    from .duckdb_runner import run_shard
    payload = json.loads(Path(shards_file).read_text(encoding="utf-8"))
    if not (isinstance(payload, dict) and "config" in payload and "shards" in payload):
        raise click.ClickException(
            "shards file must be {'config': <path>, 'shards': [...]} "
            "(Step 9.2 finalises this format).")
    shards = payload["shards"]
    if shard_index < 0 or shard_index >= len(shards):
        raise click.ClickException(
            f"shard_index {shard_index} out of range (0..{len(shards) - 1})")
    config = load_config(payload["config"])
    ctx = _build_ctx_from_shard(config, shards[shard_index])
    con = duckdb.connect(":memory:")
    try:
        run_shard(con, ctx)
    finally:
        con.close()


@main.command()
@click.option("--dataset", "dataset_name", required=True)
@click.option("--storage-root", "storage_root", default="data", show_default=True,
              type=click.Path(file_okay=False))
@click.option("--layer", default="raw", show_default=True,
              type=click.Choice(["raw", "processed", "curated"]))
def status(dataset_name: str, storage_root: str, layer: str) -> None:
    """Print fragment-level status + most recent submission record(s).

    For chunked submissions (Step 9.5) aggregates ``squeue`` task states
    across every chunk's array job id when ``squeue`` is on PATH.
    """
    from . import _sbatch
    layer_root = Path(storage_root) / layer / dataset_name
    lines, job_ids = _sbatch.format_status_summary(layer_root)
    for line in lines:
        click.echo(line)
    if job_ids and shutil.which("squeue"):
        outs = [subprocess.run(["squeue", "-h", "-j", jid, "-o", "%T"],
                               capture_output=True, text=True, check=False).stdout
                for jid in job_ids]
        states = _sbatch.aggregate_squeue_states(outs)
        if states:
            click.echo("Slurm states (aggregated): "
                       + ", ".join(f"{k}={v}" for k, v in sorted(states.items())))
    counts: dict[str, int] = {"ok": 0, "error": 0}
    frag_dir = layer_root / "_fragments"
    for f in (frag_dir.glob("*.json") if frag_dir.is_dir() else ()):
        try:
            s = json.loads(f.read_text(encoding="utf-8")).get("status", "error")
        except (json.JSONDecodeError, OSError):
            continue
        counts[s] = counts.get(s, 0) + 1
    click.echo(f"Fragments: ok={counts.get('ok', 0)}  error={counts.get('error', 0)}")


@main.command()
@click.argument("transform_yaml", type=click.Path(exists=True, dir_okay=False))
def transform(transform_yaml: str) -> None:
    """Stage-2 transform (full wiring deferred to Step 9.x — see PLAN.md)."""
    raise click.ClickException(
        f"fairway transform: full Stage-2 wiring lands in Step 9.x "
        f"(see PLAN.md). Requested transform={transform_yaml!r}."
    )


@main.command()
@click.argument("dataset_path", type=click.Path(exists=True))
@click.option("--rules", required=True,
              type=click.Path(exists=True, dir_okay=False))
def validate(dataset_path: str, rules: str) -> None:
    """Apply validation rules to a landed parquet dataset.

    Exit 0 on all-pass; exit 2 on any failure.
    """
    import duckdb
    import yaml
    from .config import _parse_validations
    from .validations import ShardValidationError, apply_validations
    spec = yaml.safe_load(Path(rules).read_text(encoding="utf-8")) or {}
    validations_cfg = _parse_validations(spec.get("validations") if isinstance(spec, dict) else spec)
    con = duckdb.connect(":memory:")
    glob = str(Path(dataset_path)).replace("'", "''") + "/**/*.parquet"
    try:
        rel = con.sql(f"SELECT * FROM read_parquet('{glob}')")
        try:
            apply_validations(rel, validations_cfg)
        except ShardValidationError as exc:
            click.echo(f"FAIL  {exc}", err=True)
            sys.exit(2)
    finally:
        con.close()
    click.echo(f"OK    all checks passed: {dataset_path}")


@main.command()
def enrich() -> None:
    """Deferred stub — see PLAN.md re-entry triggers."""
    click.echo("enrich is deferred — see PLAN.md re-entry triggers", err=True)
    sys.exit(2)


@main.command()
@click.argument("dataset_path", type=click.Path(exists=True))
@click.option("--output", "output_path", required=True,
              type=click.Path(dir_okay=False),
              help="Path to write the summary CSV.")
def summarize(dataset_path: str, output_path: str) -> None:
    """Generate a column-level summary CSV via DuckDB over landed parquet."""
    from .summarize import generate_summary
    generate_summary(Path(dataset_path), Path(output_path))


@main.group()
def manifest() -> None:
    """Inspect and finalize fragment manifests."""


@manifest.command("finalize")
@click.argument("layer_root", type=click.Path(exists=True, file_okay=False))
def manifest_finalize(layer_root: str) -> None:
    """Merge ``<layer_root>/_fragments/*.json`` → ``manifest.json`` + ``schema_summary.json``."""
    merged = _manifest.finalize(layer_root)
    counts: dict[str, int] = {"ok": 0, "error": 0}
    for frag in merged.get("fragments", []):
        counts[frag.get("status", "error")] = counts.get(frag.get("status", "error"), 0) + 1
    click.echo(f"finalized {layer_root}\n  fragments merged: {merged.get('fragment_count', 0)}\n"
               f"  ok={counts.get('ok', 0)}  error={counts.get('error', 0)}")


@main.command()
@click.argument("config_yaml", type=click.Path(exists=True, dir_okay=False))
@click.option("--dry-run", is_flag=True,
              help="Render shards.json + sbatch script(s); do not invoke sbatch.")
@click.option("--allow-skip", is_flag=True,
              help="Skip files in source_glob not matching naming_pattern "
                   "(logged to manifest/_skipped/<ts>.json).")
@click.option("--force", is_flag=True,
              help="Re-submit every shard, bypassing idempotent-resume skip.")
@click.option("--chunk-size", type=int, default=None,
              help="Override Config.slurm_chunk_size for this submission.")
def submit(config_yaml: str, dry_run: bool, allow_skip: bool,
           force: bool, chunk_size: int | None) -> None:
    """Render sbatch script(s) and submit a Slurm array (Steps 9.2–9.5)."""
    from . import _sbatch, batcher
    from .duckdb_runner import _layer_root
    config = load_config(config_yaml)
    layer_root = _layer_root(config)
    matched, unmatched = batcher.expand_and_validate(config)
    _sbatch.handle_unmatched(unmatched, layer_root, allow_skip=allow_skip, dry_run=dry_run)
    if not matched:
        raise click.ClickException(f"No shards from source_glob={config.source_glob!r}.")
    to_submit, skipped_ok = _sbatch.filter_resumable_shards(matched, layer_root, force)
    if dry_run:
        click.echo(f"Would submit: {len(to_submit)} shards\n"
                   f"Would skip (existing ok): {len(skipped_ok)} shards")
    if not to_submit:
        click.echo("All shards already complete — nothing to submit. "
                   "Use --force to override.")
        return
    effective_size = chunk_size if chunk_size is not None else config.slurm_chunk_size
    chunks = _sbatch.chunk_shards(to_submit, effective_size)
    scripts = _sbatch.write_chunk_artifacts(config, chunks, Path(config_yaml))
    if dry_run:
        for script in scripts:
            click.echo(f"Would submit: sbatch {script}")
        return
    if shutil.which("sbatch") is None:
        click.echo("Error: sbatch not found on PATH. Run from a Slurm-enabled "
                   "host or pass --dry-run to render scripts only.", err=True)
        sys.exit(2)
    ts = _manifest.utc_now_iso()
    job_ids: list[str] = []
    for i, (script, chunk) in enumerate(zip(scripts, chunks)):
        proc = subprocess.run(["sbatch", str(script)], check=True,
                              capture_output=True, text=True)
        jid = _sbatch.parse_array_job_id(proc.stdout)
        _sbatch.write_submission_record(
            layer_root, sbatch_script=script, array_job_id=jid,
            n_shards=len(chunk), ts=ts, chunk_index=i, total_chunks=len(chunks))
        job_ids.append(jid)
    click.echo(_sbatch.format_submission_summary(
        len(to_submit), len(skipped_ok), len(chunks), job_ids))


if __name__ == "__main__":
    main()
