"""fairway CLI — v0.3 redesign (tables/ layout + manifest as SoT).

Top-level commands:

  init <proj> [--table <name>]        scaffold project / add a table
  discover <table>                    populate tables/<t>/schema.yaml
  submit <table> [--force]            Slurm array + afterany finalize dep
  run <table> [--force]               single-machine inline finalize
  finalize <table>                    merge fragments → manifest
  status <table>                      manifest + fragment-dir status
  validate <table>                    rules from config.yaml
  _shard <table> --shards-file F --shard-index N    internal worker
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import click

from . import _init_templates as _t
from . import _sbatch
from . import manifest as _manifest
from .config import (
    ConfigError,
    PROJECT_FILE,
    resolve_config,
)
from .batcher import Shard
from .pipeline import run_inline, run_shard_direct
from .schema import SchemaError, load_schema


logger = logging.getLogger(__name__)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging.")
def main(verbose: bool) -> None:
    """fairway — partitioned-parquet ingest for HPC."""
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(name)s %(levelname)s %(message)s",
        )


# ── init ────────────────────────────────────────────────────────────


def _scaffold_project(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / PROJECT_FILE).write_text(_t.FAIRWAY_YAML_TEMPLATE, encoding="utf-8")
    (root / ".gitignore").write_text(_t.GITIGNORE_TEMPLATE, encoding="utf-8")
    (root / "README.md").write_text(
        _t.README_TEMPLATE.format(name=root.name), encoding="utf-8")
    (root / "tables").mkdir(parents=True, exist_ok=True)


def _scaffold_table(root: Path, name: str) -> None:
    table_dir = root / "tables" / name
    table_dir.mkdir(parents=True, exist_ok=True)
    (table_dir / "config.yaml").write_text(
        _t.TABLE_CONFIG_YAML_TEMPLATE.format(name=name), encoding="utf-8")
    (table_dir / "schema.yaml").write_text(
        _t.TABLE_SCHEMA_YAML_STUB.format(name=name), encoding="utf-8")
    (table_dir / "transform.py").write_text(
        _t.TABLE_TRANSFORM_PY_TEMPLATE.format(name=name), encoding="utf-8")


@main.command()
@click.argument("project_dir", type=click.Path(file_okay=False))
@click.option("--table", "table_name", default=None,
              help="Add a tables/<name>/ scaffold (existing project only).")
def init(project_dir: str, table_name: str | None) -> None:
    """Scaffold a fairway project (and/or a per-table directory)."""
    root = Path(project_dir).resolve()
    project_exists = (root / PROJECT_FILE).is_file()
    if not project_exists:
        if root.exists() and any(root.iterdir()):
            raise click.ClickException(
                f"Path {project_dir!r} is non-empty and missing {PROJECT_FILE}; "
                f"point --table at an empty dir or run `fairway init <new>`."
            )
        _scaffold_project(root)
        if table_name is None:
            _scaffold_table(root, "example")
    if table_name:
        _scaffold_table(root, table_name)
    click.echo(f"Initialized {root}")


# ── discover ─────────────────────────────────────────────────────────


@main.command()
@click.argument("table")
@click.option("--sample-files", default=50, show_default=True, type=int)
@click.option("--rows-per-file", default=1000, show_default=True, type=int)
@click.option("--force", is_flag=True, help="Overwrite an existing schema.yaml.")
def discover(table: str, sample_files: int, rows_per_file: int, force: bool) -> None:
    """Two-phase header union + type inference → schema.yaml."""
    from .discover import DiscoverError, discover as _discover
    try:
        config = resolve_config(table=table)
        path = _discover(config, sample_files=sample_files,
                         rows_per_file=rows_per_file, force=force)
    except (ConfigError, DiscoverError, SchemaError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Wrote {path}")


# ── run (single-machine) ─────────────────────────────────────────────


@main.command()
@click.argument("table")
@click.option("--force", is_flag=True, help="Re-run every shard, ignoring manifest.")
def run(table: str, force: bool) -> None:
    """Single-machine ingest with inline finalize after the last shard."""
    try:
        config = resolve_config(table=table)
        schema = load_schema(config.table_dir)
        result = run_inline(config, schema, force=force)
    except (ConfigError, SchemaError) as exc:
        raise click.ClickException(str(exc)) from exc
    ok = sum(1 for r in result.shard_results if not r.failed)
    err = result.submitted - ok
    click.echo(
        f"Done: total_shards={result.total_shards} submitted={result.submitted} "
        f"skipped={result.skipped} ok={ok} error={err}"
    )


# ── submit (Slurm) ───────────────────────────────────────────────────


@main.command()
@click.argument("table")
@click.option("--force", is_flag=True, help="Re-submit every shard, ignoring manifest.")
@click.option("--dry-run", is_flag=True,
              help="Render artifacts; do not invoke sbatch.")
def submit(table: str, force: bool, dry_run: bool) -> None:
    """Slurm array submit + afterany finalize dependency."""
    try:
        config = resolve_config(table=table)
        schema = load_schema(config.table_dir)
        to_run, skipped, unmatched = _sbatch.plan_submission(
            config, schema, force=force,
        )
    except (ConfigError, SchemaError) as exc:
        raise click.ClickException(str(exc)) from exc
    _sbatch.handle_unmatched(unmatched)

    if not to_run:
        click.echo(
            f"All {len(skipped)} shard(s) up-to-date — nothing to submit. "
            f"Use --force to re-run."
        )
        return

    shards_file, array_script, finalize_script = _sbatch.write_artifacts(
        config, to_run,
    )
    click.echo(f"Wrote {shards_file}")
    click.echo(f"Wrote {array_script}")
    click.echo(f"Wrote {finalize_script}")
    if dry_run:
        click.echo(f"Would submit: sbatch {array_script}")
        click.echo(f"Would submit: sbatch --dependency=afterany:<JID> {finalize_script}")
        return
    if shutil.which("sbatch") is None:
        click.echo(
            "Error: sbatch not found on PATH. Run from a Slurm-enabled host "
            "or pass --dry-run.", err=True,
        )
        sys.exit(2)
    manifest = _manifest.load_manifest(config.table_dir)
    _manifest.clear_last_run(manifest)
    _manifest.save_manifest(manifest, config.table_dir)
    array_jid = _sbatch.run_sbatch(array_script)
    finalize_jid = _sbatch.run_sbatch_dependency(finalize_script, array_jid)
    click.echo(_sbatch.format_submission_summary(
        len(to_run), len(skipped), array_jid, finalize_jid,
    ))


# ── _shard (internal worker) ─────────────────────────────────────────


@main.command(name="_shard")
@click.argument("table")
@click.option("--shards-file", required=True,
              type=click.Path(exists=True, dir_okay=False))
@click.option("--shard-index", required=True, type=int)
def _shard(table: str, shards_file: str, shard_index: int) -> None:
    """Internal worker — invoked by the sbatch array script."""
    payload = json.loads(Path(shards_file).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "shards" not in payload:
        raise click.ClickException(
            f"shards file {shards_file!r} must contain a `shards` array"
        )
    shards_data = payload["shards"]
    if shard_index < 0 or shard_index >= len(shards_data):
        raise click.ClickException(
            f"shard_index {shard_index} out of range (0..{len(shards_data) - 1})"
        )
    entry = shards_data[shard_index]
    shard = Shard(
        shard_id=str(entry["shard_id"]),
        shard_values=dict(entry.get("shard_values") or {}),
        leaves={
            leaf: [Path(p) for p in files]
            for leaf, files in (entry.get("leaves") or {}).items()
        },
    )
    project_root = Path(payload.get("project_root", ".")).resolve()
    try:
        config = resolve_config(project_root=project_root, table=table)
        schema = load_schema(config.table_dir)
        result = run_shard_direct(config, schema, shard)
    except (ConfigError, SchemaError, IndexError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"Shard {result.shard_id}: ok={len(result.leaves_ok)} "
        f"error={len(result.leaves_error)}"
    )


# ── finalize ─────────────────────────────────────────────────────────


@main.command()
@click.argument("table")
def finalize(table: str) -> None:
    """Merge ``_state/fragments/*.json`` → ``manifest.json``."""
    try:
        config = resolve_config(table=table)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    fragment_count = _manifest.fragment_count(config.table_dir)
    _manifest.finalize(
        config.table_dir,
        layer="processed",
        shard_by=list(config.shard_by),
    )
    click.echo(f"Finalized {config.table_dir} (merged {fragment_count} fragment(s))")


# ── status ───────────────────────────────────────────────────────────


def _format_status(config_table: Any, manifest: dict[str, Any], frag_count: int) -> list[str]:
    layer = (manifest.get("layers", {}) or {}).get("processed", {})
    parts = layer.get("partitions", {})
    ok = sum(1 for p in parts.values() if p.get("status") == "ok")
    err = sum(1 for p in parts.values() if p.get("status") == "error")
    last_run = layer.get("last_run") or {}
    failed = last_run.get("failed_shards") or []
    shard_by = (last_run.get("shard_grouping") or {}).get("shard_by", [])
    lines = [
        f"Table:      {config_table.table}",
        f"Layer:      processed",
        f"Partitions: {ok} ok, {err} error",
        f"Last run:   {manifest.get('finalized_at') or '(none)'} "
        f"(shard_by: {shard_by})",
        f"Failed shards: {len(failed) if failed else 'none'}",
        "",
        f"Fragment dir: {frag_count} unmerged fragment(s)",
    ]
    if frag_count:
        lines.append(
            f"  → finalize pending or failed. Re-run "
            f"`fairway finalize {config_table.table}` if the dependency job has finished."
        )
    return lines


@main.command()
@click.argument("table")
def status(table: str) -> None:
    """Print manifest + fragment status."""
    try:
        config = resolve_config(table=table)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    manifest = _manifest.load_manifest(config.table_dir)
    frag_count = _manifest.fragment_count(config.table_dir)
    for line in _format_status(config, manifest, frag_count):
        click.echo(line)


# ── validate ─────────────────────────────────────────────────────────


@main.command()
@click.argument("table")
def validate(table: str) -> None:
    """Apply ``validations:`` rules to landed parquet under storage_processed/<t>/."""
    import duckdb
    from .config import _parse_validations
    from .validations import ShardValidationError, apply_validations
    try:
        config = resolve_config(table=table)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    layer_root = Path(config.storage_processed) / config.table
    if not layer_root.is_dir():
        raise click.ClickException(f"No data at {layer_root}")
    glob = str(layer_root).replace("'", "''") + "/**/*.parquet"
    con = duckdb.connect(":memory:")
    try:
        rel = con.sql(f"SELECT * FROM read_parquet('{glob}')")
        try:
            apply_validations(rel, config.validations)
        except ShardValidationError as exc:
            click.echo(f"FAIL  {exc}", err=True)
            sys.exit(2)
    finally:
        con.close()
    click.echo(f"OK    all checks passed: {layer_root}")


if __name__ == "__main__":
    main()
