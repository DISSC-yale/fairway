"""Tests for Slurm submission gating + script rendering."""
from __future__ import annotations

from textwrap import dedent

import pytest

from fairway import _sbatch, manifest as _manifest
from fairway.config import resolve_config
from fairway.batcher import enumerate_shards
from fairway.schema import (
    SchemaError,
    load_schema,
    schema_fingerprint,
)


def test_filter_reads_manifest_not_fragments(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    schema = load_schema(cfg.table_dir)
    schema_fp = schema_fingerprint(schema)
    # Pre-populate manifest with valid leaves for one shard
    m = _manifest.empty_manifest("example")
    files = shards[0].leaves["state=CT/year=2023"]
    sources = [str(p) for p in files]
    hashes = [_manifest.source_hash(p) for p in files]
    _manifest.record_leaf(
        m, "processed", "state=CT/year=2023",
        source_files=sources, source_hashes=hashes,
        schema_fingerprint=schema_fp,
        row_count=2, status="ok",
    )
    _manifest.save_manifest(m, cfg.table_dir)
    # Drop a stale fragment that would be misleading if SoT logic peeked
    frag_dir = _manifest.fragment_dir(cfg.table_dir)
    frag_dir.mkdir(parents=True, exist_ok=True)
    (frag_dir / "stale.json").write_text("{}", encoding="utf-8")
    to_run, skipped = _sbatch.filter_resumable_shards(
        shards, cfg.table_dir, schema_fp, force=False,
    )
    assert {s.shard_id for s in skipped} == {"state=CT_year=2023"}
    assert {s.shard_id for s in to_run} == {"state=NY_year=2023"}


def test_force_includes_all(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    schema = load_schema(cfg.table_dir)
    schema_fp = schema_fingerprint(schema)
    to_run, skipped = _sbatch.filter_resumable_shards(
        shards, cfg.table_dir, schema_fp, force=True,
    )
    assert len(to_run) == len(shards) and skipped == []


def test_plan_submission_validates_schema_first(tmp_project, monkeypatch):
    """A fixed-width schema missing a `physical:` block must error before any submit."""
    table_dir = tmp_project / "tables" / "fw"
    table_dir.mkdir(parents=True)
    raw = tmp_project / "data" / "raw" / "fw"
    raw.mkdir(parents=True)
    (raw / "CT_2023.csv").write_text("xxx", encoding="utf-8")
    (table_dir / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/fw/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        source_format: fixed_width
    """), encoding="utf-8")
    (table_dir / "schema.yaml").write_text(dedent("""\
        on_drift: strict
        columns:
          - {name: id, type: INTEGER}
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="fw")
    schema = load_schema(cfg.table_dir)
    with pytest.raises(SchemaError, match="physical"):
        _sbatch.plan_submission(cfg, schema)


def test_render_array_script_substitutes_keys(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    body = _sbatch.render_array_script(cfg, n_shards=3,
                                       shards_file=tmp_project / "shards.json")
    assert "fairway-example" in body
    assert "--array=0-2" in body
    assert "fairway _shard example" in body


def test_render_finalize_script_runs_finalize(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    body = _sbatch.render_finalize_script(cfg)
    assert "fairway finalize example" in body
    assert "fairway-finalize-example" in body


def test_write_artifacts_emits_three_files(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    shards_file, array_script, finalize_script = _sbatch.write_artifacts(cfg, shards)
    for p in (shards_file, array_script, finalize_script):
        assert p.is_file()


def test_parse_array_job_id():
    assert _sbatch.parse_array_job_id("Submitted batch job 12345\n") == "12345"
