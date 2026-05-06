"""Tests for the orchestrator: shard_by validation, run_inline, finalize."""
from __future__ import annotations

from textwrap import dedent

import pytest

from fairway.batcher import enumerate_shards, validate_shard_by
from fairway.config import ConfigError, resolve_config
from fairway.manifest import load_manifest, fragment_count
from fairway.pipeline import run_inline
from fairway.schema import load_schema


def test_validate_shard_by_not_prefix(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        shard_by: [year]
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    with pytest.raises(ConfigError, match="must be a strict prefix"):
        validate_shard_by(cfg)


def test_validate_shard_by_empty_ok(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        shard_by: []
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    validate_shard_by(cfg)


def test_validate_shard_by_full_prefix_ok(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        shard_by: [state, year]
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    validate_shard_by(cfg)


def test_enumerate_shards_partition_grouping(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    shards, unmatched = enumerate_shards(cfg)
    assert unmatched == []
    leaves = {leaf for s in shards for leaf in s.leaves}
    assert leaves == {"state=CT/year=2023", "state=NY/year=2023"}


def test_enumerate_shards_with_shard_by_state(example_table, tmp_project, monkeypatch):
    cfg_yaml = example_table / "config.yaml"
    cfg_yaml.write_text(cfg_yaml.read_text() + "shard_by: [state]\n", encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    assert len(shards) == 2
    sid_set = {s.shard_id for s in shards}
    assert sid_set == {"state=CT", "state=NY"}


def test_run_inline_writes_parquet_and_finalizes(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    schema = load_schema(cfg.table_dir)
    result = run_inline(cfg, schema)
    assert result.total_shards == 2 and result.submitted == 2
    parts = (tmp_project / "data" / "processed" / "example").rglob("*.parquet")
    assert any(parts)
    m = load_manifest(cfg.table_dir)
    parts_dict = m["layers"]["processed"]["partitions"]
    assert set(parts_dict) == {"state=CT/year=2023", "state=NY/year=2023"}
    assert fragment_count(cfg.table_dir) == 0


def test_run_inline_skips_up_to_date(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    schema = load_schema(cfg.table_dir)
    run_inline(cfg, schema)
    second = run_inline(cfg, schema)
    assert second.skipped == 2 and second.submitted == 0


def test_run_inline_force_reruns_all(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    schema = load_schema(cfg.table_dir)
    run_inline(cfg, schema)
    forced = run_inline(cfg, schema, force=True)
    assert forced.submitted == forced.total_shards


def test_run_inline_clears_last_run_first(example_table, tmp_project, monkeypatch):
    """A failed prior run's failed_shards must not bleed into a clean re-run."""
    from fairway.manifest import (
        empty_manifest, save_manifest, set_last_run,
    )
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    schema = load_schema(cfg.table_dir)
    stale = empty_manifest("example")
    set_last_run(stale, "processed",
                 shard_by=["state", "year"],
                 shards_observed=["old"],
                 failed_shards=[{"shard_id": "old", "error_message": "stale"}])
    save_manifest(stale, cfg.table_dir)
    run_inline(cfg, schema)
    m = load_manifest(cfg.table_dir)
    failed = m["layers"]["processed"]["last_run"]["failed_shards"]
    assert all(f.get("error_message") != "stale" for f in failed)
