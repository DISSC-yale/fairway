"""CLI surface tests — init / discover / run / finalize / status / _shard."""
from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from fairway.cli import main


def _run(args, *, cwd: Path | None = None):
    runner = CliRunner()
    if cwd:
        with runner.isolated_filesystem():
            import os
            os.chdir(cwd)
            return runner.invoke(main, args, catch_exceptions=False)
    return runner.invoke(main, args, catch_exceptions=False)


def test_init_no_eager_dirs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(main, ["init", "myproj"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert (tmp_path / "myproj" / "fairway.yaml").is_file()
    assert (tmp_path / "myproj" / "tables" / "example" / "config.yaml").is_file()
    assert (tmp_path / "myproj" / "tables" / "example" / "schema.yaml").is_file()
    assert not (tmp_path / "myproj" / "data").is_dir()
    assert not (tmp_path / "myproj" / "build").is_dir()
    assert not (tmp_path / "myproj" / "transforms").is_dir()


def test_init_table_flag_adds_only_table(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    runner.invoke(main, ["init", "myproj"], catch_exceptions=False)
    res = runner.invoke(main, ["init", "myproj", "--table", "sales"],
                        catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert (tmp_path / "myproj" / "tables" / "sales" / "config.yaml").is_file()
    # Existing example dir unchanged
    assert (tmp_path / "myproj" / "tables" / "example").is_dir()


def test_dataset_flag_removed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    res = runner.invoke(main, ["submit", "--dataset", "x"])
    assert res.exit_code != 0
    assert "no such option" in res.output.lower() or "unexpected" in res.output.lower()


def test_removed_commands_rejected(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    for cmd in ("transform", "enrich", "summarize"):
        res = runner.invoke(main, [cmd, "anything"])
        assert res.exit_code != 0
        assert "no such command" in res.output.lower() or "usage" in res.output.lower()


def test_run_command_executes(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    res = CliRunner().invoke(main, ["run", "example"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert "Done" in res.output
    assert (tmp_project / "tables" / "example" / "manifest.json").is_file()


def test_finalize_command_exists(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    res = CliRunner().invoke(main, ["finalize", "example"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert "Finalized" in res.output


def test_status_clean_state(example_table, tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    runner = CliRunner()
    runner.invoke(main, ["run", "example"], catch_exceptions=False)
    res = runner.invoke(main, ["status", "example"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    assert "Partitions: 2 ok" in res.output
    assert "0 unmerged fragment" in res.output


def test_status_fragment_hint(example_table, tmp_project, monkeypatch):
    from fairway.manifest import fragment_dir, write_fragment
    monkeypatch.chdir(tmp_project)
    fdir = fragment_dir(tmp_project / "tables" / "example")
    write_fragment(tmp_project / "tables" / "example", "leftover",
                   layer="processed", leaves={}, failed=None,
                   shard_by=["state", "year"])
    res = CliRunner().invoke(main, ["status", "example"], catch_exceptions=False)
    assert res.exit_code == 0
    assert "1 unmerged fragment" in res.output
    assert "finalize pending" in res.output


def test_shard_command_internal(example_table, tmp_project, monkeypatch):
    """`fairway _shard` accepts a shards-file payload + index and runs one shard."""
    monkeypatch.chdir(tmp_project)
    from fairway.batcher import enumerate_shards
    from fairway.config import resolve_config
    from fairway._sbatch import write_artifacts
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    shards_file, _, _ = write_artifacts(cfg, shards)
    res = CliRunner().invoke(
        main, ["_shard", "example", "--shards-file", str(shards_file),
               "--shard-index", "0"], catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output


def test_shard_uses_payload_not_reenumeration(example_table, tmp_project, monkeypatch):
    """Regression for the silent-data-loss bug: when shards.json holds the
    filtered (to_run) subset, the worker MUST process payload[shard_index],
    not enumerate_shards()[shard_index] — otherwise `to_run[k]` and
    `all_shards[k]` diverge once any shard is skipped, and a worker writes
    the wrong shard's leaves.
    """
    from fairway._sbatch import write_artifacts
    from fairway.batcher import enumerate_shards
    from fairway.config import resolve_config
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="example")
    all_shards, _ = enumerate_shards(cfg)
    # all_shards is sorted: [state=CT_year=2023, state=NY_year=2023].
    # Simulate "CT already done, only NY needs running" — write a shards.json
    # holding ONLY the second shard.
    only_ny = [all_shards[1]]
    shards_file, _, _ = write_artifacts(cfg, only_ny)
    res = CliRunner().invoke(
        main, ["_shard", "example", "--shards-file", str(shards_file),
               "--shard-index", "0"], catch_exceptions=False,
    )
    assert res.exit_code == 0, res.output
    # Output must reference NY's shard_id, not CT's.
    assert "state=NY_year=2023" in res.output
    assert "state=CT_year=2023" not in res.output
    # Parquet must have been written under NY's leaf, not CT's.
    processed = tmp_project / "data" / "processed" / "example"
    assert (processed / "state=NY/year=2023/data.parquet").is_file()
    assert not (processed / "state=CT/year=2023/data.parquet").is_file()


def test_shard_index_out_of_range_clean_error(example_table, tmp_project, monkeypatch):
    """`fairway _shard` index past payload length surfaces a clean ClickException,
    not a Python traceback through Slurm logs.
    """
    monkeypatch.chdir(tmp_project)
    from fairway._sbatch import write_artifacts
    from fairway.batcher import enumerate_shards
    from fairway.config import resolve_config
    cfg = resolve_config(table="example")
    shards, _ = enumerate_shards(cfg)
    shards_file, _, _ = write_artifacts(cfg, shards)
    res = CliRunner().invoke(
        main, ["_shard", "example", "--shards-file", str(shards_file),
               "--shard-index", "999"], catch_exceptions=False,
    )
    assert res.exit_code != 0
    assert "out of range" in res.output.lower()


def test_discover_command_exists(tmp_project, monkeypatch):
    table_dir = tmp_project / "tables" / "sales"
    table_dir.mkdir()
    raw = tmp_project / "data" / "raw" / "sales"
    raw.mkdir(parents=True)
    (raw / "CT_2023.csv").write_text("id,amount\n1,10\n", encoding="utf-8")
    (table_dir / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/sales/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        source_format: delimited
        delimiter: ","
        has_header: true
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    res = CliRunner().invoke(main, ["discover", "sales"], catch_exceptions=False)
    assert res.exit_code == 0, res.output
    schema = (table_dir / "schema.yaml").read_text()
    assert "id" in schema and "amount" in schema


def test_help_shows_no_legacy_commands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(main, ["--help"], catch_exceptions=False)
    out = res.output
    for legacy in ("transform", "enrich", "summarize", "manifest"):
        # Each must not appear as a top-level subcommand line:
        assert f"  {legacy} " not in out


def test_help_lists_new_commands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(main, ["--help"], catch_exceptions=False)
    for cmd in ("init", "discover", "run", "submit", "finalize", "status",
                "validate"):
        assert cmd in res.output
