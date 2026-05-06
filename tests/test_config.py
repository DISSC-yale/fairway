"""Tests for project config walk-up + per-table TableConfig."""
from __future__ import annotations

from textwrap import dedent

import pytest

from fairway.config import (
    ConfigError,
    PROJECT_FILE,
    find_project_root,
    resolve_config,
)


def test_find_project_root_walks_up(tmp_project, monkeypatch):
    nested = tmp_project / "tables" / "x" / "deeper"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    assert find_project_root().resolve() == tmp_project.resolve()


def test_find_project_root_not_found(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="No `fairway.yaml` found"):
        find_project_root()


def test_apply_type_a_project_level_rejected(tmp_project, monkeypatch):
    fairway_yaml = tmp_project / PROJECT_FILE
    fairway_yaml.write_text(fairway_yaml.read_text() + "\napply_type_a: true\n",
                            encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
    """), encoding="utf-8")
    with pytest.raises(ConfigError, match="apply_type_a"):
        resolve_config(table="x")


def test_apply_type_a_table_level_works(tmp_project, monkeypatch, capsys):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        apply_type_a: true
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    assert cfg.apply_type_a is True
    out = capsys.readouterr().out
    assert "Project root:" in out


def test_storage_raw_removed(tmp_project, monkeypatch):
    """`storage_raw` is no longer accepted in either project or table config."""
    fairway_yaml = tmp_project / PROJECT_FILE
    fairway_yaml.write_text(fairway_yaml.read_text() + "\nstorage_raw: data/raw\n",
                            encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    with pytest.raises(ConfigError, match="storage_raw"):
        from fairway.config import load_project_config
        load_project_config(tmp_project)


def test_shard_by_default_is_partition_by(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    assert cfg.shard_by == ["state", "year"]


def test_shard_by_empty_is_valid(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        shard_by: []
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    assert cfg.shard_by == []


def test_shard_by_null_coerced_to_empty(tmp_project, monkeypatch):
    """Bare `shard_by:` loads as None; resolve_config coerces to []."""
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        shard_by: ~
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    assert cfg.shard_by == []


def test_table_missing_raises_clear_error(tmp_project, monkeypatch):
    monkeypatch.chdir(tmp_project)
    with pytest.raises(ConfigError, match="Table 'nope' not found"):
        resolve_config(table="nope")


def test_source_glob_resolved_relative_to_project_root(tmp_project, monkeypatch):
    """Regression: walk-up discovery is the headline UX of v0.3, but if
    source_glob is stored CWD-relative, running from any subdirectory finds
    zero files and the pipeline silently no-ops. source_glob must be
    resolved against project_root at config load time.
    """
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
    """), encoding="utf-8")
    raw = tmp_project / "data" / "raw" / "x"
    raw.mkdir(parents=True)
    (raw / "CT_2023.csv").write_text("id\n1\n", encoding="utf-8")
    # Invoke from a deeply-nested subdirectory — walk-up still finds the project.
    sub = tmp_project / "tables" / "x"
    monkeypatch.chdir(sub)
    cfg = resolve_config(table="x")
    import glob
    matched = glob.glob(cfg.source_glob)
    assert len(matched) == 1, (
        f"source_glob {cfg.source_glob!r} from CWD {sub} should match 1 file, "
        f"got {matched}"
    )


def test_source_glob_absolute_path_passthrough(tmp_project, monkeypatch):
    """Absolute source_glob must be left intact (no project_root prefix)."""
    (tmp_project / "tables" / "x").mkdir(parents=True)
    abs_glob = str(tmp_project / "data" / "raw" / "x" / "*.csv")
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent(f"""\
        source_glob: {abs_glob}
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{{4}})\\.csv'
        partition_by: [state, year]
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    cfg = resolve_config(table="x")
    assert cfg.source_glob == abs_glob


def test_naming_pattern_groups_must_match_partition_by(tmp_project, monkeypatch):
    (tmp_project / "tables" / "x").mkdir(parents=True)
    (tmp_project / "tables" / "x" / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/x/*.csv
        naming_pattern: '(?P<state>[A-Z]+)\\.csv'
        partition_by: [state, year]
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    with pytest.raises(ConfigError, match="must equal"):
        resolve_config(table="x")
