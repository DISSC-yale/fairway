"""Tests for `fairway discover` two-phase schema inference."""
from __future__ import annotations

from textwrap import dedent

import pytest
import yaml

from fairway.config import resolve_config
from fairway.discover import DiscoverError, discover


def _make_table(tmp_project, name: str, csvs: dict[str, str]) -> None:
    table_dir = tmp_project / "tables" / name
    table_dir.mkdir(parents=True)
    raw = tmp_project / "data" / "raw" / name
    raw.mkdir(parents=True)
    for filename, body in csvs.items():
        (raw / filename).write_text(body, encoding="utf-8")
    (table_dir / "config.yaml").write_text(dedent(f"""\
        source_glob: data/raw/{name}/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{{4}})\\.csv'
        partition_by: [state, year]
        source_format: delimited
        delimiter: ","
        has_header: true
    """), encoding="utf-8")


def test_discover_union_headers(tmp_project, monkeypatch, capsys):
    _make_table(tmp_project, "sales", {
        "CT_2023.csv": "id,amount\n1,10\n",
        "NY_2023.csv": "id,amount,note\n2,20,foo\n",
        "MA_2023.csv": "id,amount,extra\n3,30,bar\n",
    })
    monkeypatch.chdir(tmp_project)
    config = resolve_config(table="sales")
    capsys.readouterr()
    path = discover(config)
    written = yaml.safe_load(path.read_text(encoding="utf-8"))
    names = [c["name"] for c in written["columns"]]
    assert set(names) == {"id", "amount", "note", "extra"}


def test_discover_fixed_width_refused(tmp_project, monkeypatch):
    table_dir = tmp_project / "tables" / "fw"
    table_dir.mkdir(parents=True)
    raw = tmp_project / "data" / "raw" / "fw"
    raw.mkdir(parents=True)
    (raw / "CT_2023.csv").write_text("garbage", encoding="utf-8")
    (table_dir / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/fw/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        source_format: fixed_width
    """), encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    config = resolve_config(table="fw")
    with pytest.raises(DiscoverError, match="Fixed-width schemas must be authored"):
        discover(config)


def test_discover_no_force_existing_nonempty(tmp_project, monkeypatch):
    _make_table(tmp_project, "sales", {"CT_2023.csv": "id\n1\n"})
    schema = tmp_project / "tables" / "sales" / "schema.yaml"
    schema.write_text("on_drift: strict\ncolumns:\n  - {name: x, type: INTEGER}\n",
                      encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    config = resolve_config(table="sales")
    with pytest.raises(DiscoverError, match="--force"):
        discover(config)


def test_discover_force_overwrites(tmp_project, monkeypatch):
    _make_table(tmp_project, "sales", {"CT_2023.csv": "id,amount\n1,10\n"})
    schema = tmp_project / "tables" / "sales" / "schema.yaml"
    schema.write_text("on_drift: strict\ncolumns:\n  - {name: x, type: INTEGER}\n",
                      encoding="utf-8")
    monkeypatch.chdir(tmp_project)
    config = resolve_config(table="sales")
    discover(config, force=True)
    written = yaml.safe_load(schema.read_text(encoding="utf-8"))
    names = [c["name"] for c in written["columns"]]
    assert "id" in names and "amount" in names


def test_discover_no_files_matched(tmp_project, monkeypatch):
    _make_table(tmp_project, "sales", {})
    monkeypatch.chdir(tmp_project)
    config = resolve_config(table="sales")
    with pytest.raises(DiscoverError, match="No files matched"):
        discover(config)
