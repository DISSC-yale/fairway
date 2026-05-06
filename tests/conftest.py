"""Shared pytest configuration for fairway tests (v0.3 redesign)."""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from textwrap import dedent

import pytest


_repo_root_baseline: "set[str] | None" = None


def pytest_configure(config):
    global _repo_root_baseline
    repo_root = Path(__file__).parent.parent
    (repo_root / "build" / "coverage").mkdir(parents=True, exist_ok=True)
    try:
        _repo_root_baseline = set(os.listdir(repo_root))
    except OSError:
        _repo_root_baseline = None


def pytest_sessionfinish(session, exitstatus):
    repo_root = Path(__file__).parent.parent
    basetemp = repo_root / "build" / "test-tmp"
    if basetemp.exists():
        shutil.rmtree(basetemp, ignore_errors=True)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if _repo_root_baseline is None:
        return
    repo_root = Path(__file__).parent.parent
    ignored = {
        "build", ".pytest_cache", ".ruff_cache", ".venv",
        "__pycache__", ".reports", "htmlcov",
        "coverage.xml", "coverage.json",
    }
    try:
        after = set(os.listdir(repo_root))
    except OSError:
        return
    leaked = sorted((after - _repo_root_baseline) - ignored)
    if leaked:
        terminalreporter.write_line(
            f"TEST LEAK DETECTED — new repo-root entries after session: {leaked}",
            red=True,
        )


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """A minimal v0.3 project: fairway.yaml + tables/example/ scaffolded."""
    (tmp_path / "fairway.yaml").write_text(dedent("""\
        storage_root: data
        storage_processed: data/processed
        storage_curated: data/curated
        scratch_dir: null
        encoding: utf-8
        encoding_fallback: latin-1
        allow_encoding_fallback: false
        row_group_size: 1024
        slurm_account: null
        slurm_partition: null
        slurm_chunk_size: 4000
        slurm_concurrency: 64
        slurm_mem: 4G
        slurm_cpus_per_task: 1
        slurm_time: "0:30:00"
    """), encoding="utf-8")
    (tmp_path / "tables").mkdir()
    return tmp_path


@pytest.fixture
def example_table(tmp_project: Path) -> Path:
    """A `tables/example/` dir with config + schema + sample CSVs."""
    table_dir = tmp_project / "tables" / "example"
    table_dir.mkdir()
    raw = tmp_project / "data" / "raw" / "example"
    raw.mkdir(parents=True)
    (raw / "CT_2023.csv").write_text(
        "id,amount\n1,10.5\n2,20.5\n", encoding="utf-8")
    (raw / "NY_2023.csv").write_text(
        "id,amount\n3,30.5\n4,40.5\n", encoding="utf-8")
    (table_dir / "config.yaml").write_text(dedent("""\
        source_glob: data/raw/example/*.csv
        naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{4})\\.csv'
        partition_by: [state, year]
        source_format: delimited
        delimiter: ","
        has_header: true
        validations:
          min_rows: 0
    """), encoding="utf-8")
    (table_dir / "schema.yaml").write_text(dedent("""\
        on_drift: strict
        columns:
          - {name: id, type: INTEGER}
          - {name: amount, type: DOUBLE}
    """), encoding="utf-8")
    return table_dir
