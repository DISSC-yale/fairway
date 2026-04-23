"""Canonical DuckDB end-to-end pipeline test.

This is the reference whole-pipeline test for local CI:
real config -> fairway run -> curated data + manifest + run metadata + logs.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.conftest import require_duckdb_for_pipeline
from tests.helpers import read_curated


def _write_duckdb_project(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (raw_dir / "employees.csv").write_text(
        "employee_id,name,team,salary\n"
        "1,Alice,platform,120000\n"
        "2,Bob,data,110000\n"
        "3,Carol,platform,130000\n"
    )
    config_path = config_dir / "fairway.yaml"
    config_path.write_text(
        "\n".join(
            [
                "project: full_pipeline_duckdb",
                "dataset_name: full_pipeline_duckdb",
                "engine: duckdb",
                "storage:",
                f"  root: {tmp_path / 'data'}",
                "tables:",
                "  - name: employees",
                f"    path: {raw_dir / 'employees.csv'}",
                "    format: csv",
                "    schema:",
                "      employee_id: BIGINT",
                "      name: VARCHAR",
                "      team: VARCHAR",
                "      salary: DOUBLE",
                "    validations:",
                "      check_nulls:",
                "        - employee_id",
            ]
        )
        + "\n"
    )
    return config_path


@pytest.mark.local
@pytest.mark.pipeline
def test_duckdb_cli_run_writes_all_pipeline_artifacts(tmp_path, monkeypatch):
    require_duckdb_for_pipeline()
    monkeypatch.chdir(tmp_path)
    config_path = _write_duckdb_project(tmp_path)

    from fairway.cli import main
    from fairway.config_loader import Config

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["run", "--config", str(config_path), "--skip-summary"],
    )
    assert result.exit_code == 0, result.output

    cfg = Config(str(config_path))
    curated = read_curated(tmp_path, "employees")
    assert len(curated) == 3
    assert set(curated["team"].tolist()) == {"platform", "data"}

    manifest_file = cfg.paths.manifest_dir / "employees.json"
    assert manifest_file.exists(), manifest_file
    manifest = json.loads(manifest_file.read_text())
    statuses = [entry["status"] for entry in manifest.get("files", {}).values()]
    assert statuses and all(status == "success" for status in statuses), manifest

    run_metadata = sorted(cfg.paths.log_dir.glob("*/run_*.json"))
    assert run_metadata, f"expected run metadata under {cfg.paths.log_dir}"
    run_payload = json.loads(run_metadata[-1].read_text())
    assert run_payload["project"] == "full_pipeline_duckdb"
    assert run_payload["exit_status"] == "success"

    run_logs = sorted(cfg.paths.log_dir.glob("*/run_*.jsonl"))
    assert run_logs, f"expected jsonl logs under {cfg.paths.log_dir}"
    log_text = run_logs[-1].read_text()
    assert "Starting ingestion for dataset" in log_text
