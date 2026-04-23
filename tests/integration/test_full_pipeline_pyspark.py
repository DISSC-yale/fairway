"""Canonical PySpark end-to-end pipeline test.

Runs the real Fairway pipeline against a local Spark master so CI can
exercise the Spark path without needing Slurm or an external cluster.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.conftest import require_pyspark_for_pipeline
from tests.helpers import read_curated


def _write_pyspark_project(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (raw_dir / "transactions.csv").write_text(
        "transaction_id,account_id,amount\n"
        "100,1,10.5\n"
        "101,1,15.0\n"
        "102,2,7.25\n"
    )
    config_path = config_dir / "fairway.yaml"
    config_path.write_text(
        "\n".join(
            [
                "project: full_pipeline_pyspark",
                "dataset_name: full_pipeline_pyspark",
                "engine: pyspark",
                "storage:",
                f"  root: {tmp_path / 'data'}",
                "tables:",
                "  - name: transactions",
                f"    path: {raw_dir / 'transactions.csv'}",
                "    format: csv",
                "    schema:",
                "      transaction_id: BIGINT",
                "      account_id: BIGINT",
                "      amount: DOUBLE",
                "    validations:",
                "      check_nulls:",
                "        - transaction_id",
            ]
        )
        + "\n"
    )
    return config_path


@pytest.mark.local
@pytest.mark.pipeline
@pytest.mark.spark
@pytest.mark.spark_pipeline
def test_pyspark_cli_run_writes_all_pipeline_artifacts(tmp_path, monkeypatch):
    require_pyspark_for_pipeline()
    monkeypatch.chdir(tmp_path)
    config_path = _write_pyspark_project(tmp_path)

    from fairway.cli import main
    from fairway.config_loader import Config

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "run",
            "--config",
            str(config_path),
            "--spark-master",
            "local[*]",
            "--skip-summary",
        ],
    )
    assert result.exit_code == 0, result.output

    cfg = Config(str(config_path))
    curated = read_curated(tmp_path, "transactions")
    assert len(curated) == 3
    assert sorted(round(v, 2) for v in curated["amount"].tolist()) == [7.25, 10.5, 15.0]

    manifest_file = cfg.paths.manifest_dir / "transactions.json"
    assert manifest_file.exists(), manifest_file
    manifest = json.loads(manifest_file.read_text())
    statuses = [entry["status"] for entry in manifest.get("files", {}).values()]
    assert statuses and all(status == "success" for status in statuses), manifest

    run_metadata = sorted(cfg.paths.log_dir.glob("*/run_*.json"))
    assert run_metadata, f"expected run metadata under {cfg.paths.log_dir}"
    run_payload = json.loads(run_metadata[-1].read_text())
    assert run_payload["project"] == "full_pipeline_pyspark"
    assert run_payload["exit_status"] == "success"

    run_logs = sorted(cfg.paths.log_dir.glob("*/run_*.jsonl"))
    assert run_logs, f"expected jsonl logs under {cfg.paths.log_dir}"
    log_text = run_logs[-1].read_text()
    assert "Starting ingestion for dataset" in log_text
