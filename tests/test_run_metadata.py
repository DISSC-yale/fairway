"""Phase 2b — run_id binding + run.json finalization.

Pins the invariants that the FAIRWAY_HOME plan Phase 2b makes: each
IngestionPipeline run produces exactly one run.json under the project's
log_dir, atomically, with terminal exit_status on clean termination.
"""
import json
import os

import pytest
import yaml


pytest.importorskip("duckdb")


def _write_config(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    (raw_dir / "people.csv").write_text(
        "id,name,age\n1,alice,30\n2,bob,25\n3,carol,35\n"
    )
    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.safe_dump({
        "project": "phase2b_project",
        "dataset_name": "phase2b_project",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [{
            "name": "people",
            "path": str(raw_dir / "people.csv"),
            "format": "csv",
            "schema": {"id": "BIGINT", "name": "VARCHAR", "age": "INTEGER"},
        }],
    }))
    monkeypatch.chdir(tmp_path)
    return config_path


def test_run_id_bound_to_pipeline(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))
    assert pipeline.run_id
    assert pipeline.config.paths.run_id == pipeline.run_id


def test_run_id_honors_fairway_run_id_env(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    monkeypatch.setenv("FAIRWAY_RUN_ID", "manual-abc")
    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))
    assert pipeline.run_id == "manual-abc"


def test_run_json_written_at_start(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))
    meta_path = pipeline.config.paths.run_metadata_file_for(pipeline.run_id)
    assert meta_path.exists()
    data = json.loads(meta_path.read_text())
    assert data["exit_status"] == "running"
    assert data["run_id"] == pipeline.run_id
    assert data["project"] == "phase2b_project"


def test_run_json_finalized_on_success(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))
    pipeline.run(skip_summary=True)
    data = json.loads(
        pipeline.config.paths.run_metadata_file_for(pipeline.run_id).read_text()
    )
    assert data["exit_status"] == "success"
    assert "finished_at" in data


def test_run_json_finalized_on_failure(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))

    def _boom(*a, **k):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(pipeline, "_run", _boom)
    with pytest.raises(RuntimeError, match="injected failure"):
        pipeline.run()
    data = json.loads(
        pipeline.config.paths.run_metadata_file_for(pipeline.run_id).read_text()
    )
    assert data["exit_status"] == "failed"


def test_run_abandon_only_updates_running(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from click.testing import CliRunner
    from fairway.cli import main
    from fairway.pipeline import IngestionPipeline

    pipeline = IngestionPipeline(str(config_path))
    pipeline.run(skip_summary=True)
    meta_path = pipeline.config.paths.run_metadata_file_for(pipeline.run_id)
    before = json.loads(meta_path.read_text())
    assert before["exit_status"] == "success"

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["run-abandon", "--config", str(config_path), pipeline.run_id],
    )
    assert result.exit_code == 0, result.output
    after = json.loads(meta_path.read_text())
    # Must not overwrite terminal status.
    assert after["exit_status"] == "success"


def test_run_abandon_marks_running_as_unfinished(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path, monkeypatch)
    from click.testing import CliRunner
    from fairway.cli import main
    from fairway.pipeline import IngestionPipeline

    pipeline = IngestionPipeline(str(config_path))
    # Skip run() entirely — the run.json is still "running" from __init__.
    meta_path = pipeline.config.paths.run_metadata_file_for(pipeline.run_id)
    assert json.loads(meta_path.read_text())["exit_status"] == "running"

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["run-abandon", "--config", str(config_path), pipeline.run_id],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(meta_path.read_text())["exit_status"] == "unfinished"
