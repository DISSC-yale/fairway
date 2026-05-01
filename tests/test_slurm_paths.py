"""Phase 2d — Slurm coordination files live under PathResolver state.

Pins:
- `SlurmSparkManager` accepts `spark_coordination_dir` override from
  the spark_cfg dict and routes state files there instead of `~/`.
- Without override, falls back to legacy `~/.fairway-spark/` paths
  (preserves pre-refactor behavior and existing tests).
- `SlurmManager.submit_job` routes `#SBATCH --output` via
  `PathResolver.slurm_log_dir` rather than the CWD-relative `logs/`.
"""
import os
from pathlib import Path
from unittest import mock

import pytest
import yaml

pytestmark = pytest.mark.hpc_contract


def test_spark_coord_override_routes_state_files(tmp_path):
    pytest.skip("slurm_cluster removed in v0.3 Step 1; full file deleted in Step 4")
    coord = tmp_path / "state" / "spark"
    mgr = SlurmSparkManager(
        {"spark_coordination_dir": str(coord)},
        driver_job_id="12345",
    )
    for path_attr in ("master_url_file", "job_id_file", "conf_dir_file", "cores_file"):
        p = Path(getattr(mgr, path_attr))
        assert str(coord) in str(p), f"{path_attr} escaped coord dir: {p}"
        assert "12345" in str(p), f"{path_attr} missing driver_job_id: {p}"


def test_spark_coord_override_without_driver_id(tmp_path):
    pytest.skip("slurm_cluster removed in v0.3 Step 1; full file deleted in Step 4")
    coord = tmp_path / "state" / "spark"
    mgr = SlurmSparkManager({"spark_coordination_dir": str(coord)})
    # Without driver_job_id, files live at coord root (no subdir), so
    # concurrent clusters without driver_job_id still collide — but at
    # least they're under project state, not `~/`.
    assert str(coord) in mgr.master_url_file
    assert "~" not in mgr.master_url_file


def test_spark_coord_missing_falls_back_to_home(tmp_path):
    """No spark_coordination_dir in config → legacy ~/ path, unchanged."""
    pytest.skip("slurm_cluster removed in v0.3 Step 1; full file deleted in Step 4")
    mgr = SlurmSparkManager({}, driver_job_id="abc")
    assert ".fairway-spark/abc" in mgr.master_url_file


def test_slurm_manager_uses_slurm_log_dir(tmp_path, monkeypatch):
    from fairway.config_loader import Config
    from fairway.hpc import SlurmManager

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.safe_dump({
        "project": "phase2d_project",
        "dataset_name": "phase2d_project",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [],
    }))
    cfg = Config(str(config_path))
    mgr = SlurmManager(cfg)

    # Spy on subprocess.run to capture the rendered template without
    # actually invoking sbatch.
    captured = {}
    def _fake_run(cmd, capture_output=False, text=False):
        captured["script_path"] = cmd[1]
        with open(cmd[1]) as f:
            captured["body"] = f.read()
        class _R:
            returncode = 0
            stdout = "Submitted batch job 999"
            stderr = ""
        return _R()

    monkeypatch.setattr("fairway.hpc.subprocess.run", _fake_run)
    mgr.submit_job(
        "submit_bare.sh",
        "submitting",
        extra_params={"summary_flag": ""},
    )
    body = captured["body"]
    # Accept either an #SBATCH --output pointing at slurm_log_dir or a
    # plain reference to it — template layout may shift, but the path
    # substitution is what matters.
    expected = str(cfg.paths.slurm_log_dir)
    assert expected in body, f"slurm_log_dir {expected} not in rendered script:\n{body}"
    assert cfg.paths.slurm_log_dir.exists(), "slurm_log_dir must be created at submit"
