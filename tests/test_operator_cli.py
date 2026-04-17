"""Phase 6 — operator tooling: state init, state path, doctor, preflight.

These commands make FAIRWAY_HOME and related state visible/actionable
from the CLI. Tests pin that:
  - `state init` creates the directory tree idempotently.
  - `state path` surfaces every managed path under a stable label.
  - `doctor` reports env vars and dir existence without crashing when
     dirs are missing.
  - `preflight` catches missing table roots and placeholder accounts.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from fairway.cli import main


def _write_config(tmp_path, extra=None):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "test.csv").write_text("a,b\n1,2")
    body = {
        "project": "phase6_project",
        "dataset_name": "phase6_project",
        "engine": "duckdb",
        "tables": [{
            "name": "t",
            "path": str(tmp_path / "data" / "test.csv"),
            "format": "csv",
        }],
    }
    if extra:
        body.update(extra)
    path = config_dir / "fairway.yaml"
    path.write_text(yaml.safe_dump(body))
    return str(path)


@pytest.fixture
def runner():
    return CliRunner()


class TestStateInit:
    def test_creates_all_project_dirs(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        result = runner.invoke(main, ["state", "init", "--config", config_path])
        assert result.exit_code == 0, result.output
        # Output lists every managed directory
        for label in ("manifest_dir", "log_dir", "slurm_log_dir",
                      "spark_coordination_dir", "lock_dir", "cache_dir", "temp_dir"):
            assert label in result.output

        # FAIRWAY_HOME autouse fixture points these at tmp dirs under the
        # test's own tmp_path; verify the tree actually exists.
        state_root = Path(os.environ["FAIRWAY_HOME"]) / "projects" / "phase6_project"
        for sub in ("manifest", "logs", "slurm_logs", "spark", "locks"):
            assert (state_root / sub).exists(), sub

    def test_is_idempotent(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        first = runner.invoke(main, ["state", "init", "--config", config_path])
        second = runner.invoke(main, ["state", "init", "--config", config_path])
        assert first.exit_code == 0 and second.exit_code == 0


class TestStatePath:
    def test_prints_every_managed_path(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        result = runner.invoke(main, ["state", "path", "--config", config_path])
        assert result.exit_code == 0, result.output
        for label in ("project_state_dir", "manifest_dir", "log_dir",
                      "slurm_log_dir", "spark_coordination_dir", "lock_dir",
                      "project_scratch_dir", "cache_dir", "temp_dir"):
            assert label in result.output


class TestDoctor:
    def test_reports_env_and_dirs(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        result = runner.invoke(main, ["doctor", "--config", config_path])
        assert result.exit_code == 0, result.output
        assert "FAIRWAY_HOME=" in result.output
        assert "FAIRWAY_SCRATCH=" in result.output
        assert "project:" in result.output
        assert "phase6_project" in result.output

    def test_flags_missing_dirs_without_crashing(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        # No state init — every managed dir is missing.
        result = runner.invoke(main, ["doctor", "--config", config_path])
        assert result.exit_code == 0, result.output
        assert "[miss]" in result.output


class TestPreflight:
    def test_passes_on_valid_config(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        result = runner.invoke(main, ["preflight", "--config", config_path])
        assert result.exit_code == 0, result.output
        assert "preflight OK" in result.output

    def test_fails_on_missing_table_root(self, tmp_path, runner):
        """A missing table root fails preflight. Config loader may catch
        this first (stricter) — either way, exit is non-zero and the
        rejection mentions the missing path."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "fairway.yaml").write_text(yaml.safe_dump({
            "project": "phase6_project",
            "dataset_name": "phase6_project",
            "engine": "duckdb",
            "tables": [{
                "name": "t",
                "root": str(tmp_path / "nope"),
                "path": "*.csv",
                "format": "csv",
            }],
        }))
        result = runner.invoke(
            main, ["preflight", "--config", str(config_dir / "fairway.yaml")]
        )
        assert result.exit_code != 0
        detail = result.output + (repr(result.exception) if result.exception else "")
        assert "nope" in detail or "does not exist" in detail.lower()

    def test_require_account_rejects_placeholder(self, tmp_path, runner):
        config_path = _write_config(tmp_path)
        (tmp_path / "config" / "spark.yaml").write_text(
            yaml.safe_dump({"account": "your-account"})
        )
        result = runner.invoke(
            main, ["preflight", "--config", config_path, "--require-account"]
        )
        assert result.exit_code != 0
        assert "placeholder" in result.output.lower()
