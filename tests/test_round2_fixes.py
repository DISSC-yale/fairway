"""Round-2 fix pins: state-init legacy warning, cache clean real+legacy
targets, submit --dry-run resolves slurm_log_dir.

These lock the behavior that critic review said was un-pinned after
the first round of review-response fixes.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from fairway.cli import main

pytestmark = pytest.mark.hpc_contract


pytest.importorskip("duckdb")


def _write_config(tmp_path, extra=None):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "t.csv").write_text("a,b\n1,2")
    body = {
        "project": "round2_project",
        "dataset_name": "round2_project",
        "engine": "duckdb",
        "tables": [{
            "name": "t",
            "path": str(tmp_path / "data" / "t.csv"),
            "format": "csv",
        }],
    }
    if extra:
        body.update(extra)
    path = cfg_dir / "fairway.yaml"
    path.write_text(yaml.safe_dump(body))
    return str(path)


@pytest.fixture
def runner():
    return CliRunner()


# --- state init legacy manifest warning ---------------------------------

class TestStateInitLegacyWarning:
    def test_warns_when_config_dir_has_legacy_manifest(self, tmp_path, runner):
        cfg = _write_config(tmp_path)
        # Simulate a pre-v4.1 manifest dir next to the config file.
        legacy = Path(cfg).parent / "manifest"
        legacy.mkdir()
        (legacy / "old_table.json").write_text('{"files": {}}')

        result = runner.invoke(main, ["state", "init", "--config", cfg])
        assert result.exit_code == 0, result.output
        # Warning goes to stderr but CliRunner merges; check substring.
        assert "legacy manifest dir found" in result.output or \
               "legacy manifest dir found" in (result.stderr_bytes or b"").decode()

    def test_silent_when_no_legacy_manifest(self, tmp_path, runner):
        cfg = _write_config(tmp_path)
        result = runner.invoke(main, ["state", "init", "--config", cfg])
        assert result.exit_code == 0, result.output
        assert "legacy manifest dir found" not in result.output

    def test_silent_when_legacy_dir_empty(self, tmp_path, runner):
        cfg = _write_config(tmp_path)
        # Empty manifest dir (no *.json) — not real legacy state.
        (Path(cfg).parent / "manifest").mkdir()
        result = runner.invoke(main, ["state", "init", "--config", cfg])
        assert result.exit_code == 0, result.output
        assert "legacy manifest dir found" not in result.output


# --- cache clean -------------------------------------------------------

class TestCacheClean:
    def test_clears_resolved_cache_dir(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        # Trigger Config → paths.cache_dir creation then drop a file in it.
        from fairway.config_loader import Config
        c = Config(cfg)
        cache = c.paths.cache_dir
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "marker").write_text("x")

        result = runner.invoke(
            main,
            ["cache", "clean", "--config", cfg, "--force"],
        )
        assert result.exit_code == 0, result.output
        assert not cache.exists(), f"cache_dir still exists: {cache}"

    def test_clears_legacy_cwd_cache(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        legacy = tmp_path / ".fairway_cache"
        legacy.mkdir()
        (legacy / "marker").write_text("x")

        result = runner.invoke(
            main,
            ["cache", "clean", "--config", cfg, "--force"],
        )
        assert result.exit_code == 0, result.output
        assert not legacy.exists(), f"legacy cache still exists: {legacy}"

    def test_handles_symlinked_cache_dir(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        from fairway.config_loader import Config
        c = Config(cfg)
        cache = c.paths.cache_dir
        # Make cache_dir a symlink to a real dir elsewhere.
        real = tmp_path / "real_cache"
        real.mkdir()
        cache.parent.mkdir(parents=True, exist_ok=True)
        if cache.exists():
            cache.rmdir()
        os.symlink(str(real), str(cache))

        result = runner.invoke(
            main,
            ["cache", "clean", "--config", cfg, "--force"],
        )
        assert result.exit_code == 0, result.output
        assert not os.path.lexists(str(cache)), "symlink still present"
        # The symlink target is preserved — we only unlink the pointer.
        assert real.exists()

    def test_handles_dangling_symlink(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        from fairway.config_loader import Config
        c = Config(cfg)
        cache = c.paths.cache_dir
        # Point cache at a target, then delete the target so cache_dir
        # is a dangling symlink. islink() is True; exists() is False.
        target = tmp_path / "gone"
        target.mkdir()
        cache.parent.mkdir(parents=True, exist_ok=True)
        if cache.exists():
            cache.rmdir()
        os.symlink(str(target), str(cache))
        target.rmdir()
        assert not cache.exists() and os.path.islink(str(cache))

        result = runner.invoke(
            main,
            ["cache", "clean", "--config", cfg, "--force"],
        )
        assert result.exit_code == 0, result.output
        assert not os.path.lexists(str(cache)), "dangling symlink still present"

    def test_nothing_to_clear(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(
            main,
            ["cache", "clean", "--config", cfg, "--force"],
        )
        assert result.exit_code == 0, result.output
        assert "No cache to clear" in result.output


# --- submit --dry-run uses slurm_log_dir -------------------------------

class TestSubmitDryRun:
    def test_dry_run_renders_real_slurm_log_dir(self, tmp_path, monkeypatch, runner):
        cfg = _write_config(tmp_path)
        # Drop a minimal spark.yaml so the account-placeholder check
        # passes without needing to patch the config.
        (Path(cfg).parent / "spark.yaml").write_text(
            yaml.safe_dump({"slurm": {"account": "real_acct"}})
        )
        monkeypatch.chdir(tmp_path)
        from fairway.config_loader import Config
        c = Config(cfg)
        expected_log_dir = str(c.paths.slurm_log_dir)

        result = runner.invoke(main, ["submit", "--config", cfg, "--dry-run"])
        assert result.exit_code == 0, result.output
        # The rendered sbatch script must carry the resolved dir, not
        # the old hardcoded 'logs' literal.
        assert expected_log_dir in result.output, \
            f"expected {expected_log_dir} in dry-run output:\n{result.output}"
        assert "#SBATCH --output=logs/fairway" not in result.output, \
            f"stale 'logs' literal leaked into dry-run:\n{result.output}"

    # test_dry_run_with_spark_renders_container_env_forwarding removed in
    # v0.3 Step 2 — the spark/container submit path was deleted.

    def test_binds_include_config_dir_for_with_spark(self, tmp_path, monkeypatch, runner):
        project = tmp_path / "project"
        config_dir = project / "config"
        config_dir.mkdir(parents=True)
        data_dir = tmp_path / "outside_data"
        data_dir.mkdir()
        (data_dir / "t.csv").write_text("a,b\n1,2")
        cfg = config_dir / "fairway.yaml"
        cfg.write_text(yaml.safe_dump({
            "project": "round2_project",
            "dataset_name": "round2_project",
            "engine": "duckdb",
            "tables": [{
                "name": "t",
                "path": str(data_dir / "t.csv"),
                "format": "csv",
            }],
        }))
        (config_dir / "spark.yaml").write_text(
            yaml.safe_dump({"slurm": {"account": "real_acct"}})
        )
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(
            main, ["submit", "--config", str(cfg), "--with-spark", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert str(config_dir) in result.output, result.output
