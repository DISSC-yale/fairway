"""Phase 3 — config paths resolve against config_dir only, never CWD.

Reproducibility invariant: the same fairway.yaml must resolve to the
same absolute paths regardless of where the user invoked fairway from.
That rules out CWD-relative resolution. These tests pin the new rule.
"""
import os
import pytest
import yaml

from fairway.config import Config


class TestResolvePathConfigDirOnly:

    def _make_config(self, tmp_path):
        config_dir = tmp_path / "config"
        config_dir.mkdir(exist_ok=True)
        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [],
        }))
        return Config(str(config_path))

    def test_absolute_path_unchanged(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("/absolute/path.yaml", config_dir)
        assert result == "/absolute/path.yaml"

    def test_none_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        assert cfg._resolve_path(None, config_dir) is None

    def test_relative_resolves_against_config_dir_even_when_cwd_has_file(self, tmp_path, monkeypatch):
        """CWD is never consulted, even when it contains a matching file."""
        monkeypatch.chdir(tmp_path)
        # Bait: put the file at CWD-relative path.
        (tmp_path / "scripts").mkdir()
        (tmp_path / "scripts" / "foo.py").write_text("# should be ignored")

        cfg = self._make_config(tmp_path)
        config_dir = tmp_path / "config"
        result = cfg._resolve_path("scripts/foo.py", str(config_dir))
        # Must resolve under config_dir, not CWD.
        assert str(config_dir) in result
        assert result.endswith(os.path.join("config", "scripts", "foo.py"))

    def test_resolves_when_file_is_under_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config" / "specs").mkdir(parents=True, exist_ok=True)
        (tmp_path / "config" / "specs" / "spec.yaml").write_text("columns: []")

        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("specs/spec.yaml", config_dir)
        assert result == str(tmp_path / "config" / "specs" / "spec.yaml")

    def test_missing_file_defaults_to_config_dir(self, tmp_path, monkeypatch):
        """Missing files resolve relative to config_dir — the error the
        user sees ('no such file: <config_dir>/schema/x.yaml') is the
        same regardless of invocation directory, which is the point."""
        monkeypatch.chdir(tmp_path)
        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("schema/us1950_spec.yaml", config_dir)
        assert result == str(tmp_path / "config" / "schema" / "us1950_spec.yaml")


class TestPreprocessScriptResolution:
    """Preprocess scripts resolve under config_dir only."""

    def test_script_resolved_from_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        # Script lives beside the config file.
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_scripts = config_dir / "scripts"
        config_scripts.mkdir()
        (config_scripts / "preprocess.py").write_text("def process_file(f, o): pass")

        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'test_table',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
                'preprocess': {
                    'action': 'scripts/preprocess.py',
                    'scope': 'per_file',
                },
            }],
        }))

        cfg = Config(str(config_path))
        action = cfg.tables[0]['preprocess']['action']
        assert action == str(config_scripts / "preprocess.py")

    def test_cwd_script_is_ignored(self, tmp_path, monkeypatch):
        """Even if a CWD-relative script exists, it must not be picked."""
        monkeypatch.chdir(tmp_path)

        (tmp_path / "scripts").mkdir()
        (tmp_path / "scripts" / "preprocess.py").write_text("# should be ignored")

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        # Config_dir has NO scripts/ — so resolution falls through to
        # the default config_dir/scripts/preprocess.py (nonexistent).
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'test_table',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
                'preprocess': {
                    'action': 'scripts/preprocess.py',
                    'scope': 'per_file',
                },
            }],
        }))

        cfg = Config(str(config_path))
        action = cfg.tables[0]['preprocess']['action']
        # Must resolve under config_dir, not CWD.
        assert str(config_dir) in action
        assert str(tmp_path / "scripts" / "preprocess.py") != action


class TestFixedWidthSpecResolution:

    def test_spec_resolves_under_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        (data_dir / "test.dat").write_text("data")

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'test_table',
                'root': str(data_dir),
                'path': '*.dat',
                'format': 'fixed_width',
                'fixed_width_spec': 'schema/spec.yaml',
                'preprocess': {
                    'action': 'scripts/preprocess.py',
                    'scope': 'per_file',
                },
            }],
        }))

        cfg = Config(str(config_path))
        spec_path = cfg.tables[0]['fixed_width_spec']
        # Must resolve under config_dir — not <project>/schema/.
        assert spec_path == str(config_dir / "schema" / "spec.yaml")


class TestValidateConfigPaths:

    def test_rejects_non_absolute_path(self, tmp_path, monkeypatch):
        """validate_config_paths surfaces relative paths as errors."""
        from fairway.config import ConfigValidationError, validate_config_paths

        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'test_table',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
            }],
        }))
        cfg = Config(str(config_path))
        # Tamper: force a relative path into the loaded table to pin
        # the rule (normal load always yields absolute paths now).
        cfg.tables[0].path = "relative/path.csv"
        with pytest.raises(ConfigValidationError, match="not absolute"):
            validate_config_paths(cfg)

    def test_accepts_absolute_paths(self, tmp_path, monkeypatch):
        """Normally-loaded configs pass validation."""
        from fairway.config import validate_config_paths

        monkeypatch.chdir(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "test.yaml"
        config_path.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'test_table',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
            }],
        }))
        cfg = Config(str(config_path))
        validate_config_paths(cfg)  # must not raise
