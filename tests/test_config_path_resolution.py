"""
TDD Tests: Path resolution should prefer CWD (project root) over config directory.

When a config file lives in config/census_1950.yaml, relative paths like
"scripts/preprocess_ipums.py" and "schema/us1950_spec.yaml" should resolve
against the project root (CWD), not the config/ subdirectory.
"""
import os
import pytest
import yaml

from fairway.config_loader import Config


class TestResolvePathCWDFirst:
    """_resolve_path should check CWD before config_dir."""

    def _make_config(self, tmp_path):
        """Create a Config with config file inside a config/ subdirectory."""
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

    def test_prefers_cwd_when_file_exists_there(self, tmp_path, monkeypatch):
        """If scripts/foo.py exists at CWD, resolve there (not config_dir)."""
        monkeypatch.chdir(tmp_path)
        # Create file at CWD-relative path
        (tmp_path / "scripts").mkdir()
        (tmp_path / "scripts" / "foo.py").write_text("# script")
        # Also create at config_dir-relative path
        (tmp_path / "config" / "scripts").mkdir(parents=True, exist_ok=True)
        (tmp_path / "config" / "scripts" / "foo.py").write_text("# wrong")

        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("scripts/foo.py", config_dir)
        assert result == str(tmp_path / "scripts" / "foo.py")

    def test_falls_back_to_config_dir(self, tmp_path, monkeypatch):
        """If file only exists relative to config_dir, use that."""
        monkeypatch.chdir(tmp_path)
        # Only create at config_dir-relative path
        (tmp_path / "config" / "specs").mkdir(parents=True, exist_ok=True)
        (tmp_path / "config" / "specs" / "spec.yaml").write_text("columns: []")

        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("specs/spec.yaml", config_dir)
        assert result == str(tmp_path / "config" / "specs" / "spec.yaml")

    def test_defaults_to_cwd_when_neither_exists(self, tmp_path, monkeypatch):
        """For files that don't exist yet (e.g. spec to be generated), default to CWD."""
        monkeypatch.chdir(tmp_path)
        cfg = self._make_config(tmp_path)
        config_dir = str(tmp_path / "config")
        result = cfg._resolve_path("schema/us1950_spec.yaml", config_dir)
        assert result == str(tmp_path / "schema" / "us1950_spec.yaml")


class TestPreprocessScriptResolution:
    """Preprocess action scripts should resolve from CWD first."""

    def test_script_resolved_from_cwd(self, tmp_path, monkeypatch):
        """scripts/preprocess.py in config resolves to <project>/scripts/preprocess.py."""
        monkeypatch.chdir(tmp_path)

        # Create script at project root
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "preprocess.py").write_text("def process_file(f, o): pass")

        # Create data file so table expansion works
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        # Config lives in config/ subdir
        config_dir = tmp_path / "config"
        config_dir.mkdir(exist_ok=True)
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
        assert action == str(scripts_dir / "preprocess.py"), \
            f"Expected CWD-relative script path, got: {action}"

    def test_script_fallback_to_config_dir(self, tmp_path, monkeypatch):
        """If script only exists relative to config_dir, use that."""
        monkeypatch.chdir(tmp_path)

        # Create data file
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2")

        # Create script only in config/ subdir
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
        assert action == str(config_scripts / "preprocess.py"), \
            f"Expected config_dir-relative script path, got: {action}"


class TestFixedWidthSpecResolution:
    """fixed_width_spec should resolve from CWD when schema/ exists at project root."""

    def test_spec_resolves_to_cwd_schema_dir(self, tmp_path, monkeypatch):
        """fixed_width_spec: 'schema/spec.yaml' resolves to <project>/schema/spec.yaml."""
        monkeypatch.chdir(tmp_path)

        # Create schema dir at project root (spec file may not exist yet)
        schema_dir = tmp_path / "schema"
        schema_dir.mkdir()

        # Create data
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        (data_dir / "test.dat").write_text("data")

        # Config in config/ subdir
        config_dir = tmp_path / "config"
        config_dir.mkdir(exist_ok=True)
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
        # Should point to <project>/schema/spec.yaml, not config/schema/spec.yaml
        assert spec_path == str(schema_dir / "spec.yaml"), \
            f"Expected CWD-relative spec path, got: {spec_path}"
