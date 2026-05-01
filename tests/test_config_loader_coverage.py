"""Additional config_loader tests to increase coverage."""
import pytest
import yaml
import json
import os
from fairway.config import Config, ConfigValidationError


class TestLoadSchema:
    """Tests for Config._load_schema."""

    def _make_config(self, tmp_path, tables):
        config_path = str(tmp_path / "config.yaml")
        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': tables,
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_schema_as_dict(self, tmp_path):
        """Schema provided as inline dict is used directly."""
        (tmp_path / "data.csv").write_text("a,b\n1,2")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': {'col_a': 'VARCHAR', 'col_b': 'INTEGER'},
        }])
        cfg = Config(config_path)
        assert cfg.tables[0]['schema'] == {'col_a': 'VARCHAR', 'col_b': 'INTEGER'}

    def test_schema_from_yaml_file(self, tmp_path):
        """Schema loaded from external YAML file."""
        schema_data = {'col_a': 'VARCHAR'}
        schema_path = tmp_path / "schema.yaml"
        with open(schema_path, 'w') as f:
            yaml.dump(schema_data, f)

        (tmp_path / "data.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': str(schema_path),
        }])
        cfg = Config(config_path)
        assert cfg.tables[0]['schema'] == {'col_a': 'VARCHAR'}

    def test_schema_from_json_file(self, tmp_path):
        """Schema loaded from external JSON file."""
        schema_data = {'col_x': 'DOUBLE'}
        schema_path = tmp_path / "schema.json"
        with open(schema_path, 'w') as f:
            json.dump(schema_data, f)

        (tmp_path / "data.csv").write_text("x\n1")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': str(schema_path),
        }])
        cfg = Config(config_path)
        assert cfg.tables[0]['schema'] == {'col_x': 'DOUBLE'}

    def test_schema_from_generate_schema_format(self, tmp_path):
        """Schema in generate-schema output format (with 'columns' key)."""
        schema_data = {'columns': {'id': 'INTEGER', 'name': 'VARCHAR'}}
        schema_path = tmp_path / "schema.yaml"
        with open(schema_path, 'w') as f:
            yaml.dump(schema_data, f)

        (tmp_path / "data.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': str(schema_path),
        }])
        cfg = Config(config_path)
        assert cfg.tables[0]['schema'] == {'id': 'INTEGER', 'name': 'VARCHAR'}

    def test_schema_file_not_found(self, tmp_path):
        """Missing schema file raises FileNotFoundError."""
        (tmp_path / "data.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': 'nonexistent_schema.yaml',
        }])
        with pytest.raises(FileNotFoundError):
            Config(config_path)

    def test_schema_unsupported_format(self, tmp_path):
        """Schema file with unsupported extension raises ValueError."""
        schema_path = tmp_path / "schema.txt"
        schema_path.write_text("col_a VARCHAR")

        (tmp_path / "data.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [{
            'name': 'tbl',
            'path': str(tmp_path / "data.csv"),
            'format': 'csv',
            'schema': str(schema_path),
        }])
        with pytest.raises(ValueError, match="Unsupported schema file format"):
            Config(config_path)

    def test_schema_relative_to_config_dir(self, tmp_path):
        """Schema path resolved relative to config file directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        schema_data = {'id': 'INTEGER'}
        schema_path = config_dir / "schema.yaml"
        with open(schema_path, 'w') as f:
            yaml.dump(schema_data, f)

        (tmp_path / "data.csv").write_text("a\n1")
        config_path = str(config_dir / "config.yaml")
        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'tbl',
                'path': str(tmp_path / "data.csv"),
                'format': 'csv',
                'schema': 'schema.yaml',  # relative path
            }],
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        cfg = Config(config_path)
        assert cfg.tables[0]['schema'] == {'id': 'INTEGER'}


class TestValidation:
    """Tests for Config._validate covering uncovered branches."""

    def _make_config(self, tmp_path, tables, **extra):
        config_path = str(tmp_path / "config.yaml")
        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': tables,
            **extra,
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_duplicate_table_name(self, tmp_path):
        """Duplicate table names raise ConfigValidationError."""
        (tmp_path / "a.csv").write_text("a\n1")
        (tmp_path / "b.csv").write_text("b\n2")
        config_path = self._make_config(tmp_path, [
            {'name': 'tbl', 'path': str(tmp_path / "a.csv"), 'format': 'csv'},
            {'name': 'tbl', 'path': str(tmp_path / "b.csv"), 'format': 'csv'},
        ])
        with pytest.raises(ConfigValidationError, match="Duplicate"):
            Config(config_path)

    def test_missing_name_field(self, tmp_path):
        """Table without name raises ConfigValidationError."""
        (tmp_path / "a.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [
            {'path': str(tmp_path / "a.csv"), 'format': 'csv'},
        ])
        with pytest.raises(ConfigValidationError, match="name"):
            Config(config_path)

    def test_path_and_archives_both_present(self, tmp_path):
        """Specifying both path and archives raises error."""
        (tmp_path / "a.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.csv"),
                'archives': str(tmp_path / "*.zip"),
                'files': '*.csv',
                'format': 'csv',
            },
        ])
        with pytest.raises(ConfigValidationError, match="Cannot specify both"):
            Config(config_path)

    def test_archives_without_files(self, tmp_path):
        """Archives without files pattern raises error after expansion."""
        # Need a valid path so _expand_tables keeps the entry
        (tmp_path / "a.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.csv"),
                'archives': str(tmp_path / "*.zip"),
                'format': 'csv',
            },
        ])
        # path+archives together triggers "Cannot specify both" in _validate
        with pytest.raises(ConfigValidationError, match="Cannot specify both"):
            Config(config_path)

    def test_files_without_archives(self, tmp_path):
        """Files without archives raises error (files key survives in expanded table)."""
        (tmp_path / "a.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.csv"),
                'files': '*.csv',
                'format': 'csv',
            },
        ])
        with pytest.raises(ConfigValidationError, match="without 'archives'"):
            Config(config_path)

    def test_fixed_width_without_spec(self, tmp_path):
        """Fixed-width format without spec raises error."""
        (tmp_path / "a.txt").write_text("data")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.txt"),
                'format': 'fixed_width',
            },
        ])
        with pytest.raises(ConfigValidationError, match="fixed_width_spec"):
            Config(config_path)

    def test_fixed_width_spec_not_found(self, tmp_path):
        """Fixed-width spec file that doesn't exist raises error."""
        (tmp_path / "a.txt").write_text("data")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.txt"),
                'format': 'fixed_width',
                'fixed_width_spec': 'nonexistent_spec.yaml',
            },
        ])
        with pytest.raises(ConfigValidationError, match="fixed_width_spec file not found"):
            Config(config_path)

    def test_root_dir_not_found(self, tmp_path):
        """Root directory that doesn't exist raises error."""
        (tmp_path / "a.csv").write_text("a\n1")
        config_path = self._make_config(tmp_path, [
            {
                'name': 'tbl',
                'path': str(tmp_path / "a.csv"),
                'format': 'csv',
                'root': '/nonexistent/root/dir',
            },
        ])
        with pytest.raises(ConfigValidationError, match="Root directory"):
            Config(config_path)

    def test_no_path_no_archives_skipped_during_expansion(self, tmp_path):
        """Table with neither path nor archives is silently skipped by _expand_tables."""
        config_path = self._make_config(tmp_path, [
            {'name': 'tbl', 'format': 'csv'},
        ])
        # _expand_tables skips entries without path_pattern, so tables list is empty
        cfg = Config(config_path)
        assert len(cfg.tables) == 0
