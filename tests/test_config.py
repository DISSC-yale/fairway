
import warnings

import pytest
import yaml
import os
from fairway.config_loader import Config, ConfigValidationError

def create_temp_config(filename, content):
    with open(filename, 'w') as f:
        yaml.dump(content, f)

def test_valid_engine(tmp_path):
    config_path = tmp_path / 'valid_engine.yaml'
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': []
    }
    create_temp_config(str(config_path), config_data)
    config = Config(str(config_path))
    assert config.engine == 'duckdb'

    config_data['engine'] = 'pyspark'
    create_temp_config(str(config_path), config_data)
    config = Config(str(config_path))
    assert config.engine == 'pyspark'

def test_invalid_engine(tmp_path):
    config_path = tmp_path / 'invalid_engine.yaml'
    config_data = {
        'dataset_name': 'test',
        'engine': 'invalid',
        'tables': []
    }
    create_temp_config(str(config_path), config_data)
    with pytest.raises(ValueError) as excinfo:
        Config(str(config_path))
    assert "Invalid engine" in str(excinfo.value)

def test_valid_table_format(tmp_path):
    data_dir = tmp_path / 'data'
    data_dir.mkdir()
    (data_dir / 'test.csv').write_text('a,b\n1,2')
    (data_dir / 'test.json').write_text('{"a":1}')
    (data_dir / 'test.parquet').write_text('PAR1')

    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': [
            {'name': 'csv_table', 'path': str(data_dir / 'test.csv'), 'format': 'csv'},
            {'name': 'json_table', 'path': str(data_dir / 'test.json'), 'format': 'json'},
            {'name': 'parquet_table', 'path': str(data_dir / 'test.parquet'), 'format': 'parquet'}
        ]
    }
    config_path = tmp_path / 'valid_format.yaml'
    create_temp_config(str(config_path), config_data)
    config = Config(str(config_path))
    assert len(config.tables) == 3

def test_invalid_table_format(tmp_path):
    data_dir = tmp_path / 'data_invalid'
    data_dir.mkdir()
    (data_dir / 'test.txt').write_text('content')

    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': [
            {'path': str(data_dir / 'test.txt'), 'format': 'txt'}
        ]
    }
    config_path = tmp_path / 'invalid_format.yaml'
    create_temp_config(str(config_path), config_data)
    with pytest.raises(ValueError) as excinfo:
        Config(str(config_path))
    assert "Invalid format" in str(excinfo.value)


class TestBatchStrategyValidation:
    """Validation tests for the batch_strategy config key."""

    def _write_config(self, tmp_path, tables):
        config_path = str(tmp_path / "config.yaml")
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        # Create dummy files so path validation passes
        (data_dir / "CT_2023_01.csv").write_text("a,b\n1,2")
        (data_dir / "NY_2023_01.csv").write_text("a,b\n3,4")

        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': tables
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_valid_partition_aware_config(self, tmp_path):
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state>[A-Z]{2})_(?P<year>\d{4})',
            'partition_by': ['state', 'year'],
            'batch_strategy': 'partition_aware',
        }])
        config = Config(config_path)
        assert config.tables[0]['batch_strategy'] == 'partition_aware'

    def test_default_batch_strategy_is_bulk(self, tmp_path):
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
        }])
        config = Config(config_path)
        assert config.tables[0]['batch_strategy'] == 'bulk'

    def test_explicit_bulk_strategy(self, tmp_path):
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'batch_strategy': 'bulk',
        }])
        config = Config(config_path)
        assert config.tables[0]['batch_strategy'] == 'bulk'

    def test_partition_aware_without_naming_pattern_fails(self, tmp_path):
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'partition_by': ['state', 'year'],
            'batch_strategy': 'partition_aware',
        }])
        with pytest.raises(ConfigValidationError, match="naming_pattern"):
            Config(config_path)

    def test_partition_aware_without_partition_by_fails(self, tmp_path):
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state>[A-Z]{2})_(?P<year>\d{4})',
            'batch_strategy': 'partition_aware',
        }])
        with pytest.raises(ConfigValidationError, match="partition_by"):
            Config(config_path)

    def test_partition_aware_missing_regex_groups_fails(self, tmp_path):
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state>[A-Z]{2})_\d{4}',  # no year group
            'partition_by': ['state', 'year'],
            'batch_strategy': 'partition_aware',
        }])
        with pytest.raises(ConfigValidationError, match="year"):
            Config(config_path)

    def test_invalid_batch_strategy_value_fails(self, tmp_path):
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "*.csv"),
            'format': 'csv',
            'batch_strategy': 'invalid_strategy',
        }])
        with pytest.raises(ConfigValidationError, match="batch_strategy"):
            Config(config_path)


class TestIdentifierValidation:
    """RULE-119: Validate user-provided identifiers to prevent injection."""

    def _write_config(self, tmp_path, tables, global_partition_by=None):
        config_path = str(tmp_path / "config.yaml")
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        (data_dir / "test.csv").write_text("a,b\n1,2")

        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': tables
        }
        if global_partition_by:
            config['partition_by'] = global_partition_by
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_valid_table_name(self, tmp_path):
        """Valid identifier names should pass."""
        config_path = self._write_config(tmp_path, [{
            'name': 'valid_table_123',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
        }])
        config = Config(config_path)
        assert config.tables[0]['name'] == 'valid_table_123'

    def test_invalid_table_name_with_spaces(self, tmp_path):
        """Table name with spaces should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'invalid table',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
        }])
        with pytest.raises(ConfigValidationError, match="Invalid table name"):
            Config(config_path)

    def test_invalid_table_name_with_sql_injection(self, tmp_path):
        """Table name with SQL injection attempt should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'table; DROP TABLE--',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
        }])
        with pytest.raises(ConfigValidationError, match="Invalid table name"):
            Config(config_path)

    def test_invalid_table_name_starting_with_number(self, tmp_path):
        """Table name starting with number should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': '123_table',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
        }])
        with pytest.raises(ConfigValidationError, match="Invalid table name"):
            Config(config_path)

    def test_valid_partition_by_columns(self, tmp_path):
        """Valid partition_by column names should pass."""
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
            'partition_by': ['state', 'year_2023'],
        }])
        config = Config(config_path)
        assert config.tables[0]['partition_by'] == ['state', 'year_2023']

    def test_invalid_partition_by_with_injection(self, tmp_path):
        """partition_by with injection attempt should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
            'partition_by': ['state', "year'); DROP TABLE x--"],
        }])
        with pytest.raises(ConfigValidationError, match="Invalid partition_by column"):
            Config(config_path)

    def test_invalid_global_partition_by(self, tmp_path):
        """Global partition_by with invalid identifier should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(
            tmp_path,
            [{
                'name': 'claims',
                'path': str(tmp_path / "data" / "test.csv"),
                'format': 'csv',
            }],
            global_partition_by=['valid_col', 'invalid-col']
        )
        with pytest.raises(ConfigValidationError, match="Invalid column"):
            Config(config_path)

    def test_valid_naming_pattern_groups(self, tmp_path):
        """Valid naming_pattern capture groups should pass."""
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state>[A-Z]{2})_(?P<year_2023>\d{4})',
        }])
        config = Config(config_path)
        assert 'naming_pattern' in config.tables[0]

    def test_invalid_naming_pattern_group_with_hyphen(self, tmp_path):
        """naming_pattern group with hyphen should fail."""
        from fairway.config_loader import ConfigValidationError
        config_path = self._write_config(tmp_path, [{
            'name': 'claims',
            'path': str(tmp_path / "data" / "test.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state-code>[A-Z]{2})',
        }])
        with pytest.raises(ConfigValidationError, match="Invalid naming_pattern group"):
            Config(config_path)


class TestValidationHardening:
    """C3 + C4: config validation hardening."""

    def _write(self, tmp_path, data, name="fw.yaml"):
        p = tmp_path / name
        p.write_text(yaml.dump(data))
        return str(p)

    def _make_data_dir(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "t.csv").write_text("id\n1\n")
        return str(data_dir)

    def test_nested_validations_raises_error(self, tmp_path):
        """Nested dict under an unknown key must raise — catches typos like 'levl1: {min_rows:1}'."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"levl1": {"min_rows": 1}}}],
        })
        with pytest.raises(ConfigValidationError) as exc_info:
            Config(path)
        assert "flat" in str(exc_info.value).lower()

    def test_unknown_top_level_key_warns(self, tmp_path):
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")}, "tables": [],
            "unknown_key_xyz": "should warn",
        })
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Config(path)
        messages = [str(warning.message) for warning in w]
        assert any("unknown_key_xyz" in m for m in messages), (
            f"No warning emitted. Messages: {messages}"
        )

    def test_check_unique_raises_at_load(self, tmp_path):
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"check_unique": ["id"]}}],
        })
        with pytest.raises(ConfigValidationError) as exc_info:
            Config(path)
        assert "check_unique" in str(exc_info.value)

    def test_data_sources_aliased_with_warning(self, tmp_path):
        """data.sources should be aliased to data.tables with deprecation warning."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "data": {"sources": [{
                "name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
            }]},
        })
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = Config(path)
        assert len(config.tables) == 1, "data.sources was silently dropped"
        messages = [str(warning.message).lower() for warning in w]
        assert any("sources" in m for m in messages), (
            f"No deprecation warning. Messages: {messages}"
        )

    def test_unknown_validation_key_warns(self, tmp_path):
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"weird_made_up_check": True}}],
        })
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Config(path)
        messages = [str(warning.message).lower() for warning in w]
        assert any("weird_made_up_check" in m for m in messages)

    def test_both_sources_and_tables_raises(self, tmp_path):
        """Both data.sources and data.tables — error, don't silently pick one."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "data": {
                "sources": [{"name": "a", "path": f"{data_dir}/*.csv", "format": "csv"}],
                "tables":  [{"name": "b", "path": f"{data_dir}/*.csv", "format": "csv"}],
            },
        })
        with pytest.raises(ConfigValidationError, match="both 'data.sources'"):
            Config(path)

    def test_legacy_level1_nesting_does_not_error(self, tmp_path):
        """Validator supports legacy level1/level2; config load must not reject it."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"level1": {"min_rows": 1}}}],
        })
        Config(path)  # must not raise

    def test_known_key_wrong_shape_raises(self, tmp_path):
        """min_rows must be int — a dict value should fail at load, not at validator runtime."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"min_rows": {"nested": 5}}}],
        })
        with pytest.raises(ConfigValidationError, match="min_rows"):
            Config(path)

    def test_check_range_accepts_dict_value(self, tmp_path):
        """check_range legitimately takes dict of col -> [min, max] — must not trip shape check."""
        data_dir = self._make_data_dir(tmp_path)
        path = self._write(tmp_path, {
            "dataset_name": "x", "engine": "duckdb",
            "storage": {"root": str(tmp_path / "out")},
            "tables": [{"name": "t", "path": f"{data_dir}/*.csv", "format": "csv",
                        "validations": {"check_range": {"age": [0, 120]}}}],
        })
        Config(path)  # must not raise
