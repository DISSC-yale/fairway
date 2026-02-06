
import pytest
import yaml
import os
from fairway.config_loader import Config

def create_temp_config(filename, content):
    with open(filename, 'w') as f:
        yaml.dump(content, f)

def test_valid_engine():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': []
    }
    create_temp_config('valid_engine.yaml', config_data)
    try:
        config = Config('valid_engine.yaml')
        assert config.engine == 'duckdb'
        
        config_data['engine'] = 'pyspark'
        create_temp_config('valid_engine.yaml', config_data)
        config = Config('valid_engine.yaml')
        assert config.engine == 'pyspark'
    finally:
        if os.path.exists('valid_engine.yaml'):
            os.remove('valid_engine.yaml')

def test_invalid_engine():
    config_data = {
        'dataset_name': 'test',
        'engine': 'invalid',
        'tables': []
    }
    create_temp_config('invalid_engine.yaml', config_data)
    try:
        with pytest.raises(ValueError) as excinfo:
            Config('invalid_engine.yaml')
        assert "Invalid engine" in str(excinfo.value)
    finally:
        if os.path.exists('invalid_engine.yaml'):
            os.remove('invalid_engine.yaml')

def test_valid_table_format():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': [
            {'path': 'data/test.csv', 'format': 'csv'},
            {'path': 'data/test.json', 'format': 'json'},
            {'path': 'data/test.parquet', 'format': 'parquet'}
        ]
    }
    # Create dummy files so expansion works
    os.makedirs('data', exist_ok=True)
    with open('data/test.csv', 'w') as f: f.write('a,b\n1,2')
    with open('data/test.json', 'w') as f: f.write('{"a":1}')
    with open('data/test.parquet', 'w') as f: f.write('PAR1')

    create_temp_config('valid_format.yaml', config_data)
    try:
        # We need to be careful about strict path checking in _expand_tables
        # The Config class checks for existence.
        config = Config('valid_format.yaml')
        assert len(config.tables) == 3
    finally:
        if os.path.exists('valid_format.yaml'):
            os.remove('valid_format.yaml')
        import shutil
        if os.path.exists('data'):
            shutil.rmtree('data')

def test_invalid_table_format():
    config_data = {
        'dataset_name': 'test',
        'engine': 'duckdb',
        'tables': [
            {'path': 'data_invalid/test.txt', 'format': 'txt'}
        ]
    }
    os.makedirs('data_invalid', exist_ok=True)
    with open('data_invalid/test.txt', 'w') as f: f.write('content')

    create_temp_config('invalid_format.yaml', config_data)
    try:
        with pytest.raises(ValueError) as excinfo:
            Config('invalid_format.yaml')
        assert "Invalid format" in str(excinfo.value)
    finally:
        if os.path.exists('invalid_format.yaml'):
            os.remove('invalid_format.yaml')
        import shutil
        if os.path.exists('data_invalid'):
            shutil.rmtree('data_invalid')


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
