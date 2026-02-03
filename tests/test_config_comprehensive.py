"""Comprehensive tests for fairway.yaml configuration options."""
import pytest
import os
from pathlib import Path


class TestStorageConfig:
    """Test storage configuration options."""

    @pytest.fixture
    def config_with_storage(self, tmp_path):
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config = f"""
dataset_name: test
engine: duckdb
storage:
  raw_dir: {tmp_path}/data/raw
  intermediate_dir: {tmp_path}/data/intermediate
  final_dir: {tmp_path}/data/final
  format: parquet
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config)
        return config_path

    def test_storage_dirs_accessible(self, config_with_storage):
        from fairway.config_loader import Config
        cfg = Config(str(config_with_storage))

        assert cfg.storage is not None
        assert 'raw_dir' in cfg.storage
        assert 'intermediate_dir' in cfg.storage
        assert 'final_dir' in cfg.storage


class TestTableFormats:
    """Test all supported table formats."""

    @pytest.fixture
    def data_dir(self, tmp_path):
        d = tmp_path / "data"
        d.mkdir()
        return d

    def test_csv_format(self, tmp_path, data_dir):
        (data_dir / "test.csv").write_text("a,b\n1,2\n")
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: csv_table
    path: {data_dir}/*.csv
    format: csv
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['format'] == 'csv'

    def test_tsv_format(self, tmp_path, data_dir):
        (data_dir / "test.tsv").write_text("a\tb\n1\t2\n")
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tsv_table
    path: {data_dir}/*.tsv
    format: tsv
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['format'] == 'tsv'

    def test_json_format(self, tmp_path, data_dir):
        (data_dir / "test.json").write_text('[{"a":1}]')
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: json_table
    path: {data_dir}/*.json
    format: json
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['format'] == 'json'


class TestTableOptions:
    """Test table-level configuration options."""

    @pytest.fixture
    def base_config(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")
        return tmp_path, data_dir

    def test_hive_partitioning(self, base_config):
        tmp_path, data_dir = base_config
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: partitioned
    path: {data_dir}
    format: csv
    hive_partitioning: true
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['hive_partitioning'] == True

    def test_write_mode(self, base_config):
        tmp_path, data_dir = base_config
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
    write_mode: append
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['write_mode'] == 'append'

    def test_partition_by(self, base_config):
        tmp_path, data_dir = base_config
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
    partition_by:
      - year
      - month
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['partition_by'] == ['year', 'month']

    def test_read_options(self, base_config):
        tmp_path, data_dir = base_config
        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
    read_options:
      header: false
      delim: "|"
"""
        (tmp_path / "config.yaml").write_text(config)
        from fairway.config_loader import Config
        cfg = Config(str(tmp_path / "config.yaml"))
        assert cfg.tables[0]['read_options']['header'] == False
        assert cfg.tables[0]['read_options']['delim'] == '|'


class TestValidationsConfig:
    """Test global validations configuration."""

    @pytest.fixture
    def config_with_validations(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("id,name\n1,alice\n")

        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
validations:
  level1:
    min_rows: 1
    max_rows: 1000000
  level2:
    check_nulls:
      - id
    check_unique:
      - id
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config)
        return config_path

    def test_validations_level1(self, config_with_validations):
        from fairway.config_loader import Config
        cfg = Config(str(config_with_validations))

        validations = cfg.data.get('validations', {})
        assert validations is not None
        assert validations['level1']['min_rows'] == 1
        assert validations['level1']['max_rows'] == 1000000

    def test_validations_level2(self, config_with_validations):
        from fairway.config_loader import Config
        cfg = Config(str(config_with_validations))

        validations = cfg.data.get('validations', {})
        assert 'check_nulls' in validations['level2']
        assert 'id' in validations['level2']['check_nulls']


class TestPerformanceConfig:
    """Test performance configuration."""

    @pytest.fixture
    def config_with_performance(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config = f"""
dataset_name: test
engine: duckdb
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
performance:
  target_rows: 100000
  compression: zstd
  salting: true
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config)
        return config_path

    def test_performance_options(self, config_with_performance):
        from fairway.config_loader import Config
        cfg = Config(str(config_with_performance))

        performance = cfg.data.get('performance', {})
        assert performance is not None
        assert performance.get('target_rows') == 100000
        assert performance.get('compression') == 'zstd'
        assert performance.get('salting') == True
