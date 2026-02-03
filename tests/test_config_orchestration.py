"""Tests for orchestration config features - TDD."""
import pytest
import os
from pathlib import Path


class TestOrchestrationConfig:
    """Test orchestration configuration section."""

    @pytest.fixture
    def config_with_orchestration(self, tmp_path):
        """Create config with orchestration section."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_content = f"""
dataset_name: test
engine: duckdb

orchestration:
  batch_size: 50
  work_dir: {tmp_path}/.fairway/work

tables:
  - name: test_table
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config_content)
        return config_path

    def test_orchestration_batch_size(self, config_with_orchestration):
        """Orchestration batch_size is accessible."""
        from fairway.config_loader import Config
        cfg = Config(str(config_with_orchestration))

        orchestration = cfg.data.get('orchestration', {})
        assert orchestration.get('batch_size') == 50

    def test_orchestration_work_dir(self, config_with_orchestration, tmp_path):
        """Orchestration work_dir is accessible."""
        from fairway.config_loader import Config
        cfg = Config(str(config_with_orchestration))

        orchestration = cfg.data.get('orchestration', {})
        assert str(tmp_path) in orchestration.get('work_dir')


class TestTableDependsOnConfig:
    """Test table depends_on configuration for dependencies."""

    @pytest.fixture
    def config_with_depends_on(self, tmp_path):
        """Create config with depends_on dependencies."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_content = f"""
dataset_name: test
engine: duckdb

tables:
  - name: raw_data
    path: {data_dir}/*.csv
    format: csv

  - name: processed_data
    depends_on: raw_data
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config_content)
        return config_path

    def test_table_depends_on_field(self, config_with_depends_on):
        """Table depends_on field is preserved."""
        from fairway.config_loader import Config
        cfg = Config(str(config_with_depends_on))

        # Check raw_data table (no dependency)
        raw_table = cfg.get_table_by_name('raw_data')
        assert raw_table is not None
        assert raw_table.get('depends_on') is None

        # Check processed_data table (has dependency)
        processed_table = cfg.get_table_by_name('processed_data')
        assert processed_table is not None
        assert processed_table.get('depends_on') == 'raw_data'


class TestTableLevelValidation:
    """Test table-level validation configuration."""

    @pytest.fixture
    def config_with_table_validation(self, tmp_path):
        """Create config with table-level validation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        config_content = f"""
dataset_name: test
engine: duckdb

tables:
  - name: test_table
    path: {data_dir}/*.csv
    format: csv
    validation:
      min_rows: 1
      check_nulls:
        - a
        - b
    performance:
      target_rows: 500000
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config_content)
        return config_path

    def test_table_validation_config(self, config_with_table_validation):
        """Table-level validation is accessible."""
        from fairway.config_loader import Config
        cfg = Config(str(config_with_table_validation))

        table = cfg.get_table_by_name('test_table')
        assert table is not None

        validation = table.get('validation', {})
        assert validation.get('min_rows') == 1
        assert 'a' in validation.get('check_nulls', [])

    def test_table_performance_config(self, config_with_table_validation):
        """Table-level performance is accessible."""
        from fairway.config_loader import Config
        cfg = Config(str(config_with_table_validation))

        table = cfg.get_table_by_name('test_table')
        performance = table.get('performance', {})
        assert performance.get('target_rows') == 500000
