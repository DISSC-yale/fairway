"""Tests for BatchProcessor - TDD: RED phase."""
import pytest
import os
import json
import math
from pathlib import Path


class TestBatchProcessor:
    """TDD tests for BatchProcessor class."""

    @pytest.fixture
    def sample_config(self, tmp_path):
        """Create a sample config with test files."""
        # Create test data files
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)

        for i in range(10):
            (data_dir / f"file_{i:03d}.csv").write_text(f"id,value\n{i},test{i}\n")

        # Create config file - use absolute path for glob
        config_content = f"""
dataset_name: test_dataset
engine: duckdb

orchestration:
  batch_size: 3
  work_dir: {tmp_path}/.fairway/work

tables:
  - name: test_table
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "config" / "fairway.yaml"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(config_content)

        return config_path

    @pytest.fixture
    def batch_processor(self, sample_config):
        """Create BatchProcessor instance."""
        from fairway.batch_processor import BatchProcessor
        return BatchProcessor(
            config_path=str(sample_config),
            table="test_table"
        )

    def test_init(self, batch_processor):
        """BatchProcessor initializes with config and table."""
        assert batch_processor.table_name == "test_table"
        assert batch_processor.batch_size == 3

    def test_get_file_count(self, batch_processor):
        """get_file_count returns total files for table."""
        count = batch_processor.get_file_count()
        assert count == 10

    def test_get_batch_count(self, batch_processor):
        """get_batch_count calculates batches with ceiling division."""
        # 10 files / 3 per batch = 4 batches (ceil)
        count = batch_processor.get_batch_count()
        assert count == 4

    def test_get_batch_count_exact_division(self, tmp_path):
        """get_batch_count handles exact division."""
        # Create 9 files (9 / 3 = 3 exact)
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        for i in range(9):
            (data_dir / f"file_{i:03d}.csv").write_text(f"id,value\n{i},test\n")

        config_content = f"""
dataset_name: test
engine: duckdb
orchestration:
  batch_size: 3
tables:
  - name: test_table
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "config" / "fairway.yaml"
        config_path.parent.mkdir()
        config_path.write_text(config_content)

        from fairway.batch_processor import BatchProcessor
        bp = BatchProcessor(str(config_path), "test_table")
        assert bp.get_batch_count() == 3

    def test_get_files_for_batch(self, batch_processor):
        """get_files_for_batch returns correct file subset."""
        # Batch 0: files 0-2
        batch_0 = batch_processor.get_files_for_batch(0)
        assert len(batch_0) == 3

        # Batch 3 (last): files 9 only (remainder)
        batch_3 = batch_processor.get_files_for_batch(3)
        assert len(batch_3) == 1

    def test_get_files_for_batch_invalid(self, batch_processor):
        """get_files_for_batch raises on invalid batch number."""
        with pytest.raises(ValueError, match="Invalid batch"):
            batch_processor.get_files_for_batch(99)

    def test_get_files_for_batch_deterministic(self, batch_processor):
        """get_files_for_batch returns same files on multiple calls."""
        batch_0_first = batch_processor.get_files_for_batch(0)
        batch_0_second = batch_processor.get_files_for_batch(0)
        assert batch_0_first == batch_0_second


class TestBatchProcessorWithOverride:
    """Test batch_size override from constructor."""

    @pytest.fixture
    def config_with_override(self, tmp_path):
        """Config where batch_size is overridden."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for i in range(10):
            (data_dir / f"f{i}.csv").write_text("a,b\n1,2\n")

        config_content = f"""
dataset_name: test
engine: duckdb
orchestration:
  batch_size: 5
tables:
  - name: my_table
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config_content)
        return config_path

    def test_batch_size_override(self, config_with_override):
        """Constructor batch_size overrides config."""
        from fairway.batch_processor import BatchProcessor
        bp = BatchProcessor(
            str(config_with_override),
            "my_table",
            batch_size=2  # Override config's 5
        )
        assert bp.batch_size == 2
        assert bp.get_batch_count() == 5  # 10 files / 2 = 5


class TestBatchProcessorWorkDir:
    """Test work directory management."""

    @pytest.fixture
    def processor_with_workdir(self, tmp_path):
        """Processor with configured work_dir."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        work_dir = tmp_path / ".fairway" / "work"
        config_content = f"""
dataset_name: test
engine: duckdb
orchestration:
  batch_size: 1
  work_dir: {work_dir}
tables:
  - name: tbl
    path: {data_dir}/*.csv
    format: csv
"""
        config_path = tmp_path / "fairway.yaml"
        config_path.write_text(config_content)

        from fairway.batch_processor import BatchProcessor
        return BatchProcessor(str(config_path), "tbl")

    def test_work_dir_from_config(self, processor_with_workdir, tmp_path):
        """work_dir is read from orchestration config."""
        expected = tmp_path / ".fairway" / "work"
        assert processor_with_workdir.work_dir == str(expected)

    def test_get_batch_dir(self, processor_with_workdir, tmp_path):
        """get_batch_dir returns correct path for batch."""
        batch_dir = processor_with_workdir.get_batch_dir(0)
        expected = tmp_path / ".fairway" / "work" / "tbl" / "batch_0"
        assert batch_dir == str(expected)
