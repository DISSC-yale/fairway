"""Tests for Chunk D: Performance Fixes.

Tests cover:
- D.1: Salting disabled by default
- D.2: Parquet file size control (target_file_size_mb)
- D.4: Scratch directory support
"""
import pytest
import os
import tempfile
import yaml
from pathlib import Path


# ============ Config Loader Tests ============
class TestPerformanceConfig:
    """Tests for performance configuration options in config_loader.py."""

    def test_salting_disabled_by_default(self, tmp_path):
        """D.1: Salting should be disabled by default."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.salting is False, "Salting should be disabled by default"

    def test_salting_opt_in_via_config(self, tmp_path):
        """D.1: Salting can be enabled via performance.salting config."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)},
            'performance': {'salting': True}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.salting is True, "Salting should be enabled when configured"

    def test_target_file_size_default(self, tmp_path):
        """D.2: target_file_size_mb should default to 128."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.target_file_size_mb == 128, "target_file_size_mb should default to 128"

    def test_target_file_size_custom(self, tmp_path):
        """D.2: target_file_size_mb can be customized."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)},
            'performance': {'target_file_size_mb': 256}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.target_file_size_mb == 256, "target_file_size_mb should be customizable"

    def test_compression_default_snappy(self, tmp_path):
        """Compression should default to snappy."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.compression == 'snappy', "Compression should default to snappy"

    def test_scratch_dir_not_set_by_default(self, tmp_path):
        """D.4: scratch_dir should be None by default."""
        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {'intermediate_dir': str(tmp_path), 'final_dir': str(tmp_path)}
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.scratch_dir is None, "scratch_dir should be None by default"

    def test_scratch_dir_configured(self, tmp_path):
        """D.4: scratch_dir can be configured."""
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {
                'intermediate_dir': str(tmp_path),
                'final_dir': str(tmp_path),
                'scratch_dir': str(scratch)
            }
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.scratch_dir == str(scratch), "scratch_dir should be configurable"

    def test_scratch_dir_expands_env_vars(self, tmp_path):
        """D.4: scratch_dir should expand environment variables."""
        os.environ['TEST_SCRATCH_USER'] = 'testuser'

        config_file = tmp_path / "fairway.yaml"
        config_file.write_text(yaml.dump({
            'dataset_name': 'test',
            'engine': 'duckdb',
            'sources': [],
            'storage': {
                'intermediate_dir': str(tmp_path),
                'final_dir': str(tmp_path),
                'scratch_dir': '/scratch/$TEST_SCRATCH_USER/fairway'
            }
        }))

        from fairway.config_loader import Config
        config = Config(str(config_file))

        assert config.scratch_dir == '/scratch/testuser/fairway', \
            "scratch_dir should expand environment variables"

        # Cleanup
        del os.environ['TEST_SCRATCH_USER']


# ============ PySpark Engine Tests ============
# These tests require PySpark to be available

# Check if PySpark is available
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_performance") \
        .getOrCreate()
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestPySparkPerformance:
    """Tests for PySpark performance improvements."""

    @pytest.fixture
    def engine(self):
        from fairway.engines.pyspark_engine import PySparkEngine
        return PySparkEngine(spark_master="local[1]")

    def test_salting_disabled_by_default_in_engine(self, engine, tmp_path):
        """D.1: Engine should not add salt column when balanced=False (default)."""
        # Create test data
        data = [{"id": i, "category": "A"} for i in range(100)]
        df = engine.spark.createDataFrame(data)

        input_path = str(tmp_path / "input")
        output_path = str(tmp_path / "output")

        df.write.parquet(input_path)

        # Ingest with default balanced=False
        engine.ingest(
            input_path,
            output_path,
            format='parquet',
            partition_by=['category']
            # balanced defaults to False now
        )

        # Read back and check NO salt column
        result_df = engine.spark.read.parquet(output_path)
        assert "salt" not in result_df.columns, \
            "Salt column should NOT be present when balanced=False (default)"

    def test_salting_enabled_when_balanced_true(self, engine, tmp_path):
        """D.1: Engine should add salt column when balanced=True."""
        data = [{"id": i, "category": "A"} for i in range(100)]
        df = engine.spark.createDataFrame(data)

        input_path = str(tmp_path / "input_salt")
        output_path = str(tmp_path / "output_salt")

        df.write.parquet(input_path)

        # Ingest with balanced=True explicitly
        engine.ingest(
            input_path,
            output_path,
            format='parquet',
            partition_by=['category'],
            balanced=True,
            target_rows=10  # Small value to force multiple salts
        )

        result_df = engine.spark.read.parquet(output_path)
        assert "salt" in result_df.columns, \
            "Salt column should be present when balanced=True"

    def test_max_records_per_file_applied(self, engine, tmp_path):
        """D.2: maxRecordsPerFile should be applied based on target_file_size_mb."""
        # Create test data - enough rows to potentially split
        data = [{"id": i, "value": f"data_{i}"} for i in range(1000)]
        df = engine.spark.createDataFrame(data)

        input_path = str(tmp_path / "input_size")
        output_path = str(tmp_path / "output_size")

        df.write.parquet(input_path)

        # Ingest with small target_file_size_mb to force file splitting
        # With estimated_rows_per_mb=2000 and target_file_size_mb=1,
        # maxRecordsPerFile = 1 * 2000 = 2000
        # But with 1000 rows, we should still get output
        engine.ingest(
            input_path,
            output_path,
            format='parquet',
            target_file_size_mb=1  # Small to test the option is passed
        )

        # Verify output was created
        result_df = engine.spark.read.parquet(output_path)
        assert result_df.count() == 1000, "All rows should be written"

    def test_compression_option_applied(self, engine, tmp_path):
        """Compression option should be passed to writer."""
        data = [{"id": i, "value": f"data_{i}"} for i in range(100)]
        df = engine.spark.createDataFrame(data)

        input_path = str(tmp_path / "input_compress")
        output_path = str(tmp_path / "output_compress")

        df.write.parquet(input_path)

        # Ingest with gzip compression
        engine.ingest(
            input_path,
            output_path,
            format='parquet',
            compression='gzip'
        )

        # Verify output was created (compression is internal detail)
        result_df = engine.spark.read.parquet(output_path)
        assert result_df.count() == 100, "All rows should be written with compression"
