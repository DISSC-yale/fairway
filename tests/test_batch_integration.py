"""Integration tests for batch orchestration with real engines.

These tests run the full batch pipeline on test data using DuckDB and PySpark.
"""
import pytest
import os
import json
import pandas as pd
from pathlib import Path


class TestBatchIntegrationDuckDB:
    """Integration tests using DuckDB engine."""

    @pytest.fixture
    def test_project(self, tmp_path):
        """Create a complete test project with data files."""
        # Create data directory with multiple CSV files
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)

        # Create 10 test files with different data
        for i in range(10):
            df = pd.DataFrame({
                'id': range(i * 100, (i + 1) * 100),
                'name': [f'name_{j}' for j in range(i * 100, (i + 1) * 100)],
                'value': [float(j) * 1.5 for j in range(i * 100, (i + 1) * 100)],
            })
            df.to_csv(data_dir / f'data_{i:03d}.csv', index=False)

        # Create work directory
        work_dir = tmp_path / ".fairway" / "work"
        work_dir.mkdir(parents=True)

        # Create output directory
        output_dir = tmp_path / "data" / "final"
        output_dir.mkdir(parents=True)

        # Create config
        config_content = f"""
dataset_name: test_batch_integration
engine: duckdb

orchestration:
  batch_size: 3
  work_dir: {work_dir}

storage:
  raw_dir: {data_dir}
  intermediate_dir: {tmp_path}/data/intermediate
  final_dir: {output_dir}

tables:
  - name: test_data
    path: {data_dir}/*.csv
    format: csv
"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "fairway.yaml"
        config_path.write_text(config_content)

        return {
            'root': tmp_path,
            'config': config_path,
            'data_dir': data_dir,
            'work_dir': work_dir,
            'output_dir': output_dir,
        }

    def test_batch_processor_with_real_files(self, test_project):
        """BatchProcessor correctly discovers and batches real files."""
        from fairway.batch_processor import BatchProcessor

        bp = BatchProcessor(str(test_project['config']), 'test_data')

        # Should find 10 files
        assert bp.get_file_count() == 10

        # With batch_size=3, should have 4 batches (ceil(10/3))
        assert bp.get_batch_count() == 4

        # Verify batch contents
        batch_0 = bp.get_files_for_batch(0)
        assert len(batch_0) == 3

        batch_3 = bp.get_files_for_batch(3)
        assert len(batch_3) == 1  # Last batch has remainder

    def test_schema_scan_with_duckdb(self, test_project):
        """Schema scan actually reads files and infers schema."""
        from fairway.batch_processor import BatchProcessor
        from fairway.engines.duckdb_engine import DuckDBEngine

        bp = BatchProcessor(str(test_project['config']), 'test_data')
        engine = DuckDBEngine()

        # Get files for batch 0
        batch_files = bp.get_files_for_batch(0)
        assert len(batch_files) > 0

        # Infer schema from first file
        schema = engine.infer_schema(batch_files[0], 'csv')

        # Should have our columns
        assert 'id' in schema
        assert 'name' in schema
        assert 'value' in schema

    def test_full_batch_ingest_duckdb(self, test_project):
        """Full batch ingestion with DuckDB engine."""
        import duckdb
        from fairway.batch_processor import BatchProcessor

        bp = BatchProcessor(str(test_project['config']), 'test_data')

        # Process batch 0
        batch_files = bp.get_files_for_batch(0)
        batch_dir = Path(bp.get_batch_dir(0))
        batch_dir.mkdir(parents=True, exist_ok=True)

        output_path = batch_dir / "ingested.parquet"

        # Use DuckDB to read CSVs and write parquet (no pyarrow needed)
        conn = duckdb.connect()

        # Build UNION ALL query for all batch files
        file_queries = [f"SELECT * FROM read_csv_auto('{f}')" for f in batch_files]
        union_query = " UNION ALL ".join(file_queries)

        # Write to parquet
        conn.execute(f"COPY ({union_query}) TO '{output_path}' (FORMAT PARQUET)")

        # Verify output
        assert output_path.exists()
        result = conn.execute(f"SELECT COUNT(*) FROM '{output_path}'").fetchone()[0]
        assert result == 300  # 3 files * 100 rows each
        conn.close()

    def test_cli_files_command(self, test_project):
        """CLI files command works with real data."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()

        # Test --count
        result = runner.invoke(main, [
            'files', '--config', str(test_project['config']),
            '--table', 'test_data', '--count'
        ])
        assert result.exit_code == 0
        assert '10' in result.output

        # Test --batch
        result = runner.invoke(main, [
            'files', '--config', str(test_project['config']),
            '--table', 'test_data', '--batch', '0'
        ])
        assert result.exit_code == 0
        # Should list 3 files
        lines = [l for l in result.output.strip().split('\n') if l]
        assert len(lines) == 3

    def test_cli_batches_command(self, test_project):
        """CLI batches command calculates correctly."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()

        result = runner.invoke(main, [
            'batches', '--config', str(test_project['config']),
            '--table', 'test_data'
        ])
        assert result.exit_code == 0
        assert '4' in result.output

    def test_cli_schema_scan_creates_file(self, test_project):
        """CLI schema-scan creates schema file."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()

        result = runner.invoke(main, [
            'schema-scan', '--config', str(test_project['config']),
            '--table', 'test_data', '--batch', '0'
        ])
        assert result.exit_code == 0

        # Check schema file was created
        schema_path = test_project['work_dir'] / 'test_data' / 'batch_0' / 'schema_0.json'
        assert schema_path.exists()

        # Verify schema content
        schema = json.loads(schema_path.read_text())
        assert 'id' in schema or len(schema) > 0


@pytest.mark.spark
class TestBatchIntegrationPySpark:
    """Integration tests using PySpark engine."""

    @pytest.fixture
    def test_project_spark(self, tmp_path):
        """Create test project for Spark tests."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)

        # Create test files
        for i in range(6):
            df = pd.DataFrame({
                'id': range(i * 50, (i + 1) * 50),
                'category': [f'cat_{i % 3}' for _ in range(50)],
                'amount': [float(j) for j in range(50)],
            })
            df.to_csv(data_dir / f'spark_data_{i:02d}.csv', index=False)

        work_dir = tmp_path / ".fairway" / "work"
        work_dir.mkdir(parents=True)

        config_content = f"""
dataset_name: test_spark_batch
engine: pyspark

orchestration:
  batch_size: 2
  work_dir: {work_dir}

storage:
  raw_dir: {data_dir}
  intermediate_dir: {tmp_path}/data/intermediate
  final_dir: {tmp_path}/data/final

tables:
  - name: spark_table
    path: {data_dir}/*.csv
    format: csv
"""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "fairway.yaml"
        config_path.write_text(config_content)

        return {
            'root': tmp_path,
            'config': config_path,
            'data_dir': data_dir,
            'work_dir': work_dir,
        }

    def test_batch_processor_spark_config(self, test_project_spark):
        """BatchProcessor works with Spark config."""
        from fairway.batch_processor import BatchProcessor

        bp = BatchProcessor(str(test_project_spark['config']), 'spark_table')

        assert bp.get_file_count() == 6
        assert bp.get_batch_count() == 3  # 6 files / 2 per batch

    def test_spark_schema_inference(self, test_project_spark):
        """PySpark engine can infer schema from batch files."""
        from fairway.batch_processor import BatchProcessor
        from fairway.engines.pyspark_engine import PySparkEngine

        bp = BatchProcessor(str(test_project_spark['config']), 'spark_table')

        engine = PySparkEngine(spark_master="local[2]")
        batch_files = bp.get_files_for_batch(0)
        schema = engine.infer_schema(batch_files[0], 'csv')

        assert 'id' in schema
        assert 'category' in schema
        assert 'amount' in schema

    def test_spark_batch_ingest(self, test_project_spark):
        """PySpark can ingest a batch of files."""
        from fairway.batch_processor import BatchProcessor
        from fairway.engines.pyspark_engine import PySparkEngine
        from pathlib import Path

        bp = BatchProcessor(str(test_project_spark['config']), 'spark_table')

        engine = PySparkEngine(spark_master="local[2]")
        batch_files = bp.get_files_for_batch(0)
        batch_dir = Path(bp.get_batch_dir(0))
        batch_dir.mkdir(parents=True, exist_ok=True)

        output_path = str(batch_dir / "ingested.parquet")

        # Use Spark to read and write
        spark = engine.spark
        df = spark.read.csv(batch_files, header=True, inferSchema=True)
        df.write.mode("overwrite").parquet(output_path)

        # Verify
        result = spark.read.parquet(output_path)
        assert result.count() == 100  # 2 files * 50 rows


class TestPartialStatusIntegration:
    """Integration tests for partial status tracking during batch processing."""

    @pytest.fixture
    def batch_work_dir(self, tmp_path):
        """Create work directory for batch status tests."""
        work_dir = tmp_path / ".fairway" / "work" / "test_table"
        work_dir.mkdir(parents=True)
        return work_dir

    def test_parallel_batch_status_writes(self, batch_work_dir):
        """Simulate parallel batch workers writing status."""
        from fairway.manifest import PartialStatus, StatusMerger
        import time

        # Simulate 3 parallel batches
        for batch in range(3):
            ps = PartialStatus(str(batch_work_dir), batch)
            for i in range(5):
                ps.append(f"batch{batch}_file{i}.csv", "success", f"hash_{batch}_{i}")
            time.sleep(0.01)  # Small delay between batches

        # Merge all statuses
        merger = StatusMerger(str(batch_work_dir))
        merged = merger.merge()

        # Should have 15 total entries (3 batches * 5 files)
        assert len(merged) == 15

        # Verify all files present
        for batch in range(3):
            for i in range(5):
                assert f"batch{batch}_file{i}.csv" in merged

    def test_status_conflict_resolution(self, batch_work_dir):
        """Status merger correctly resolves conflicts."""
        from fairway.manifest import PartialStatus, StatusMerger
        import time

        # Batch 0 writes a file
        ps0 = PartialStatus(str(batch_work_dir), 0)
        ps0.append("shared_file.csv", "success", "old_hash")

        time.sleep(0.02)

        # Batch 1 writes same file (simulating retry/reprocess)
        ps1 = PartialStatus(str(batch_work_dir), 1)
        ps1.append("shared_file.csv", "success", "new_hash")

        merger = StatusMerger(str(batch_work_dir))
        merged = merger.merge()

        # Should have 1 entry with newer hash
        assert len(merged) == 1
        assert merged["shared_file.csv"]["hash"] == "new_hash"

        # Conflicts should be recorded
        conflicts = merger.get_conflicts()
        assert "shared_file.csv" in conflicts
