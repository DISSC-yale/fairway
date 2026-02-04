"""Tests for batch CLI commands - TDD: RED phase."""
import pytest
import os
from click.testing import CliRunner
from pathlib import Path


@pytest.fixture
def cli_runner():
    """Click test runner."""
    return CliRunner()


@pytest.fixture
def batch_config(tmp_path):
    """Create config with test files for batch commands."""
    data_dir = tmp_path / "data" / "raw"
    data_dir.mkdir(parents=True)

    for i in range(10):
        (data_dir / f"file_{i:03d}.csv").write_text(f"id,value\n{i},test{i}\n")

    work_dir = tmp_path / ".fairway" / "work"
    config_content = f"""
dataset_name: test_dataset
engine: duckdb

orchestration:
  batch_size: 3
  work_dir: {work_dir}

tables:
  - name: test_table
    path: {data_dir}/*.csv
    format: csv
"""
    config_path = tmp_path / "config" / "fairway.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(config_content)

    return config_path


class TestFilesCommand:
    """Test fairway files command."""

    def test_files_count(self, cli_runner, batch_config):
        """fairway files --table TABLE --count returns file count."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'files', '--config', str(batch_config),
            '--table', 'test_table', '--count'
        ])

        assert result.exit_code == 0
        assert '10' in result.output

    def test_files_batch(self, cli_runner, batch_config):
        """fairway files --table TABLE --batch N returns batch files."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'files', '--config', str(batch_config),
            '--table', 'test_table', '--batch', '0'
        ])

        assert result.exit_code == 0
        # Batch 0 should have 3 files
        lines = [l for l in result.output.strip().split('\n') if l]
        assert len(lines) == 3

    def test_files_invalid_batch(self, cli_runner, batch_config):
        """fairway files --batch with invalid batch number fails."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'files', '--config', str(batch_config),
            '--table', 'test_table', '--batch', '99'
        ])

        assert result.exit_code != 0


class TestBatchesCommand:
    """Test fairway batches command."""

    def test_batches_count(self, cli_runner, batch_config):
        """fairway batches --table TABLE returns batch count."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'batches', '--config', str(batch_config),
            '--table', 'test_table'
        ])

        assert result.exit_code == 0
        # 10 files / 3 per batch = 4 batches
        assert '4' in result.output

    def test_batches_with_size_override(self, cli_runner, batch_config):
        """fairway batches --size overrides config batch_size."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'batches', '--config', str(batch_config),
            '--table', 'test_table', '--size', '5'
        ])

        assert result.exit_code == 0
        # 10 files / 5 per batch = 2 batches
        assert '2' in result.output


class TestSchemaScanCommand:
    """Test fairway schema-scan command."""

    def test_schema_scan_batch(self, cli_runner, batch_config, tmp_path):
        """fairway schema-scan --table TABLE --batch N scans batch."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'schema-scan', '--config', str(batch_config),
            '--table', 'test_table', '--batch', '0'
        ])

        # Should succeed and create schema file
        assert result.exit_code == 0
        assert 'schema' in result.output.lower() or 'batch' in result.output.lower()


class TestSchemaMergeCommand:
    """Test fairway schema-merge command."""

    def test_schema_merge(self, cli_runner, batch_config, tmp_path):
        """fairway schema-merge --table TABLE merges partial schemas to schema/{table}.yaml."""
        from fairway.cli import main
        import os
        import json
        import yaml

        # First create some schema files in batch subdirectories
        work_dir = tmp_path / ".fairway" / "work" / "test_table"

        # Create partial schema files in batch_{i}/ subdirectories
        for i in range(4):
            batch_dir = work_dir / f"batch_{i}"
            batch_dir.mkdir(parents=True)
            schema_file = batch_dir / f"schema_{i}.json"
            schema_file.write_text('{"id": "INTEGER", "value": "VARCHAR"}')

        # Run from tmp_path so schema/ is created there
        original_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = cli_runner.invoke(main, [
                'schema-merge', '--config', str(batch_config),
                '--table', 'test_table'
            ])

            assert result.exit_code == 0, f"Command failed: {result.output}"

            # Verify schema/{table}.yaml was created
            schema_file = tmp_path / "schema" / "test_table.yaml"
            assert schema_file.exists(), f"Expected {schema_file} to exist"

            # Verify content
            with open(schema_file) as f:
                schema_data = yaml.safe_load(f)
            assert schema_data['name'] == 'test_table'
            assert 'id' in schema_data['schema']
            assert 'value' in schema_data['schema']

            # NEW: Verify manifest was created (Fix #1 regression test)
            manifest_file = tmp_path / "manifest" / "test_table.json"
            assert manifest_file.exists(), f"Expected manifest at {manifest_file}"

            with open(manifest_file) as f:
                manifest_data = json.load(f)

            # Manifest should have schema tracking info
            assert 'schema' in manifest_data, "Manifest missing 'schema' section"
            assert 'output_path' in manifest_data['schema'], "Manifest missing output_path"
            assert 'files_used' in manifest_data['schema'], "Manifest missing files_used"
            assert 'combined_hash' in manifest_data['schema'], "Manifest missing combined_hash"
        finally:
            os.chdir(original_dir)


class TestIngestCommand:
    """Test fairway ingest command."""

    def test_ingest_batch(self, cli_runner, batch_config):
        """fairway ingest --table TABLE --batch N ingests batch."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'ingest', '--config', str(batch_config),
            '--table', 'test_table', '--batch', '0'
        ])

        # Should succeed (may need schema first in practice)
        assert result.exit_code == 0 or 'schema' in result.output.lower()


class TestFinalizeCommand:
    """Test fairway finalize command."""

    def test_finalize_no_batches(self, cli_runner, batch_config):
        """fairway finalize with no completed batches warns but succeeds."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'finalize', '--config', str(batch_config),
            '--table', 'test_table'
        ])

        # Should succeed but warn about no batches
        assert result.exit_code == 0
        assert 'no completed batches' in result.output.lower() or '0/' in result.output

    def test_finalize_with_partitions(self, cli_runner, batch_config, tmp_path):
        """fairway finalize merges partitions to final_dir and updates manifest."""
        from fairway.cli import main
        import json
        import duckdb

        # Create batch directories with actual parquet files
        work_dir = tmp_path / ".fairway" / "work" / "test_table"

        for i in range(3):
            batch_dir = work_dir / f"batch_{i}"
            batch_dir.mkdir(parents=True)

            # Create a small parquet file for each batch
            parquet_path = batch_dir / f"partition_{i}.parquet"
            conn = duckdb.connect()
            conn.execute(f"""
                COPY (SELECT {i} as batch_id, {i * 10} as value)
                TO '{parquet_path}' (FORMAT 'parquet')
            """)
            conn.close()

        # Run finalize
        result = cli_runner.invoke(main, [
            'finalize', '--config', str(batch_config),
            '--table', 'test_table'
        ])

        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Verify output mentions completion
        assert 'finalization complete' in result.output.lower()

        # NEW: Verify final output was created (Fix #2 regression test)
        final_dir = tmp_path / "data" / "final"
        final_file = final_dir / "test_table.parquet"
        assert final_file.exists(), f"Expected final output at {final_file}"

        # Verify the merged parquet has data from all batches
        conn = duckdb.connect()
        result_df = conn.execute(f"SELECT * FROM '{final_file}' ORDER BY batch_id").fetchall()
        conn.close()

        assert len(result_df) == 3, f"Expected 3 rows, got {len(result_df)}"
        assert result_df[0][0] == 0  # batch_id from first batch
        assert result_df[1][0] == 1  # batch_id from second batch
        assert result_df[2][0] == 2  # batch_id from third batch

        # NEW: Verify manifest was updated (Fix #2 regression test)
        manifest_file = tmp_path / "manifest" / "test_table.json"
        assert manifest_file.exists(), f"Expected manifest at {manifest_file}"

        with open(manifest_file) as f:
            manifest_data = json.load(f)

        # Manifest should have file entries with success status
        assert 'files' in manifest_data, "Manifest missing 'files' section"
        # At least some files should be marked as success
        success_count = sum(
            1 for f in manifest_data['files'].values()
            if f.get('status') == 'success'
        )
        assert success_count > 0, "No files marked as success in manifest"
