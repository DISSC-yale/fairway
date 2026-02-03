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
        """fairway schema-merge --table TABLE merges partial schemas."""
        from fairway.cli import main

        # First create some schema files
        work_dir = tmp_path / ".fairway" / "work" / "test_table"
        work_dir.mkdir(parents=True)

        # Create partial schema files
        for i in range(4):
            schema_file = work_dir / f"schema_{i}.json"
            schema_file.write_text('{"id": "INTEGER", "value": "VARCHAR"}')

        result = cli_runner.invoke(main, [
            'schema-merge', '--config', str(batch_config),
            '--table', 'test_table'
        ])

        assert result.exit_code == 0


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

    def test_finalize(self, cli_runner, batch_config):
        """fairway finalize --table TABLE finalizes processing."""
        from fairway.cli import main

        result = cli_runner.invoke(main, [
            'finalize', '--config', str(batch_config),
            '--table', 'test_table'
        ])

        # Should succeed or indicate no work to finalize
        assert result.exit_code == 0
