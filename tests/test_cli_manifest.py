"""Tests for CLI manifest commands."""
import pytest
import json
import os
from click.testing import CliRunner
from fairway.cli import main
from fairway.manifest import ManifestStore


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def setup_manifest(tmp_path, monkeypatch):
    """Create a test manifest with sample data."""
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()

    # Change to tmp_path so manifest is found
    monkeypatch.chdir(tmp_path)

    store = ManifestStore(str(manifest_dir))

    # Create test files
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    f1 = data_dir / "CT_2023_01.csv"
    f2 = data_dir / "CT_2023_02.csv"
    f3 = data_dir / "NY_2024_01.csv"
    f1.write_text("a")
    f2.write_text("b")
    f3.write_text("c")

    # Populate manifest for 'claims' table
    tm = store.get_table_manifest("claims")
    tm.update_file(str(f1), status="success", batch_id="batch_001",
                   metadata={"partition": "state=CT/year=2023"})
    tm.update_file(str(f2), status="success", batch_id="batch_001",
                   metadata={"partition": "state=CT/year=2023"})
    tm.update_file(str(f3), status="failed", batch_id="batch_002",
                   metadata={"partition": "state=NY/year=2024", "error": "parse_error"})

    # Create another table manifest
    tm2 = store.get_table_manifest("transactions")
    f4 = data_dir / "txn_001.csv"
    f4.write_text("d")
    tm2.update_file(str(f4), status="success", batch_id="batch_txn_001")

    return store


class TestManifestList:
    """Tests for 'fairway manifest list' command."""

    def test_manifest_list_shows_tables(self, runner, setup_manifest):
        """List command should show all tables with manifests."""
        result = runner.invoke(main, ['manifest', 'list'])
        assert result.exit_code == 0
        assert 'claims' in result.output
        assert 'transactions' in result.output

    def test_manifest_list_empty_manifest(self, runner, tmp_path, monkeypatch):
        """List command with empty manifest should show appropriate message."""
        manifest_dir = tmp_path / "manifest"
        manifest_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(main, ['manifest', 'list'])
        assert result.exit_code == 0
        assert 'No tables' in result.output or 'empty' in result.output.lower()


class TestManifestQuery:
    """Tests for 'fairway manifest query' command."""

    def test_query_single_file(self, runner, setup_manifest, tmp_path):
        """Query single file should return its entry."""
        # Query using relative path key
        result = runner.invoke(main, ['manifest', 'query', '--table', 'claims', '--file', 'CT_2023_01.csv'])
        assert result.exit_code == 0
        assert 'success' in result.output.lower()

    def test_query_by_status(self, runner, setup_manifest):
        """Query by status should filter results."""
        result = runner.invoke(main, ['manifest', 'query', '--table', 'claims', '--status', 'failed'])
        assert result.exit_code == 0
        assert 'NY_2024_01.csv' in result.output
        # Success files should not be in output
        assert 'CT_2023_01.csv' not in result.output

    def test_query_by_batch_id(self, runner, setup_manifest):
        """Query by batch_id should filter results."""
        result = runner.invoke(main, ['manifest', 'query', '--table', 'claims', '--batch-id', 'batch_001'])
        assert result.exit_code == 0
        assert 'CT_2023_01.csv' in result.output
        assert 'CT_2023_02.csv' in result.output
        assert 'NY_2024_01.csv' not in result.output

    def test_query_json_output(self, runner, setup_manifest):
        """Query with --json should output valid JSON."""
        result = runner.invoke(main, ['manifest', 'query', '--table', 'claims', '--json'])
        assert result.exit_code == 0

        # Output should be valid JSON
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 3  # 3 files in claims table

    def test_query_nonexistent_table(self, runner, setup_manifest):
        """Query for non-existent table should show error."""
        result = runner.invoke(main, ['manifest', 'query', '--table', 'nonexistent'])
        # Should either exit with error or show "no files found"
        assert 'not found' in result.output.lower() or 'no files' in result.output.lower() or result.exit_code != 0

    def test_query_combined_filters(self, runner, setup_manifest):
        """Query with multiple filters should AND them."""
        result = runner.invoke(main, [
            'manifest', 'query',
            '--table', 'claims',
            '--status', 'success',
            '--batch-id', 'batch_001'
        ])
        assert result.exit_code == 0
        assert 'CT_2023_01.csv' in result.output
        assert 'CT_2023_02.csv' in result.output
        # Failed file should not appear
        assert 'NY_2024_01.csv' not in result.output

    def test_query_all_files_in_table(self, runner, setup_manifest):
        """Query without filters should show all files."""
        result = runner.invoke(main, ['manifest', 'query', '--table', 'claims'])
        assert result.exit_code == 0
        assert 'CT_2023_01.csv' in result.output
        assert 'CT_2023_02.csv' in result.output
        assert 'NY_2024_01.csv' in result.output


class TestManifestSummary:
    """Tests for manifest summary information."""

    def test_list_shows_file_counts(self, runner, setup_manifest):
        """List should show number of files per table."""
        result = runner.invoke(main, ['manifest', 'list'])
        assert result.exit_code == 0
        # Should show file counts
        assert '3' in result.output  # claims has 3 files
        assert '1' in result.output  # transactions has 1 file
