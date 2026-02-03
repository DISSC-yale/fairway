"""Tests for PartialStatus - append-only manifest for batch concurrency."""
import pytest
import os
import json
import time
from pathlib import Path


class TestPartialStatus:
    """Test PartialStatus append-only status tracking."""

    @pytest.fixture
    def work_dir(self, tmp_path):
        """Create a work directory structure."""
        work = tmp_path / ".fairway" / "work" / "test_table"
        work.mkdir(parents=True)
        return work

    def test_append_status(self, work_dir):
        """PartialStatus appends status entries."""
        from fairway.manifest import PartialStatus

        ps = PartialStatus(str(work_dir), batch=0)
        ps.append("data/file1.csv", "success", "hash123")
        ps.append("data/file2.csv", "success", "hash456")

        # Read status file
        status_path = work_dir / "batch_0" / "status.jsonl"
        assert status_path.exists()

        lines = status_path.read_text().strip().split('\n')
        assert len(lines) == 2

        entry1 = json.loads(lines[0])
        assert entry1['file'] == 'data/file1.csv'
        assert entry1['status'] == 'success'
        assert entry1['hash'] == 'hash123'
        assert 'ts' in entry1

    def test_read_entries(self, work_dir):
        """PartialStatus reads all entries."""
        from fairway.manifest import PartialStatus

        ps = PartialStatus(str(work_dir), batch=0)
        ps.append("file1.csv", "success", "h1")
        ps.append("file2.csv", "failed", "h2")

        entries = ps.read_entries()
        assert len(entries) == 2
        assert entries[0]['file'] == 'file1.csv'
        assert entries[1]['status'] == 'failed'

    def test_multiple_batches(self, work_dir):
        """Different batches have separate status files."""
        from fairway.manifest import PartialStatus

        ps0 = PartialStatus(str(work_dir), batch=0)
        ps1 = PartialStatus(str(work_dir), batch=1)

        ps0.append("file_a.csv", "success", "ha")
        ps1.append("file_b.csv", "success", "hb")

        assert ps0.read_entries()[0]['file'] == 'file_a.csv'
        assert ps1.read_entries()[0]['file'] == 'file_b.csv'


class TestStatusMerger:
    """Test StatusMerger for combining partial status files."""

    @pytest.fixture
    def work_dir_with_statuses(self, tmp_path):
        """Create work dir with multiple batch status files."""
        from fairway.manifest import PartialStatus

        work = tmp_path / ".fairway" / "work" / "test_table"
        work.mkdir(parents=True)

        # Create status files for 3 batches
        for batch in range(3):
            ps = PartialStatus(str(work), batch=batch)
            ps.append(f"batch{batch}_file1.csv", "success", f"hash{batch}1")
            ps.append(f"batch{batch}_file2.csv", "success", f"hash{batch}2")

        return work

    def test_merge_all_batches(self, work_dir_with_statuses):
        """StatusMerger merges all batch status files."""
        from fairway.manifest import StatusMerger

        merger = StatusMerger(str(work_dir_with_statuses))
        merged = merger.merge()

        # Should have 6 entries total (3 batches * 2 files each)
        assert len(merged) == 6

    def test_merge_conflict_resolution(self, tmp_path):
        """StatusMerger resolves conflicts with last-write-wins."""
        from fairway.manifest import PartialStatus, StatusMerger

        work = tmp_path / ".fairway" / "work" / "test_table"
        work.mkdir(parents=True)

        # Batch 0 writes file1 first
        ps0 = PartialStatus(str(work), batch=0)
        ps0.append("shared_file.csv", "success", "hash_old")

        time.sleep(0.01)  # Ensure different timestamp

        # Batch 1 writes same file later
        ps1 = PartialStatus(str(work), batch=1)
        ps1.append("shared_file.csv", "success", "hash_new")

        merger = StatusMerger(str(work))
        merged = merger.merge()

        # Last write wins - should have hash_new
        assert len(merged) == 1
        assert merged['shared_file.csv']['hash'] == 'hash_new'

    def test_merge_logs_conflicts(self, tmp_path):
        """StatusMerger logs conflicts for review."""
        from fairway.manifest import PartialStatus, StatusMerger

        work = tmp_path / ".fairway" / "work" / "test_table"
        work.mkdir(parents=True)

        # Create conflicting entries
        ps0 = PartialStatus(str(work), batch=0)
        ps0.append("conflict.csv", "success", "hash_a")

        time.sleep(0.01)

        ps1 = PartialStatus(str(work), batch=1)
        ps1.append("conflict.csv", "failed", "hash_b")

        merger = StatusMerger(str(work))
        merged = merger.merge()
        conflicts = merger.get_conflicts()

        # Should record the conflict
        assert 'conflict.csv' in conflicts
        assert len(conflicts['conflict.csv']) == 2

    def test_write_merged_manifest(self, work_dir_with_statuses, tmp_path):
        """StatusMerger writes merged manifest file."""
        from fairway.manifest import StatusMerger

        merger = StatusMerger(str(work_dir_with_statuses))
        output_path = tmp_path / "merged_manifest.json"
        merger.write_merged(str(output_path))

        assert output_path.exists()
        data = json.loads(output_path.read_text())
        assert 'files' in data
        assert len(data['files']) == 6
