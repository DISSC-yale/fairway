"""Tests for manifest robustness: corruption, rollback, and cache invalidation."""
import zipfile

import pytest

from fairway.manifest import TableManifest


@pytest.mark.local
def test_corrupted_manifest_falls_back_to_empty(tmp_path):
    """A corrupted manifest JSON must not crash the pipeline — fall back to empty."""
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    corrupt_file = manifest_dir / "mytable.json"
    corrupt_file.write_text("{ invalid json !!!")

    # Must not raise JSONDecodeError
    manifest = TableManifest(str(manifest_dir), "mytable")
    assert manifest.data["table_name"] == "mytable"
    assert manifest.data["files"] == {}


@pytest.mark.local
def test_batch_context_rolls_back_on_exception(tmp_path):
    """Entries added during a failed batch must not be persisted."""
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    manifest = TableManifest(str(manifest_dir), "test_rollback")

    with pytest.raises(RuntimeError):
        with manifest.batch():
            manifest.update_file(
                "/fake/file.csv", status="success",
                metadata={}, table_root="/fake"
            )
            raise RuntimeError("Simulated mid-batch failure")

    # Reload manifest from disk — the file entry must NOT be present
    reloaded = TableManifest(str(manifest_dir), "test_rollback")
    assert reloaded.data["files"] == {}, (
        "Batch partial-commit: entry was persisted despite exception in batch block"
    )


@pytest.mark.local
def test_cache_invalidated_when_action_changes(tmp_path):
    """Changing the preprocessing action must invalidate the cache."""
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    manifest = TableManifest(str(manifest_dir), "cache_test")

    zip_path = tmp_path / "data.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.csv", "id\n1\n")

    cached_path = tmp_path / "old_output"
    cached_path.mkdir()

    # Record a cache entry for action="unzip"
    manifest.record_preprocessing(
        original_path=str(zip_path),
        preprocessed_path=str(cached_path),
        action="unzip",
    )

    # Same file, same action => cache hit
    result = manifest.get_preprocessed_path(str(zip_path), action="unzip")
    assert result == str(cached_path), "Cache should hit for same action"

    # Same file, different action => cache miss
    result = manifest.get_preprocessed_path(str(zip_path), action="my_script.py")
    assert result is None, (
        "Cache hit returned for different preprocessing action"
    )
