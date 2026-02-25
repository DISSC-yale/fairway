import pytest
import json
import os
from fairway.manifest import (
    TableManifest, GlobalManifest, ManifestStore,
    TABLE_MANIFEST_VERSION, GLOBAL_MANIFEST_VERSION
)


class TestTableManifest:
    def test_create_new_table_manifest(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        tm = TableManifest(manifest_dir, "sales")
        assert tm.data["version"] == TABLE_MANIFEST_VERSION
        assert tm.data["table_name"] == "sales"
        assert tm.data["files"] == {}

    def test_table_manifest_file_tracking(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")

        tm = TableManifest(manifest_dir, "sales")
        assert tm.should_process(str(test_file)) is True

        tm.update_file(str(test_file), status="success", metadata={"rows": 1})
        assert tm.should_process(str(test_file)) is False

        # Modify file
        test_file.write_text("col1,col2\n1,2\n3,4\n")
        assert tm.should_process(str(test_file)) is True

    def test_table_manifest_persistence(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        test_file = tmp_path / "data.csv"
        test_file.write_text("test")

        tm1 = TableManifest(manifest_dir, "sales")
        tm1.update_file(str(test_file), status="success")

        tm2 = TableManifest(manifest_dir, "sales")
        assert tm2.should_process(str(test_file)) is False

    def test_table_manifest_batch_mode(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "f1.csv"
        f2 = tmp_path / "f2.csv"
        f1.write_text("a")
        f2.write_text("b")

        tm = TableManifest(manifest_dir, "sales")
        with tm.batch():
            tm.update_file(str(f1), status="success")
            tm.update_file(str(f2), status="success")

        assert len(tm.data["files"]) == 2

    def test_table_manifest_preprocessing_cache(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        source = tmp_path / "archive.zip"
        extracted = tmp_path / "extracted"
        source.write_text("zip content")
        extracted.mkdir()

        tm = TableManifest(manifest_dir, "sales")
        tm.record_preprocessing(str(source), str(extracted), "unzip")

        cached = tm.get_preprocessed_path(str(source))
        assert cached == str(extracted)

    def test_table_manifest_schema_tracking(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        tm = TableManifest(manifest_dir, "sales")

        assert tm.is_schema_stale(["hash1", "hash2"]) is True

        tm.record_schema(["file1.csv", "file2.csv"], ["hash1", "hash2"], "/schema/sales.yaml")
        assert tm.is_schema_stale(["hash1", "hash2"]) is False
        assert tm.is_schema_stale(["hash1", "hash3"]) is True


    def test_get_pending_files_all_new(self, tmp_path):
        """All files are new → returns all."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("data1")
        f2.write_text("data2")

        tm = TableManifest(manifest_dir, "sales")
        pending = tm.get_pending_files([str(f1), str(f2)])
        assert len(pending) == 2

    def test_get_pending_files_some_processed(self, tmp_path):
        """Some already processed → returns only new/changed."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("data1")
        f2.write_text("data2")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")

        pending = tm.get_pending_files([str(f1), str(f2)])
        assert pending == [str(f2)]

    def test_get_pending_files_all_processed(self, tmp_path):
        """All already processed → returns empty list."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("data1")
        f2.write_text("data2")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")
        tm.update_file(str(f2), status="success")

        pending = tm.get_pending_files([str(f1), str(f2)])
        assert pending == []

    def test_get_pending_files_modified_file(self, tmp_path):
        """Modified file detected as pending."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f1.write_text("original")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")

        # Modify the file
        f1.write_text("modified content")
        pending = tm.get_pending_files([str(f1)])
        assert pending == [str(f1)]

    def test_get_pending_files_empty_list(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        tm = TableManifest(manifest_dir, "sales")
        assert tm.get_pending_files([]) == []

    def test_should_process_retries_failed_files(self, tmp_path):
        """Files with status='failed' should be retried."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f1.write_text("data1")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="failed")

        # Should still need processing even though hash matches
        assert tm.should_process(str(f1)) is True

    def test_get_pending_files_includes_failed(self, tmp_path):
        """get_pending_files returns previously failed files."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("data1")
        f2.write_text("data2")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")
        tm.update_file(str(f2), status="failed")

        pending = tm.get_pending_files([str(f1), str(f2)])
        assert pending == [str(f2)]

    def test_get_pending_files_with_table_root(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        root = tmp_path / "data"
        root.mkdir()
        f1 = root / "a.csv"
        f1.write_text("data1")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", table_root=str(root))

        pending = tm.get_pending_files([str(f1)], table_root=str(root))
        assert pending == []


class TestGlobalManifest:
    def test_create_new_global_manifest(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        gm = GlobalManifest(manifest_dir)
        assert gm.data["version"] == GLOBAL_MANIFEST_VERSION
        assert gm.data["extractions"] == {}

    def test_global_manifest_extraction_tracking(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        archive = tmp_path / "data.zip"
        extracted = tmp_path / "extracted"
        archive.write_text("zip")
        extracted.mkdir()

        gm = GlobalManifest(manifest_dir)
        archive_hash = f"mtime:{archive.stat().st_mtime}_size:{archive.stat().st_size}"
        gm.record_extraction(str(archive), str(extracted), archive_hash, "sales")

        assert gm.is_extraction_valid(str(archive)) is True
        entry = gm.get_extraction(str(archive))
        assert "sales" in entry["used_by_tables"]

    def test_global_manifest_tracks_multiple_tables(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        archive = tmp_path / "shared.zip"
        extracted = tmp_path / "extracted"
        archive.write_text("zip")
        extracted.mkdir()

        gm = GlobalManifest(manifest_dir)
        archive_hash = f"mtime:{archive.stat().st_mtime}_size:{archive.stat().st_size}"
        gm.record_extraction(str(archive), str(extracted), archive_hash, "sales")
        gm.record_extraction(str(archive), str(extracted), archive_hash, "customers")

        entry = gm.get_extraction(str(archive))
        assert "sales" in entry["used_by_tables"]
        assert "customers" in entry["used_by_tables"]


class TestTableManifestQuery:
    """Tests for query methods on TableManifest."""

    def test_query_file_returns_entry(self, tmp_path):
        """query_file() should return file entry dict for existing file."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "data.csv"
        f1.write_text("col1,col2\n1,2\n")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", metadata={"rows": 100})

        result = tm.query_file("data.csv")
        assert result is not None
        assert result["status"] == "success"
        assert result["metadata"]["rows"] == 100

    def test_query_file_returns_none_for_missing(self, tmp_path):
        """query_file() should return None for non-existent file."""
        manifest_dir = str(tmp_path / "manifest")
        tm = TableManifest(manifest_dir, "sales")

        result = tm.query_file("nonexistent.csv")
        assert result is None

    def test_query_files_with_status_filter(self, tmp_path):
        """query_files(status=...) should filter by status."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f3 = tmp_path / "c.csv"
        f1.write_text("a")
        f2.write_text("b")
        f3.write_text("c")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")
        tm.update_file(str(f2), status="failed")
        tm.update_file(str(f3), status="success")

        success_files = tm.query_files(status="success")
        assert len(success_files) == 2

        failed_files = tm.query_files(status="failed")
        assert len(failed_files) == 1
        assert failed_files[0]["file_key"] == "b.csv"

    def test_query_files_with_batch_id_filter(self, tmp_path):
        """query_files(batch_id=...) should filter by batch_id in metadata."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f3 = tmp_path / "c.csv"
        f1.write_text("a")
        f2.write_text("b")
        f3.write_text("c")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", batch_id="batch_001")
        tm.update_file(str(f2), status="success", batch_id="batch_002")
        tm.update_file(str(f3), status="success", batch_id="batch_001")

        batch_001_files = tm.query_files(batch_id="batch_001")
        assert len(batch_001_files) == 2

        batch_002_files = tm.query_files(batch_id="batch_002")
        assert len(batch_002_files) == 1

    def test_query_files_all_returns_all(self, tmp_path):
        """query_files() with no filters returns all entries."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("a")
        f2.write_text("b")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success")
        tm.update_file(str(f2), status="failed")

        all_files = tm.query_files()
        assert len(all_files) == 2

    def test_query_files_combined_filters(self, tmp_path):
        """query_files() with multiple filters ANDs them together."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f3 = tmp_path / "c.csv"
        f1.write_text("a")
        f2.write_text("b")
        f3.write_text("c")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", batch_id="batch_001")
        tm.update_file(str(f2), status="failed", batch_id="batch_001")
        tm.update_file(str(f3), status="success", batch_id="batch_002")

        # success AND batch_001
        result = tm.query_files(status="success", batch_id="batch_001")
        assert len(result) == 1
        assert result[0]["file_key"] == "a.csv"

    def test_update_file_with_batch_id(self, tmp_path):
        """update_file() with batch_id stores it in metadata."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f1.write_text("a")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", batch_id="batch_001")

        entry = tm.query_file("a.csv")
        assert entry["metadata"]["batch_id"] == "batch_001"

    def test_update_file_batch_id_merged_with_metadata(self, tmp_path):
        """batch_id should be merged with other metadata."""
        manifest_dir = str(tmp_path / "manifest")
        f1 = tmp_path / "a.csv"
        f1.write_text("a")

        tm = TableManifest(manifest_dir, "sales")
        tm.update_file(str(f1), status="success", batch_id="batch_001", metadata={"partition": "state=CT"})

        entry = tm.query_file("a.csv")
        assert entry["metadata"]["batch_id"] == "batch_001"
        assert entry["metadata"]["partition"] == "state=CT"


class TestPreprocessedFileKeyCollision:
    """Tests for manifest key uniqueness when preprocessed files share basenames.

    Bug: When multiple zips are extracted to batch_dir/<zip_stem>/, files with
    the same basename (e.g. us1950b_usa_res.dat) get the same manifest key
    because get_file_key() falls back to os.path.basename() when files aren't
    under table_root. The second entry overwrites the first.
    """

    def test_same_basename_different_dirs_without_root_collides(self, tmp_path):
        """Demonstrates the bug: same basename → same key without proper root."""
        manifest_dir = str(tmp_path / "manifest")
        batch_dir = tmp_path / "batch_v1"
        dir_a = batch_dir / "zip_a"
        dir_b = batch_dir / "zip_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        f1 = dir_a / "data.dat"
        f2 = dir_b / "data.dat"
        f1.write_text("content_a")
        f2.write_text("content_b")

        tm = TableManifest(manifest_dir, "census")
        # Without a matching root, both get key "data.dat"
        key1 = tm.get_file_key(str(f1))
        key2 = tm.get_file_key(str(f2))
        assert key1 == key2 == "data.dat"  # This IS the bug

    def test_same_basename_different_dirs_with_batch_root_unique(self, tmp_path):
        """Fix: using batch_dir as table_root produces unique keys."""
        manifest_dir = str(tmp_path / "manifest")
        batch_dir = tmp_path / "batch_v1"
        dir_a = batch_dir / "zip_a"
        dir_b = batch_dir / "zip_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        f1 = dir_a / "data.dat"
        f2 = dir_b / "data.dat"
        f1.write_text("content_a")
        f2.write_text("content_b")

        tm = TableManifest(manifest_dir, "census")
        key1 = tm.get_file_key(str(f1), table_root=str(batch_dir))
        key2 = tm.get_file_key(str(f2), table_root=str(batch_dir))

        assert key1 != key2
        assert key1 == "zip_a/data.dat"
        assert key2 == "zip_b/data.dat"

    def test_update_file_no_collision_with_batch_root(self, tmp_path):
        """Both files recorded as separate entries when batch_dir used as root."""
        manifest_dir = str(tmp_path / "manifest")
        batch_dir = tmp_path / "batch_v1"
        dir_a = batch_dir / "zip_a"
        dir_b = batch_dir / "zip_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        f1 = dir_a / "data.dat"
        f2 = dir_b / "data.dat"
        f1.write_text("content_a")
        f2.write_text("content_b")

        tm = TableManifest(manifest_dir, "census")
        tm.update_file(str(f1), status="success", table_root=str(batch_dir))
        tm.update_file(str(f2), status="success", table_root=str(batch_dir))

        assert len(tm.data["files"]) == 2
        assert "zip_a/data.dat" in tm.data["files"]
        assert "zip_b/data.dat" in tm.data["files"]

    def test_get_pending_files_no_false_dedup_with_batch_root(self, tmp_path):
        """get_pending_files returns both files when batch_dir used as root."""
        manifest_dir = str(tmp_path / "manifest")
        batch_dir = tmp_path / "batch_v1"
        dir_a = batch_dir / "zip_a"
        dir_b = batch_dir / "zip_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        f1 = dir_a / "data.dat"
        f2 = dir_b / "data.dat"
        f1.write_text("content_a")
        f2.write_text("content_b")

        tm = TableManifest(manifest_dir, "census")
        # Record only f1 as processed
        tm.update_file(str(f1), status="success", table_root=str(batch_dir))

        # f2 should still be pending (different key)
        pending = tm.get_pending_files([str(f1), str(f2)], table_root=str(batch_dir))
        assert pending == [str(f2)]

    def test_should_process_distinguishes_same_basename_files(self, tmp_path):
        """should_process correctly distinguishes files with same basename."""
        manifest_dir = str(tmp_path / "manifest")
        batch_dir = tmp_path / "batch_v1"
        dir_a = batch_dir / "zip_a"
        dir_b = batch_dir / "zip_b"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        f1 = dir_a / "data.dat"
        f2 = dir_b / "data.dat"
        f1.write_text("content_a")
        f2.write_text("content_b")

        tm = TableManifest(manifest_dir, "census")
        tm.update_file(str(f1), status="success", table_root=str(batch_dir))

        assert tm.should_process(str(f1), table_root=str(batch_dir)) is False
        assert tm.should_process(str(f2), table_root=str(batch_dir)) is True


class TestManifestStore:
    def test_manifest_store_creates_table_manifests(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        store = ManifestStore(manifest_dir)

        tm1 = store.get_table_manifest("sales")
        tm2 = store.get_table_manifest("customers")

        assert tm1.table_name == "sales"
        assert tm2.table_name == "customers"
        # Same instance on repeat call
        assert store.get_table_manifest("sales") is tm1

    def test_manifest_store_global_manifest(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        store = ManifestStore(manifest_dir)

        gm = store.global_manifest
        assert isinstance(gm, GlobalManifest)
        assert store.global_manifest is gm  # Same instance

    def test_manifest_store_list_tables(self, tmp_path):
        manifest_dir = str(tmp_path / "manifest")
        store = ManifestStore(manifest_dir)

        # Create some table manifests
        f1 = tmp_path / "f1.csv"
        f1.write_text("a")
        store.get_table_manifest("sales").update_file(str(f1))
        store.get_table_manifest("customers").update_file(str(f1))

        tables = store.list_tables()
        assert "sales" in tables
        assert "customers" in tables
