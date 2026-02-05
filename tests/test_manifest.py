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
