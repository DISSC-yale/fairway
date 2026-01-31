import pytest
import json
import os
from fairway.manifest import ManifestManager, MANIFEST_VERSION


class TestManifestVersioning:
    def test_new_manifest_has_version(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        assert manifest.manifest.get("version") == MANIFEST_VERSION

    def test_new_manifest_has_required_sections(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        assert "files" in manifest.manifest
        assert "schemas" in manifest.manifest
        assert "preprocessing" in manifest.manifest

    def test_v1_manifest_migration(self, tmp_path):
        # Create v1 manifest (no version field)
        v1_path = tmp_path / "v1.json"
        v1_path.write_text('{"files": {"test/file.csv": {"hash": "abc", "status": "success"}}}')

        manifest = ManifestManager(str(v1_path))
        assert manifest.manifest.get("version") == MANIFEST_VERSION
        assert "test/file.csv" in manifest.manifest["files"]
        assert manifest.manifest["files"]["test/file.csv"]["hash"] == "abc"

    def test_v1_migration_preserves_files(self, tmp_path):
        v1_path = tmp_path / "v1.json"
        v1_data = {
            "files": {
                "src1/a.csv": {"hash": "h1", "status": "success"},
                "src2/b.csv": {"hash": "h2", "status": "failed"}
            }
        }
        v1_path.write_text(json.dumps(v1_data))

        manifest = ManifestManager(str(v1_path))
        assert len(manifest.manifest["files"]) == 2
        assert manifest.manifest["files"]["src1/a.csv"]["hash"] == "h1"


class TestAtomicWrites:
    def test_atomic_write_creates_file(self, tmp_path):
        path = tmp_path / "manifest.json"
        manifest = ManifestManager(str(path))
        manifest.update_manifest("/test/file", status="success", table_name="test")

        assert path.exists()
        content = json.loads(path.read_text())
        assert "files" in content

    def test_atomic_write_no_temp_files_remain(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.update_manifest("/test/file", status="success", table_name="test")

        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert not any(f.suffix == '.tmp' for f in files)

    def test_atomic_write_creates_directory(self, tmp_path):
        nested_path = tmp_path / "nested" / "dir" / "manifest.json"
        manifest = ManifestManager(str(nested_path))
        manifest.update_manifest("/test/file", status="success", table_name="test")

        assert nested_path.exists()


class TestBatchMode:
    def test_batch_defers_saves(self, tmp_path):
        path = tmp_path / "manifest.json"
        manifest = ManifestManager(str(path))

        with manifest.batch():
            manifest.update_manifest("/test/f1", status="success", table_name="t1")
            manifest.update_manifest("/test/f2", status="success", table_name="t2")
            # File should not exist yet or be empty (deferred)
            if path.exists():
                content = json.loads(path.read_text())
                assert len(content.get("files", {})) == 0

        # After batch, should have both entries
        content = json.loads(path.read_text())
        assert len(content["files"]) == 2

    def test_batch_saves_on_exit(self, tmp_path):
        path = tmp_path / "manifest.json"
        manifest = ManifestManager(str(path))

        with manifest.batch():
            manifest.update_manifest("/test/file", status="success", table_name="test")

        assert path.exists()
        content = json.loads(path.read_text())
        assert "test/file" in content["files"]

    def test_batch_saves_even_on_exception(self, tmp_path):
        path = tmp_path / "manifest.json"
        manifest = ManifestManager(str(path))

        try:
            with manifest.batch():
                manifest.update_manifest("/test/file", status="success", table_name="test")
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Should still save despite exception
        assert path.exists()
        content = json.loads(path.read_text())
        assert "test/file" in content["files"]


class TestSchemaTracking:
    def test_record_schema_run(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.record_schema_run(
            dataset_name="test_dataset",
            tables_info=[{"name": "src1", "files_used": ["/a.csv"], "file_hashes": ["hash1"]}],
            output_path="/schemas/test.yaml"
        )

        schema = manifest.get_latest_schema_run("test_dataset")
        assert schema is not None
        assert schema["output_path"] == "/schemas/test.yaml"
        assert "generated_at" in schema
        assert "tables_hash" in schema

    def test_schema_staleness_new_dataset(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        tables_info = [{"name": "src1", "files_used": ["/a.csv"], "file_hashes": ["hash1"]}]

        # New dataset should be stale
        assert manifest.is_schema_stale("new_dataset", tables_info) is True

    def test_schema_staleness_unchanged(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        tables_info = [{"name": "src1", "files_used": ["/a.csv"], "file_hashes": ["hash1"]}]

        manifest.record_schema_run("test_dataset", tables_info, "/schema.yaml")

        # Same sources should not be stale
        assert manifest.is_schema_stale("test_dataset", tables_info) is False

    def test_schema_staleness_changed(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        tables_info = [{"name": "src1", "files_used": ["/a.csv"], "file_hashes": ["hash1"]}]

        manifest.record_schema_run("test_dataset", tables_info, "/schema.yaml")

        # Changed hash should be stale
        new_tables_info = [{"name": "src1", "files_used": ["/a.csv"], "file_hashes": ["hash2"]}]
        assert manifest.is_schema_stale("test_dataset", new_tables_info) is True

    def test_tables_hash_computation(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))

        sources1 = [{"name": "a", "file_hashes": ["h1", "h2"]}]
        sources2 = [{"name": "a", "file_hashes": ["h2", "h1"]}]  # Same hashes, different order
        sources3 = [{"name": "a", "file_hashes": ["h1", "h3"]}]  # Different hash

        hash1 = manifest._compute_tables_hash(sources1)
        hash2 = manifest._compute_tables_hash(sources2)
        hash3 = manifest._compute_tables_hash(sources3)

        # Same hashes in different order should produce same result (sorted)
        assert hash1 == hash2
        # Different hashes should produce different result
        assert hash1 != hash3


class TestPreprocessingCache:
    def test_record_preprocessing(self, tmp_path):
        # Create a test file
        test_file = tmp_path / "source.zip"
        test_file.write_text("test content")
        preprocessed = tmp_path / "extracted"
        preprocessed.mkdir()

        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.record_preprocessing(
            str(test_file), str(preprocessed), "unzip", "test_source"
        )

        assert "preprocessing" in manifest.manifest
        assert len(manifest.manifest["preprocessing"]) == 1

    def test_preprocessing_cache_hit(self, tmp_path):
        # Create a test file
        test_file = tmp_path / "source.zip"
        test_file.write_text("test content")
        preprocessed = tmp_path / "extracted"
        preprocessed.mkdir()

        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.record_preprocessing(
            str(test_file), str(preprocessed), "unzip", "test_source"
        )

        cached = manifest.get_preprocessed_path(str(test_file), "test_source")
        assert cached == str(preprocessed)

    def test_preprocessing_cache_miss_file_changed(self, tmp_path):
        # Create a test file
        test_file = tmp_path / "source.zip"
        test_file.write_text("test content")
        preprocessed = tmp_path / "extracted"
        preprocessed.mkdir()

        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.record_preprocessing(
            str(test_file), str(preprocessed), "unzip", "test_source"
        )

        # Modify the source file
        test_file.write_text("modified content")

        cached = manifest.get_preprocessed_path(str(test_file), "test_source")
        assert cached is None

    def test_preprocessing_cache_miss_output_deleted(self, tmp_path):
        # Create a test file
        test_file = tmp_path / "source.zip"
        test_file.write_text("test content")
        preprocessed = tmp_path / "extracted"
        preprocessed.mkdir()

        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        manifest.record_preprocessing(
            str(test_file), str(preprocessed), "unzip", "test_source"
        )

        # Delete the preprocessed directory
        preprocessed.rmdir()

        cached = manifest.get_preprocessed_path(str(test_file), "test_source")
        assert cached is None

    def test_preprocessing_cache_not_found(self, tmp_path):
        manifest = ManifestManager(str(tmp_path / "manifest.json"))
        cached = manifest.get_preprocessed_path("/nonexistent/file.zip", "test_source")
        assert cached is None


class TestManifestPersistence:
    def test_manifest_persists_across_instances(self, tmp_path):
        path = str(tmp_path / "manifest.json")

        # First instance
        m1 = ManifestManager(path)
        m1.update_manifest("/test/file", status="success", table_name="test")

        # Second instance should load the saved data
        m2 = ManifestManager(path)
        assert "test/file" in m2.manifest["files"]

    def test_schema_persists_across_instances(self, tmp_path):
        path = str(tmp_path / "manifest.json")

        m1 = ManifestManager(path)
        m1.record_schema_run(
            "test_dataset",
            [{"name": "src", "files_used": [], "file_hashes": []}],
            "/schema.yaml"
        )

        m2 = ManifestManager(path)
        schema = m2.get_latest_schema_run("test_dataset")
        assert schema is not None
        assert schema["output_path"] == "/schema.yaml"
