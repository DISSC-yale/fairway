"""Integration tests: fairway.yaml → IngestionPipeline → curated parquet.

Uses simple_project and partitioned_project fixtures from integration/conftest.py.
No mocking of fairway internals.
"""
import json
import pytest
import duckdb

pytest.importorskip("duckdb")


@pytest.mark.local
class TestSimplePipelineRun:

    def test_curated_parquet_exists_after_run(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = simple_project / "data" / "curated" / "people.parquet"
        assert curated.exists(), f"Expected curated output at {curated}"

    def test_curated_parquet_has_correct_row_count(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = str(simple_project / "data" / "curated" / "people.parquet")
        count = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{curated}')").fetchone()[0]
        assert count == 3

    def test_curated_parquet_has_correct_columns(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = str(simple_project / "data" / "curated" / "people.parquet")
        cols = [r[0] for r in duckdb.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{curated}'))"
        ).fetchall()]
        assert set(cols) == {"id", "name", "age"}

    def test_manifest_records_success(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        manifest_file = simple_project / "data" / "manifest" / "people.json"
        assert manifest_file.exists(), "Manifest file not created"
        data = json.loads(manifest_file.read_text())
        statuses = [f["status"] for f in data.get("files", {}).values()]
        assert all(s == "success" for s in statuses), f"Unexpected statuses: {statuses}"

    def test_idempotent_run(self, simple_project, monkeypatch):
        """Running pipeline twice produces identical output (manifest skip)."""
        monkeypatch.chdir(simple_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = str(simple_project / "data" / "curated" / "people.parquet")
        count = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{curated}')").fetchone()[0]
        assert count == 3

    def test_config_paths_derived_from_storage_root(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.config_loader import Config
        config_path = str(simple_project / "config" / "fairway.yaml")
        cfg = Config(config_path)
        assert cfg.curated_dir == str(simple_project / "data" / "curated")


@pytest.mark.local
class TestPartitionedPipelineRun:

    def test_partitioned_output_directories_exist(self, partitioned_project, monkeypatch):
        monkeypatch.chdir(partitioned_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(partitioned_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = partitioned_project / "data" / "curated" / "sales"
        assert curated.is_dir(), f"Expected partition dir at {curated}"
        assert list(curated.rglob("*.parquet")), "No parquet files in partitioned output"

    def test_partitioned_total_row_count(self, partitioned_project, monkeypatch):
        monkeypatch.chdir(partitioned_project)
        from fairway.pipeline import IngestionPipeline
        config_path = str(partitioned_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        curated = str(partitioned_project / "data" / "curated" / "sales")
        count = duckdb.execute(
            f"SELECT COUNT(*) FROM read_parquet('{curated}/**/*.parquet', hive_partitioning=true)"
        ).fetchone()[0]
        assert count == 4
