"""Golden-path end-to-end test: write fairway.yaml → place raw data → run pipeline.

This is the most important test in the suite. It validates the complete user
journey — the fairway.yaml is the single source of truth for all paths.
"""
import yaml
import pytest
import duckdb

pytest.importorskip("duckdb")


@pytest.mark.local
class TestGoldenPathWorkflow:

    def test_full_workflow_from_scratch(self, tmp_path, monkeypatch):
        """Write fairway.yaml → CSV → pipeline.run() → assert parquet correctness."""
        monkeypatch.chdir(tmp_path)

        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (tmp_path / "config").mkdir()

        (raw_dir / "employees.csv").write_text(
            "emp_id,name,department,salary\n"
            "1,Alice,Engineering,95000\n"
            "2,Bob,Marketing,75000\n"
            "3,Carol,Engineering,105000\n"
            "4,Dave,HR,65000\n"
        )

        config = {
            "dataset_name": "hr_data",
            "engine": "duckdb",
            "storage": {"root": str(tmp_path / "data")},
            "tables": [{
                "name": "employees",
                "path": str(raw_dir / "employees.csv"),
                "format": "csv",
                "schema": {
                    "emp_id": "BIGINT",
                    "name": "VARCHAR",
                    "department": "VARCHAR",
                    "salary": "DOUBLE",
                },
            }],
        }
        config_path = tmp_path / "config" / "fairway.yaml"
        config_path.write_text(yaml.dump(config))

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(str(config_path)).run(skip_summary=True)

        curated = tmp_path / "data" / "curated" / "employees.parquet"
        assert curated.exists()

        count, min_sal, max_sal = duckdb.execute(
            f"SELECT COUNT(*), MIN(salary), MAX(salary) FROM read_parquet('{curated}')"
        ).fetchone()
        assert count == 4
        assert min_sal == 65000.0
        assert max_sal == 105000.0

        cols = [r[0] for r in duckdb.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{curated}'))"
        ).fetchall()]
        assert set(cols) == {"emp_id", "name", "department", "salary"}

    def test_storage_root_drives_all_output_paths(self, tmp_path, monkeypatch):
        """storage.root in fairway.yaml is the only definition of output locations."""
        monkeypatch.chdir(tmp_path)

        custom_root = tmp_path / "custom_storage"
        (custom_root / "raw").mkdir(parents=True)
        (custom_root / "raw" / "data.csv").write_text("id,val\n1,100\n2,200\n")
        (tmp_path / "config").mkdir()

        config = {
            "dataset_name": "root_test",
            "engine": "duckdb",
            "storage": {"root": str(custom_root)},
            "tables": [{
                "name": "data",
                "path": str(custom_root / "raw" / "data.csv"),
                "format": "csv",
            }],
        }
        config_path = tmp_path / "config" / "fairway.yaml"
        config_path.write_text(yaml.dump(config))

        from fairway.config import Config
        cfg = Config(str(config_path))
        assert cfg.raw_dir == str(custom_root / "raw")
        assert cfg.processed_dir == str(custom_root / "processed")
        assert cfg.curated_dir == str(custom_root / "curated")

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(str(config_path)).run(skip_summary=True)
        assert (custom_root / "curated" / "data.parquet").exists()
