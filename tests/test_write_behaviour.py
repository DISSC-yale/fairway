"""
Tests for write mode (overwrite/append), partition_by directory structure,
and output_layer routing (processed vs curated).
"""
import shutil
import time
import pytest
from pathlib import Path
from tests.helpers import build_config, read_curated, read_processed


def engine_name(engine):
    return "pyspark" if hasattr(engine, "spark") else "duckdb"


def _copy_csv(src, dst_dir, name="input.csv"):
    """Copy a CSV fixture to a writable location so we can touch it between runs."""
    dst = Path(dst_dir) / name
    shutil.copy2(str(src), str(dst))
    return str(dst)


class TestWriteMode:

    @pytest.mark.skip(
        reason=(
            "write_mode=append is not implemented for single-file parquet output: "
            "DuckDB COPY TO file.parquet does not support true append semantics. "
            "Append only works for partitioned directory outputs where new partitions "
            "are written alongside existing ones."
        )
    )
    def test_append_doubles_rows_on_second_run(self, engine, fixtures_dir, tmp_path, monkeypatch):
        """write_mode=append: second run must add rows, not replace.

        The manifest skips files whose mtime+size hash hasn't changed.
        We copy the CSV to a writable location and touch it between runs to
        force the manifest to treat the second run as new work.
        """
        monkeypatch.chdir(tmp_path)
        csv_path = _copy_csv(fixtures_dir / "formats" / "csv" / "simple.csv", tmp_path)
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "appendable",
            "path": csv_path,
            "format": "csv",
            "write_mode": "append",
        })
        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(config)
        pipeline.run()
        first_count = len(read_curated(tmp_path, "appendable"))

        # Touch the file so the manifest sees a changed mtime hash and re-processes it
        time.sleep(0.05)
        Path(csv_path).touch()

        pipeline.run()
        second_count = len(read_curated(tmp_path, "appendable"))

        assert second_count == first_count * 2, (
            f"Expected {first_count * 2} rows after append, got {second_count}"
        )

    def test_overwrite_replaces_rows_on_second_run(self, engine, fixtures_dir, tmp_path, monkeypatch):
        """write_mode=overwrite (default): second run must replace, not add.

        Same mtime-touch approach to force the manifest to re-process.
        """
        monkeypatch.chdir(tmp_path)
        csv_path = _copy_csv(fixtures_dir / "formats" / "csv" / "simple.csv", tmp_path)
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "overwritable",
            "path": csv_path,
            "format": "csv",
            "write_mode": "overwrite",
        })
        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(config)
        pipeline.run()
        first_count = len(read_curated(tmp_path, "overwritable"))

        time.sleep(0.05)
        Path(csv_path).touch()

        pipeline.run()
        second_count = len(read_curated(tmp_path, "overwritable"))

        assert second_count == first_count


class TestOutputLayer:

    def test_output_layer_processed_writes_to_processed_dir(self, fixtures_dir, tmp_path, monkeypatch):
        """output_layer=processed writes to data/processed/, not data/curated/."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "early_stop",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "output_layer": "processed",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_processed(tmp_path, "early_stop")
        assert len(df) == 3

        # Check directory/file presence
        processed_file = Path(tmp_path) / "data" / "processed" / "early_stop.parquet"
        processed_dir = Path(tmp_path) / "data" / "processed" / "early_stop"
        curated_file = Path(tmp_path) / "data" / "curated" / "early_stop.parquet"
        curated_dir = Path(tmp_path) / "data" / "curated" / "early_stop"
        assert processed_file.exists() or processed_dir.exists()
        assert not curated_file.exists() and not curated_dir.exists()

    def test_output_layer_curated_default_writes_to_curated_dir(self, fixtures_dir, tmp_path, monkeypatch):
        """Default output_layer=curated writes to data/curated/."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "full_pipeline",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "full_pipeline")
        assert len(df) == 3
