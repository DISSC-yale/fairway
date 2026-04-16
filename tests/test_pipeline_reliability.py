"""P0 pipeline reliability tests.

Tests for three critical fixes:
  1. Spark transformation write bug — partitioned write must call .parquet()
  2. Atomic finalization — curated data survives write failure
  3. Error handling — one table failure must not kill the pipeline
"""
import os
import shutil
import pytest
import yaml
from unittest.mock import patch, MagicMock, PropertyMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_path, tables, engine="duckdb"):
    """Write a minimal fairway config and return its path."""
    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "reliability_test",
        "engine": engine,
        "storage": {"root": str(tmp_path / "data")},
        "tables": tables,
    }))
    return str(config_path)


def _make_csv(path, content="id,name\n1,alice\n2,bob\n"):
    """Write a small CSV file and return its path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    return path


def _make_pipeline(config_path):
    """Create an IngestionPipeline without starting any engine."""
    from fairway.pipeline import IngestionPipeline
    return IngestionPipeline(config_path)


# ---------------------------------------------------------------------------
# Bug 1: Spark transformation write — partitioned .parquet() call
# ---------------------------------------------------------------------------

class TestPartitionedWrite:
    """Pipeline with partition_by must produce Hive-layout partition directories.

    Uses real DuckDB engine. Asserts on physical filesystem output.
    """

    @pytest.mark.local
    def test_partitioned_write_creates_subdirectories(self, tmp_path):
        """When partition_by=['region'], processed output must have region=<value>/ dirs."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)

        csv_path = _make_csv(
            str(data_dir / "sales.csv"),
            "id,amount,region\n1,100,north\n2,200,south\n3,150,north\n",
        )
        config_path = _write_config(tmp_path, [{
            "name": "sales",
            "path": csv_path,
            "format": "csv",
            "partition_by": ["region"],
        }])
        _make_pipeline(config_path).run(skip_summary=True)

        processed_dir = tmp_path / "data" / "processed" / "sales"
        assert processed_dir.exists(), f"No processed dir at {processed_dir}"
        subdirs = {p.name for p in processed_dir.iterdir() if p.is_dir()}
        assert "region=north" in subdirs, f"Missing region=north. Got: {subdirs}"
        assert "region=south" in subdirs, f"Missing region=south. Got: {subdirs}"

    @pytest.mark.local
    def test_non_partitioned_write_produces_single_output(self, tmp_path):
        """Without partition_by, processed output is a single parquet (no subdirs)."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        csv_path = _make_csv(str(data_dir / "items.csv"))
        config_path = _write_config(tmp_path, [{
            "name": "items", "path": csv_path, "format": "csv",
        }])
        _make_pipeline(config_path).run(skip_summary=True)

        processed_dir = tmp_path / "data" / "processed"
        assert processed_dir.exists()
        parquet_files = list(processed_dir.rglob("*.parquet"))
        assert len(parquet_files) >= 1, "No parquet output produced"


# ---------------------------------------------------------------------------
# Bug 2: Atomic finalization — curated data survives write failure
# ---------------------------------------------------------------------------

class TestAtomicFinalization:
    """Finalization must not destroy existing curated data if the
    replacement write fails."""

    @pytest.mark.local
    def test_curated_data_survives_write_failure(self, tmp_path):
        """If enforce_types/copy fails, the original curated file
        must still exist."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        csv_path = _make_csv(str(data_dir / "test.csv"))

        config_path = _write_config(tmp_path, [{
            "name": "survive_test",
            "path": str(csv_path),
            "format": "csv",
        }])

        pipeline = _make_pipeline(config_path)

        # Run once successfully to produce curated output
        pipeline.run(skip_summary=True)

        curated_path = os.path.join(pipeline.config.curated_dir, "survive_test.parquet")
        assert os.path.exists(curated_path), "Curated file should exist after first run"

        # Record original content for comparison
        original_size = os.path.getsize(curated_path)
        assert original_size > 0

        # Now force shutil.copy2 to fail during second run
        with patch("shutil.copy2", side_effect=OSError("Simulated disk full")):
            with patch("shutil.copytree", side_effect=OSError("Simulated disk full")):
                # The pipeline should handle this gracefully
                try:
                    pipeline.run(skip_summary=True)
                except Exception:
                    pass  # We expect it might raise

        # The CRITICAL assertion: original curated data must still exist
        assert os.path.exists(curated_path), (
            "Curated data was destroyed by failed finalization — "
            "non-atomic write pattern lost data!"
        )
        assert os.path.getsize(curated_path) > 0, "Curated file is empty after failed write"

    @pytest.mark.local
    def test_temp_files_cleaned_after_successful_write(self, tmp_path):
        """After a successful finalization, no .tmp_new or .tmp_old files
        should remain."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        csv_path = _make_csv(str(data_dir / "test.csv"))

        config_path = _write_config(tmp_path, [{
            "name": "cleanup_test",
            "path": str(csv_path),
            "format": "csv",
        }])

        pipeline = _make_pipeline(config_path)
        pipeline.run(skip_summary=True)

        curated_dir = pipeline.config.curated_dir
        leftover_temps = [
            f for f in os.listdir(curated_dir)
            if f.endswith(".tmp_new") or f.endswith(".tmp_old")
        ]
        assert leftover_temps == [], f"Temp files not cleaned up: {leftover_temps}"


# ---------------------------------------------------------------------------
# Bug 3: Error handling — one table failure must not kill the pipeline
# ---------------------------------------------------------------------------

class TestProcessingLoopErrorHandling:
    """The per-table processing loop must catch errors and continue
    processing remaining tables."""

    @pytest.mark.local
    def test_pipeline_continues_after_table_failure(self, tmp_path):
        """If one table crashes during ingestion, other tables should
        still be processed successfully."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        good_csv_1 = _make_csv(str(data_dir / "good1.csv"))
        bad_csv = _make_csv(str(data_dir / "bad.csv"))
        good_csv_2 = _make_csv(str(data_dir / "good2.csv"))

        config_path = _write_config(tmp_path, [
            {"name": "table_good_1", "path": good_csv_1, "format": "csv"},
            {"name": "table_bad", "path": bad_csv, "format": "csv"},
            {"name": "table_good_2", "path": good_csv_2, "format": "csv"},
        ])

        pipeline = _make_pipeline(config_path)

        # Make ingestion crash for the bad table only
        real_ingest = pipeline.engine.ingest
        def failing_ingest(input_path, output_path, **kwargs):
            if "bad" in os.path.basename(input_path):
                raise RuntimeError("Simulated ingestion failure")
            return real_ingest(input_path, output_path, **kwargs)

        with pytest.raises(RuntimeError, match="1 failed table"):
            with patch.object(pipeline.engine, "ingest", side_effect=failing_ingest):
                pipeline.run(skip_summary=True)

        # Good tables should have curated output despite the failure
        curated_dir = pipeline.config.curated_dir
        assert os.path.exists(os.path.join(curated_dir, "table_good_1.parquet")), \
            "First good table was not processed"
        assert os.path.exists(os.path.join(curated_dir, "table_good_2.parquet")), \
            "Third good table was skipped after second table failed"

    @pytest.mark.local
    def test_failed_table_logged_in_manifest(self, tmp_path):
        """A table that raises during ingestion should be recorded
        as 'failed' in its manifest."""
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        good_csv = _make_csv(str(data_dir / "good.csv"))
        bad_csv = _make_csv(str(data_dir / "bad.csv"))

        config_path = _write_config(tmp_path, [
            {"name": "good_table", "path": good_csv, "format": "csv"},
            {"name": "bad_table", "path": bad_csv, "format": "csv"},
        ])

        pipeline = _make_pipeline(config_path)

        # Make ingestion crash for the bad table only
        real_ingest = pipeline.engine.ingest
        def failing_ingest(input_path, output_path, **kwargs):
            if "bad" in os.path.basename(input_path):
                raise RuntimeError("Simulated ingestion failure")
            return real_ingest(input_path, output_path, **kwargs)

        with pytest.raises(RuntimeError, match="1 failed table"):
            with patch.object(pipeline.engine, "ingest", side_effect=failing_ingest):
                pipeline.run(skip_summary=True)

        # The bad table's manifest should record failure
        bad_manifest = pipeline.manifest_store.get_table_manifest("bad_table")
        entry = bad_manifest.query_file(os.path.basename(bad_csv))
        assert entry is not None, "Bad table has no manifest entry"
        assert entry["status"] == "failed", f"Expected 'failed', got '{entry['status']}'"

    @pytest.mark.local
    def test_failed_tables_summary_logged(self, tmp_path):
        """After the loop, a summary of failed tables should be logged."""
        import logging
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)
        good_csv = _make_csv(str(data_dir / "good.csv"))
        bad_csv = _make_csv(str(data_dir / "bad.csv"))

        config_path = _write_config(tmp_path, [
            {"name": "good_table", "path": good_csv, "format": "csv"},
            {"name": "bad_table", "path": bad_csv, "format": "csv"},
        ])

        pipeline = _make_pipeline(config_path)

        real_ingest = pipeline.engine.ingest
        def failing_ingest(input_path, output_path, **kwargs):
            if "bad" in os.path.basename(input_path):
                raise RuntimeError("Simulated ingestion failure")
            return real_ingest(input_path, output_path, **kwargs)

        # Use a mock handler to capture log output regardless of logger propagation
        handler = logging.Handler()
        handler.setLevel(logging.ERROR)
        captured = []
        handler.emit = lambda record: captured.append(record)
        target_logger = logging.getLogger("fairway.pipeline")
        target_logger.addHandler(handler)
        try:
            with pytest.raises(RuntimeError, match="1 failed table"):
                with patch.object(pipeline.engine, "ingest", side_effect=failing_ingest):
                    pipeline.run(skip_summary=True)
        finally:
            target_logger.removeHandler(handler)

        # Should see the failed table name in an error log
        failed_msgs = [r for r in captured if "bad_table" in r.getMessage()]
        assert len(failed_msgs) > 0, "No error log mentioning the failed table"
