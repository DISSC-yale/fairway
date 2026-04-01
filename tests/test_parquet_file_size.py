"""
Tests for PySpark parquet file size control.
maxRecordsPerFile controls how many rows land in each output .parquet file.
Tests count actual output files to verify the setting is wired through.
Requires PySpark — all tests skip if PySpark unavailable.
"""
import glob
import pytest
from pathlib import Path

pyspark = pytest.importorskip("pyspark", reason="PySpark not available")


class TestMaxRecordsPerFile:

    def test_explicit_max_records_splits_output(self, pyspark_engine, tmp_path):
        """
        1000 rows with max_records_per_file=100 must produce exactly 10 parquet files.
        Proves maxRecordsPerFile is passed through to the Spark writer.
        """
        data = [{"id": i, "value": float(i)} for i in range(1000)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            max_records_per_file=100,
        )

        parquet_files = glob.glob(str(output / "**" / "*.parquet"), recursive=True)
        assert len(parquet_files) == 10, (
            f"Expected 10 files (1000 rows / 100 per file), got {len(parquet_files)}"
        )

    def test_target_file_size_mb_heuristic(self, pyspark_engine, tmp_path):
        """
        target_file_size_mb=1 → computed max = 1 * 8000 = 8000 rows/file.
        16000 rows must produce exactly 2 parquet files.
        """
        data = [{"id": i, "value": float(i)} for i in range(16_000)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            target_file_size_mb=1,
        )

        parquet_files = glob.glob(str(output / "**" / "*.parquet"), recursive=True)
        assert len(parquet_files) == 2, (
            f"Expected 2 files (16000 rows / 8000 rows-per-MB), got {len(parquet_files)}"
        )

    def test_explicit_max_records_overrides_heuristic(self, pyspark_engine, tmp_path):
        """
        When both max_records_per_file and target_file_size_mb are set,
        explicit max_records_per_file takes priority.
        500 rows / 50 per file = 10 files (not the 1 file target_file_size_mb=100 would give).
        """
        data = [{"id": i} for i in range(500)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            max_records_per_file=50,
            target_file_size_mb=100,
        )

        parquet_files = glob.glob(str(output / "**" / "*.parquet"), recursive=True)
        assert len(parquet_files) == 10, (
            f"Expected 10 files (explicit max_records overrides heuristic), "
            f"got {len(parquet_files)}"
        )

    def test_row_count_preserved_regardless_of_split(self, pyspark_engine, tmp_path):
        """Splitting into multiple files must not lose any rows."""
        data = [{"id": i} for i in range(300)]
        df = pyspark_engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        pyspark_engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            max_records_per_file=100,
        )

        result = pyspark_engine.spark.read.parquet(str(output))
        assert result.count() == 300
