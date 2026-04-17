"""Tests for separated ingestion and summarization phases."""
import os
import pytest
import shutil
import tempfile
import yaml
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from fairway.pipeline import IngestionPipeline
from fairway.summarize import Summarizer
from fairway.validations.checks import Validator


class TestRunSkipSummary:
    """Tests for run(skip_summary=True) behavior."""

    @pytest.fixture
    def temp_project(self, tmp_path):
        """Create a minimal project structure for testing."""
        # Create directories
        (tmp_path / "data" / "raw").mkdir(parents=True)
        (tmp_path / "data" / "processed").mkdir(parents=True)
        (tmp_path / "data" / "curated").mkdir(parents=True)

        # Create test CSV data
        test_data = tmp_path / "data" / "raw" / "test.csv"
        test_data.write_text("id,name,value\n1,alice,100\n2,bob,200\n")

        # Create config
        config = {
            "dataset_name": "test_dataset",
            "engine": "duckdb",
            "storage": {
                "root": str(tmp_path / "data"),
                "raw": str(tmp_path / "data" / "raw"),
                "processed": str(tmp_path / "data" / "processed"),
                "curated": str(tmp_path / "data" / "curated"),
            },
            "tables": [
                {
                    "name": "test_table",
                    "path": str(test_data),
                    "format": "csv",
                }
            ],
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        return tmp_path, config_path

    def test_skip_summary_no_summary_files(self, temp_project):
        """Test that skip_summary=True produces no summary files."""
        tmp_path, config_path = temp_project

        pipeline = IngestionPipeline(str(config_path))
        pipeline.run(skip_summary=True)

        curated_dir = tmp_path / "data" / "curated"
        # Parquet should exist
        parquet_file = curated_dir / "test_table.parquet"
        assert parquet_file.exists(), f"Parquet not found. Contents: {list(curated_dir.iterdir())}"
        # Summary files should NOT exist
        assert not (curated_dir / "test_table_summary.csv").exists()
        assert not (curated_dir / "test_table_report.md").exists()

    def test_default_run_includes_summary(self, temp_project):
        """Test that default run() produces summary files (backward compat)."""
        tmp_path, config_path = temp_project

        pipeline = IngestionPipeline(str(config_path))
        pipeline.run()

        curated_dir = tmp_path / "data" / "curated"
        # Parquet should exist
        parquet_file = curated_dir / "test_table.parquet"
        assert parquet_file.exists(), f"Parquet not found. Contents: {list(curated_dir.iterdir())}"
        # Summary files should also exist
        assert (curated_dir / "test_table_summary.csv").exists()
        assert (curated_dir / "test_table_report.md").exists()


class TestSummarizeStandalone:
    """Tests for standalone summarize() method."""

    @pytest.fixture
    def temp_project_with_data(self, tmp_path):
        """Create a project with already-ingested data in curated_dir."""
        # Create directories
        (tmp_path / "data" / "raw").mkdir(parents=True)
        (tmp_path / "data" / "processed").mkdir(parents=True)
        (tmp_path / "data" / "curated").mkdir(parents=True)

        # Create test CSV data (source)
        test_data = tmp_path / "data" / "raw" / "test.csv"
        test_data.write_text("id,name,value\n1,alice,100\n2,bob,200\n3,charlie,300\n")

        # Create pre-existing parquet in curated_dir (simulating previous ingestion)
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "value": [100, 200, 300],
        })
        curated_parquet = tmp_path / "data" / "curated" / "test_table.parquet"
        df.to_parquet(curated_parquet, index=False)

        # Create config
        config = {
            "dataset_name": "test_dataset",
            "engine": "duckdb",
            "storage": {
                "root": str(tmp_path / "data"),
                "raw": str(tmp_path / "data" / "raw"),
                "processed": str(tmp_path / "data" / "processed"),
                "curated": str(tmp_path / "data" / "curated"),
            },
            "tables": [
                {
                    "name": "test_table",
                    "path": str(test_data),
                    "format": "csv",
                }
            ],
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        return tmp_path, config_path

    def test_summarize_reads_curated_produces_reports(self, temp_project_with_data):
        """Test that summarize() reads from curated_dir and produces reports."""
        tmp_path, config_path = temp_project_with_data

        pipeline = IngestionPipeline(str(config_path))
        pipeline.summarize()

        curated_dir = tmp_path / "data" / "curated"
        # Summary files should be created
        assert (curated_dir / "test_table_summary.csv").exists()
        assert (curated_dir / "test_table_report.md").exists()

        # Verify summary content
        summary_df = pd.read_csv(curated_dir / "test_table_summary.csv")
        assert "variable" in summary_df.columns

        # Verify markdown report content
        report_content = (curated_dir / "test_table_report.md").read_text()
        assert "test_dataset" in report_content
        assert "Total Rows" in report_content and "3" in report_content

    def test_summarize_missing_data_logs_warning(self, tmp_path):
        """Test that summarize() logs warning when no finalized data exists."""
        # Create directories
        (tmp_path / "data" / "raw").mkdir(parents=True)
        (tmp_path / "data" / "processed").mkdir(parents=True)
        (tmp_path / "data" / "curated").mkdir(parents=True)  # Empty curated dir

        # Create test CSV data (source)
        test_data = tmp_path / "data" / "raw" / "test.csv"
        test_data.write_text("id,name,value\n1,alice,100\n")

        # Create config pointing to non-existent curated data
        config = {
            "dataset_name": "test_dataset",
            "engine": "duckdb",
            "storage": {
                "root": str(tmp_path / "data"),
                "raw": str(tmp_path / "data" / "raw"),
                "processed": str(tmp_path / "data" / "processed"),
                "curated": str(tmp_path / "data" / "curated"),
            },
            "tables": [
                {
                    "name": "missing_table",
                    "path": str(test_data),
                    "format": "csv",
                }
            ],
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        pipeline = IngestionPipeline(str(config_path))

        # Should not raise, just log warning and skip
        with patch("fairway.pipeline.logger") as mock_logger:
            pipeline.summarize()
            # Check that warning was logged about missing data
            warning_calls = [
                call for call in mock_logger.warning.call_args_list
                if "Skipping summary" in str(call) or "no finalized data" in str(call)
            ]
            assert len(warning_calls) > 0


class TestSparkSummaryOptimization:
    """Tests for Spark summary count optimization."""

    @pytest.mark.spark
    def test_generate_summary_spark_returns_count(self, pyspark_engine, tmp_path):
        """Test that generate_summary_spark returns row count from describe()."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame(
            [(1, "a", 10.0), (2, "b", 20.0), (3, "c", 30.0)],
            ["id", "name", "value"]
        )

        output_path = str(tmp_path / "summary.csv")
        summary_df, row_count = Summarizer.generate_summary_spark(df, output_path)

        # Verify row count was extracted correctly
        assert row_count == 3

        # Verify summary file was created
        assert os.path.exists(output_path)
        assert isinstance(summary_df, pd.DataFrame)


class TestSparkValidationOptimization:
    """Tests for Spark validation count optimizations."""

    @pytest.mark.spark
    def test_level1_uses_limit_optimization(self, pyspark_engine):
        """Test that level1_check_spark uses limit() for min_rows check."""
        spark = pyspark_engine.spark

        # Create a DataFrame with 100 rows
        df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
        config = {"level1": {"min_rows": 10}}

        # The optimization uses limit(min_rows).count() instead of full count()
        # We verify by checking that validation passes correctly
        result = Validator.level1_check_spark(df, config)

        assert result["passed"] is True
        assert len(result["errors"]) == 0

    @pytest.mark.spark
    def test_level1_limit_optimization_fails_correctly(self, pyspark_engine):
        """Test that limit optimization correctly fails when insufficient rows."""
        spark = pyspark_engine.spark

        # Create a DataFrame with only 5 rows
        df = spark.createDataFrame([(i,) for i in range(5)], ["id"])
        config = {"level1": {"min_rows": 10}}

        result = Validator.level1_check_spark(df, config)

        assert result["passed"] is False
        assert len(result["errors"]) == 1
        assert "fewer than" in result["errors"][0]

    @pytest.mark.spark
    def test_level2_logs_null_checks(self, pyspark_engine):
        """Test that level2_check_spark logs null check progress."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
        config = {"level2": {"check_nulls": ["name"]}}

        with patch("fairway.validations.checks.logger") as mock_logger:
            result = Validator.level2_check_spark(df, config)

            # Should have logged about checking nulls
            info_calls = [str(c) for c in mock_logger.info.call_args_list]
            assert any("Checking nulls" in c for c in info_calls)

        assert result["passed"] is False


class TestCLISummarize:
    """Tests for CLI summarize command."""

    def test_cli_summarize_command_exists(self, cli_runner):
        """Test that summarize command is available."""
        from fairway.cli import main

        result = cli_runner.invoke(main, ["summarize", "--help"])
        assert result.exit_code == 0
        assert "Generate summary stats" in result.output

    def test_cli_run_skip_summary_flag(self, cli_runner):
        """Test that --skip-summary flag is available on run command."""
        from fairway.cli import main

        result = cli_runner.invoke(main, ["run", "--help"])
        assert result.exit_code == 0
        assert "--skip-summary" in result.output
