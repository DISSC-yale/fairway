"""Tests for Summarizer."""
import pytest
import pandas as pd
import os
from fairway.summarize import Summarizer


class TestGenerateSummaryTable:
    """Tests for DuckDB-style summary generation."""

    def test_basic_summary(self, tmp_path):
        """Generates summary CSV with expected columns."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "amount": [10.0, 20.0, 30.0],
        })
        output = str(tmp_path / "summary.csv")

        result = Summarizer.generate_summary_table(df, output)

        assert os.path.exists(output)
        assert len(result) == 3  # one row per column
        assert "variable" in result.columns
        assert "null_count" in result.columns
        assert "distinct_count" in result.columns

    def test_numeric_stats(self, tmp_path):
        """Numeric columns get min/max/mean/median."""
        df = pd.DataFrame({"val": [10, 20, 30, 40]})
        output = str(tmp_path / "summary.csv")

        result = Summarizer.generate_summary_table(df, output)

        row = result[result["variable"] == "val"].iloc[0]
        assert row["min"] == 10
        assert row["max"] == 40
        assert row["mean"] == 25.0
        assert row["median"] == 25.0

    def test_null_counting(self, tmp_path):
        """Null values are counted correctly."""
        df = pd.DataFrame({"col": [1, None, 3, None]})
        output = str(tmp_path / "summary.csv")

        result = Summarizer.generate_summary_table(df, output)

        row = result[result["variable"] == "col"].iloc[0]
        assert row["null_count"] == 2

    def test_string_column_no_numeric_stats(self, tmp_path):
        """String columns don't have min/max/mean/median keys."""
        df = pd.DataFrame({"name": ["alice", "bob"]})
        output = str(tmp_path / "summary.csv")

        result = Summarizer.generate_summary_table(df, output)

        row = result[result["variable"] == "name"].iloc[0]
        # Should not have numeric stats (or they should be NaN)
        assert pd.isna(row.get("min", float("nan")))

    def test_empty_dataframe(self, tmp_path):
        """Empty dataframe produces empty summary."""
        df = pd.DataFrame()
        output = str(tmp_path / "summary.csv")

        result = Summarizer.generate_summary_table(df, output)

        assert len(result) == 0


class TestGenerateMarkdownReport:
    """Tests for markdown report generation."""

    def test_basic_report(self, tmp_path):
        """Generates markdown with expected sections."""
        summary_df = pd.DataFrame({
            "variable": ["id", "amount"],
            "type": ["int64", "float64"],
            "null_count": [0, 1],
        })
        stats = {"row_count": 100, "status": "success", "config_path": "config/test.yaml"}
        output = str(tmp_path / "report.md")

        Summarizer.generate_markdown_report("test_dataset", summary_df, stats, output)

        assert os.path.exists(output)
        content = open(output).read()
        assert "# test_dataset" in content
        assert "Total Rows" in content
        assert "100" in content
        assert "success" in content

    def test_report_contains_lineage(self, tmp_path):
        """Report includes lineage section."""
        summary_df = pd.DataFrame({"variable": ["x"]})
        stats = {"row_count": 5, "status": "done", "config_path": "my.yaml"}
        output = str(tmp_path / "report.md")

        Summarizer.generate_markdown_report("ds", summary_df, stats, output)

        content = open(output).read()
        assert "Lineage" in content
        assert "my.yaml" in content
