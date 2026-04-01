"""
Tests for format ingestion: CSV, TSV, JSON, JSONL, Parquet, fixed-width.
Every test runs on both DuckDB and PySpark via the parametrized engine fixture.
Every test loads real fixture files — no in-memory data construction.
"""
import pytest
from pathlib import Path
from tests.helpers import build_config, read_curated


def engine_name(engine):
    """Return 'duckdb' or 'pyspark' from engine fixture."""
    return "pyspark" if hasattr(engine, "spark") else "duckdb"


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

class TestCSVIngestion:

    def test_csv_simple(self, engine, fixtures_dir, tmp_path):
        """Simple CSV: 3 rows, id/name/value, first row id=1 name=alice value=100."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "simple",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "schema": {"id": "INTEGER", "name": "VARCHAR", "value": "INTEGER"},
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "simple").sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "alice"
        assert df.iloc[0]["value"] == 100

    def test_csv_missing_values_nulls_preserved(self, engine, fixtures_dir, tmp_path):
        """missing_values.csv: 4 rows, 3 null values across id/name/value."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "missing",
            "path": str(fixtures_dir / "formats" / "csv" / "missing_values.csv"),
            "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "missing")
        assert len(df) == 4
        assert df["id"].isna().sum() == 1    # row 2 has null id
        assert df["name"].isna().sum() == 1  # row 3 has null name
        assert df["value"].isna().sum() == 1 # row 4 has null value

    def test_csv_empty_has_zero_rows(self, engine, fixtures_dir, tmp_path):
        """empty.csv: headers only, must produce 0 rows in output."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "empty",
            "path": str(fixtures_dir / "formats" / "csv" / "empty.csv"),
            "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "empty")
        assert len(df) == 0
        assert "id" in df.columns

    def test_csv_zipped_same_rows_as_unzipped(self, engine, fixtures_dir, tmp_path):
        """csv_simple.zip must produce same row count as csv/simple.csv."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "zipped",
            "path": str(fixtures_dir / "zipped" / "csv_simple.zip"),
            "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "zipped")
        assert len(df) == 3  # same as csv/simple.csv

    def test_csv_with_headers_category_column_present(self, engine, fixtures_dir, tmp_path):
        """with_headers.csv has 4 rows and a category column."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "with_headers",
            "path": str(fixtures_dir / "formats" / "csv" / "with_headers.csv"),
            "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "with_headers")
        assert len(df) == 4
        assert "category" in df.columns
        assert set(df["category"].dropna().unique()) == {"A", "B", "C"}


# ---------------------------------------------------------------------------
# TSV
# ---------------------------------------------------------------------------

class TestTSVIngestion:

    def test_tsv_simple(self, engine, fixtures_dir, tmp_path):
        """TSV: same 3 rows as csv/simple.csv, tab-delimited."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "tsv_simple",
            "path": str(fixtures_dir / "formats" / "tsv" / "simple.tsv"),
            "format": "tsv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "tsv_simple").sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert df.iloc[0]["name"] == "alice"


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

class TestJSONIngestion:

    def test_json_records(self, engine, fixtures_dir, tmp_path):
        """JSON records array: 3 rows, id/name/value."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "json_records",
            "path": str(fixtures_dir / "formats" / "json" / "records.json"),
            "format": "json",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "json_records").sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert df.iloc[0]["id"] == 1
        assert df.iloc[2]["name"] == "carol"

    def test_json_lines(self, engine, fixtures_dir, tmp_path):
        """JSONL: one JSON object per line, 3 rows."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "json_lines",
            "path": str(fixtures_dir / "formats" / "json" / "lines.jsonl"),
            "format": "json",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "json_lines")
        assert len(df) == 3

    def test_json_missing_values(self, engine, fixtures_dir, tmp_path):
        """JSON with null fields: nulls must be preserved in output."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "json_nulls",
            "path": str(fixtures_dir / "formats" / "json" / "missing_values.json"),
            "format": "json",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "json_nulls")
        assert len(df) == 3
        assert df["id"].isna().sum() == 1


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

class TestParquetIngestion:

    def test_parquet_simple_roundtrip(self, engine, fixtures_dir, tmp_path):
        """Parquet roundtrip: read simple.parquet, write output, row count matches."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "parquet_in",
            "path": str(fixtures_dir / "formats" / "parquet" / "simple.parquet"),
            "format": "parquet",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "parquet_in")
        assert len(df) == 3
        assert set(df.columns) >= {"id", "name", "value"}


# ---------------------------------------------------------------------------
# Fixed-width
# ---------------------------------------------------------------------------

class TestFixedWidthIngestion:

    def test_fixed_width_simple_column_positions(self, engine, fixtures_dir, tmp_path):
        """Fixed-width: columns must be extracted at correct positions per spec."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "fw_simple",
            "path": str(fixtures_dir / "formats" / "fixed_width" / "simple.txt"),
            "format": "fixed_width",
            "fixed_width_spec": str(fixtures_dir / "formats" / "fixed_width" / "simple_spec.yaml"),
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "fw_simple").sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "age"]
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "Alice"
        assert df.iloc[0]["age"] == 30

    def test_fixed_width_coded_values_null_for_non_castable(self, engine, fixtures_dir, tmp_path):
        """coded_values.txt: 'ZZZ' income must become NULL (adaptive cast_mode)."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "fw_coded",
            "path": str(fixtures_dir / "formats" / "fixed_width" / "coded_values.txt"),
            "format": "fixed_width",
            "fixed_width_spec": str(fixtures_dir / "formats" / "fixed_width" / "coded_values_spec.yaml"),
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "fw_coded").sort_values("rectype").reset_index(drop=True)
        assert len(df) == 3
        # Row with income=ZZZ must have null income
        bob_row = df[df["name"] == "Bob"].iloc[0]
        assert bob_row["income"] != bob_row["income"]  # NaN != NaN

    def test_fixed_width_zipped(self, engine, fixtures_dir, tmp_path):
        """fixed_width_simple.zip: same row count as unzipped version."""
        config = build_config(tmp_path, engine=engine_name(engine), table={
            "name": "fw_zipped",
            "path": str(fixtures_dir / "zipped" / "fixed_width_simple.zip"),
            "format": "fixed_width",
            "fixed_width_spec": str(fixtures_dir / "formats" / "fixed_width" / "simple_spec.yaml"),
            "preprocess": {"action": "unzip", "scope": "per_file"},
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "fw_zipped")
        assert len(df) == 3
