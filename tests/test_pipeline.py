"""Canonical cross-stack integration tests.

These are the reference tests showing the correct pattern for any new
pipeline feature tests: write input → build_config → run → read_curated → assert.
Covers CSV, ZIP, naming_pattern, write mode, and multi-table scenarios.
"""
import zipfile
import pytest
import pandas as pd

from tests.helpers import build_config, read_curated


def _write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            f.write(",".join(str(x) for x in row) + "\n")


def _write_zip(zip_path, csv_name, header, rows):
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    content = ",".join(header) + "\n"
    for row in rows:
        content += ",".join(str(x) for x in row) + "\n"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, content)


class TestCSVIngestion:

    @pytest.mark.local
    def test_csv_ingested_correctly(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "employees.csv"
        _write_csv(csv_path, ["emp_id", "name", "dept"], [
            (1, "alice", "eng"), (2, "bob", "sales"), (3, "carol", "eng"),
        ])
        config_path = build_config(tmp_path, table={
            "name": "employees", "path": str(csv_path), "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        df = read_curated(tmp_path, "employees")
        assert len(df) == 3
        assert set(df["dept"].tolist()) == {"eng", "sales"}

    @pytest.mark.local
    def test_numeric_columns_preserved(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "readings.csv"
        _write_csv(csv_path, ["sensor_id", "temp_c"], [(101, 22.5), (102, 18.3)])
        config_path = build_config(tmp_path, table={
            "name": "readings", "path": str(csv_path), "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        df = read_curated(tmp_path, "readings")
        assert pd.api.types.is_numeric_dtype(df["temp_c"]), \
            f"temp_c should be numeric, got {df['temp_c'].dtype}"


class TestZipIngestion:

    @pytest.mark.local
    def test_zip_extracted_and_ingested(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        zip_path = tmp_path / "raw" / "archive.zip"
        _write_zip(zip_path, "records.csv", ["rec_id", "category"], [
            (10, "A"), (11, "B"), (12, "A"),
        ])
        config_path = build_config(tmp_path, table={
            "name": "records", "path": str(zip_path), "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        df = read_curated(tmp_path, "records")
        assert len(df) == 3
        assert set(df["category"].tolist()) == {"A", "B"}


class TestNamingPatternMetadata:

    @pytest.mark.local
    def test_year_extracted_from_filename(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "sales_2022.csv"
        _write_csv(csv_path, ["product", "amount"], [("widget", 500), ("gadget", 750)])
        config_path = build_config(tmp_path, table={
            "name": "sales", "path": str(csv_path), "format": "csv",
            "naming_pattern": r"sales_(?P<year>\d{4})\.csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        df = read_curated(tmp_path, "sales")
        assert "year" in df.columns, f"year column missing. Columns: {list(df.columns)}"
        assert all(str(y) == "2022" for y in df["year"])


class TestWriteMode:

    @pytest.mark.local
    def test_second_run_overwrites_by_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "orders.csv"
        _write_csv(csv_path, ["order_id", "total"], [(1, 100), (2, 200)])
        config_path = build_config(tmp_path, table={
            "name": "orders", "path": str(csv_path), "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        IngestionPipeline(config_path).run(skip_summary=True)
        df = read_curated(tmp_path, "orders")
        assert len(df) == 2, f"Expected 2 rows after overwrite, got {len(df)}"


class TestMultiTablePipeline:

    @pytest.mark.local
    def test_two_tables_both_curated(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_a = tmp_path / "raw" / "accounts.csv"
        _write_csv(csv_a, ["acct_id", "name"], [(1, "acme"), (2, "globex")])
        csv_b = tmp_path / "raw" / "transactions.csv"
        _write_csv(csv_b, ["txn_id", "acct_id", "amount"], [(101, 1, 500), (102, 2, 750), (103, 1, 250)])
        config_path = build_config(tmp_path,
            table={"name": "accounts", "path": str(csv_a), "format": "csv"},
            extra_tables=[{"name": "transactions", "path": str(csv_b), "format": "csv"}],
        )
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)
        assert len(read_curated(tmp_path, "accounts")) == 2
        assert len(read_curated(tmp_path, "transactions")) == 3
