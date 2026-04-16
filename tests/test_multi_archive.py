"""Test that multi-archive tables ingest files from ALL archives."""
import zipfile

import duckdb
import pytest
import yaml

from fairway.pipeline import IngestionPipeline


@pytest.mark.local
def test_multi_archive_all_files_ingested(tmp_path, monkeypatch):
    """All files from multiple archives must appear in the processed output."""
    monkeypatch.chdir(tmp_path)

    # Create two separate zip archives each with one CSV
    for i in range(1, 3):
        zip_path = tmp_path / f"batch_{i}.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(f"data_{i}.csv", f"id,val\n{i},value_{i}\n")

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "multi_archive_test",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [{
            "name": "combined",
            "archives": str(tmp_path / "*.zip"),
            "files": "*.csv",
            "format": "csv",
        }],
    }))

    pipeline = IngestionPipeline(str(config_path))
    pipeline.run(skip_summary=True)

    # Both rows (one from each archive) must be in the curated output
    output_path = str(tmp_path / "data" / "curated" / "combined.parquet")
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT id, val FROM read_parquet('{output_path}') ORDER BY id"
    ).fetchall()
    assert len(rows) == 2, f"Expected 2 rows from 2 archives, got {len(rows)}"
    assert rows[0] == (1, "value_1"), f"Missing row from archive 1: {rows}"
    assert rows[1] == (2, "value_2"), f"Missing row from archive 2: {rows}"
