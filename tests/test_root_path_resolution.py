"""Cross-stack tests for table root + path config combination.

When a table has both 'root' (base directory) and 'path' (relative glob),
the pipeline must join them and ingest data from root/path.
Real files, real DuckDB engine, real parquet output — no mocks.
"""
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from tests.helpers import build_config, read_curated


def _write_parquet(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), str(path))


class TestRootPathResolution:

    @pytest.mark.local
    def test_pipeline_ingests_from_root_plus_path(self, tmp_path, monkeypatch):
        """Full pipeline run with root + path discovers and ingests the file."""
        monkeypatch.chdir(tmp_path)
        data_root = tmp_path / "external_data"
        _write_parquet(
            data_root / "sensor" / "readings.parquet",
            {"device_id": [1, 2, 3], "value": [10.1, 20.2, 30.3]},
        )
        config_path = build_config(tmp_path, table={
            "name": "sensor_readings",
            "root": str(data_root),
            "path": "sensor/*.parquet",
            "format": "parquet",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)

        df = read_curated(tmp_path, "sensor_readings")
        assert len(df) == 3
        assert set(df.columns) >= {"device_id", "value"}

    @pytest.mark.local
    def test_table_without_root_uses_path_directly(self, tmp_path, monkeypatch):
        """Table with no 'root' key still ingests correctly from absolute path."""
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "items.csv"
        csv_path.parent.mkdir(parents=True)
        csv_path.write_text("id,label\n1,alpha\n2,beta\n")

        config_path = build_config(tmp_path, table={
            "name": "items",
            "path": str(csv_path),
            "format": "csv",
        })
        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)

        df = read_curated(tmp_path, "items")
        assert len(df) == 2
        assert set(df.columns) >= {"id", "label"}
