"""
Tests for fairway preprocessing pipeline stage.
Tests hit the IngestionPipeline directly — not Python's zipfile module.
Every test asserts on output data (row counts, column values).
"""
import pytest
from pathlib import Path
from tests.helpers import build_config, read_curated, get_output_mtime
from fairway.pipeline import IngestionPipeline


class TestUnzipPreprocessing:

    def test_unzip_per_file_extracts_and_ingests(self, fixtures_dir, tmp_path, monkeypatch):
        """Pipeline must extract zip, ingest CSV inside, write 3 rows to curated."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "from_zip",
            "path": str(fixtures_dir / "zipped" / "csv_simple.zip"),
            "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
        })
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "from_zip")
        assert len(df) == 3
        assert "id" in df.columns
        assert "name" in df.columns

    def test_unzip_preserves_data_values(self, fixtures_dir, tmp_path, monkeypatch):
        """Data extracted from zip must match the original CSV content exactly."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "zip_values",
            "path": str(fixtures_dir / "zipped" / "csv_simple.zip"),
            "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
            "schema": {"id": "INTEGER", "name": "VARCHAR", "value": "INTEGER"},
        })
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "zip_values").sort_values("id").reset_index(drop=True)
        assert df.iloc[0]["id"] == 1
        assert df.iloc[0]["name"] == "alice"
        assert df.iloc[0]["value"] == 100

    def test_second_run_skips_unchanged_zip(self, fixtures_dir, tmp_path, monkeypatch):
        """Second run with unchanged zip must not re-write output (manifest cache hit)."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "cached_zip",
            "path": str(fixtures_dir / "zipped" / "csv_simple.zip"),
            "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
        })
        pipeline = IngestionPipeline(config)
        pipeline.run()

        mtime_after_first = get_output_mtime(tmp_path, "cached_zip")

        import time
        time.sleep(0.05)  # ensure clock ticks if file were rewritten
        pipeline.run()

        mtime_after_second = get_output_mtime(tmp_path, "cached_zip")
        assert mtime_after_first == mtime_after_second, (
            "Output was rewritten on second run — manifest cache not working"
        )

    def test_archives_and_files_keys(self, fixtures_dir, tmp_path, monkeypatch):
        """archives + files keys must extract matching files from multi-table zip."""
        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "from_archive",
            "archives": str(fixtures_dir / "zipped" / "csv_simple.zip"),
            "files": "*.csv",
            "format": "csv",
        })
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "from_archive")
        assert len(df) == 3


class TestCustomScriptPreprocessing:

    def test_custom_script_runs_and_data_survives(self, fixtures_dir, tmp_path, monkeypatch):
        """Custom preprocess script must execute; data must appear in output.

        The pipeline security check restricts script paths to src/, scripts/,
        or transformations/ under CWD.  We create scripts/ under tmp_path and
        copy the preprocess script there, then chdir to tmp_path so the check passes.
        """
        import shutil

        # Set up scripts/ under tmp_path and chdir so manifest and security both resolve there
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        src_script = Path(__file__).parent / "fixtures" / "scripts" / "simple_preprocess.py"
        script_path = scripts_dir / "simple_preprocess.py"
        shutil.copy2(src_script, script_path)

        monkeypatch.chdir(tmp_path)

        config = build_config(tmp_path, table={
            "name": "scripted",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "preprocess": {
                "action": str(script_path),
                "scope": "per_file",
                "execution_mode": "driver",
            },
        })
        IngestionPipeline(config).run()

        df = read_curated(tmp_path, "scripted")
        assert len(df) == 3
        assert df.iloc[0]["name"] == "alice"
