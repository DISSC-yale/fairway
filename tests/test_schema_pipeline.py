"""Tests for schema_pipeline.SchemaDiscoveryPipeline.

Covers the schema-discovery wrapper around IngestionPipeline.
Follows project rules:
  - RULE-111: runnable via `pytest tests/`
  - RULE-113: uses real engine constructors via conftest fixtures
  - RULE-114: tmp_path provides automatic cleanup of generated artifacts
"""
import os
import yaml
import pytest
pytest.skip("schema_pipeline module removed in v0.3 Step 3 — deleted in Step 4", allow_module_level=True)

from fairway.schema_pipeline import (
    SchemaDiscoveryPipeline,
    _get_metadata_columns_from_pattern,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            f.write(",".join(str(x) for x in row) + "\n")


def _write_config(tmp_path, tables, engine="duckdb"):
    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "schema_discovery_test",
        "engine": engine,
        "storage": {"root": str(tmp_path / "data")},
        "tables": tables,
    }))
    return str(config_path)


# ---------------------------------------------------------------------------
# Pure helper: _get_metadata_columns_from_pattern
# ---------------------------------------------------------------------------

class TestMetadataColumnsFromPattern:

    def test_returns_empty_list_for_none(self):
        assert _get_metadata_columns_from_pattern(None) == []

    def test_returns_empty_list_for_empty_string(self):
        assert _get_metadata_columns_from_pattern("") == []

    def test_extracts_single_named_group(self):
        pattern = r"data_(?P<year>\d{4})\.csv"
        assert _get_metadata_columns_from_pattern(pattern) == ["year"]

    def test_extracts_multiple_named_groups(self):
        pattern = r"(?P<state>[A-Z]{2})_(?P<year>\d{4})_(?P<month>\d{2})\.csv"
        assert _get_metadata_columns_from_pattern(pattern) == ["state", "year", "month"]

    def test_ignores_unnamed_groups(self):
        pattern = r"(\w+)_(?P<year>\d{4})\.csv"
        assert _get_metadata_columns_from_pattern(pattern) == ["year"]


# ---------------------------------------------------------------------------
# SchemaDiscoveryPipeline.run_inference (DuckDB engine)
# ---------------------------------------------------------------------------

class TestSchemaDiscoveryPipelineDuckDB:
    """End-to-end discovery using the real DuckDB engine."""

    @pytest.mark.local
    def test_run_inference_writes_per_table_yaml(self, tmp_path, monkeypatch):
        # Run from tmp_path so manifest/ lands in a clean dir
        monkeypatch.chdir(tmp_path)

        csv_path = tmp_path / "raw" / "people.csv"
        _write_csv(csv_path, ["id", "name", "value"], [(1, "alice", 100), (2, "bob", 200)])

        config_path = _write_config(tmp_path, [{
            "name": "people",
            "path": str(csv_path),
            "format": "csv",
        }])

        out_dir = tmp_path / "schema_out"
        pipeline = SchemaDiscoveryPipeline(config_path)
        result = pipeline.run_inference(output_path=str(out_dir), sampling_ratio=1.0)

        # Consolidated dict returned
        assert result["dataset_name"] == "schema_discovery_test"
        assert len(result["tables"]) == 1
        table_entry = result["tables"][0]
        assert table_entry["name"] == "people"
        assert set(table_entry["schema"].keys()) == {"id", "name", "value"}

        # Per-table YAML written
        yaml_path = out_dir / "people.yaml"
        assert yaml_path.exists()
        loaded = yaml.safe_load(yaml_path.read_text())
        assert loaded["name"] == "people"
        assert set(loaded["schema"].keys()) == {"id", "name", "value"}

    @pytest.mark.local
    def test_run_inference_default_output_path(self, tmp_path, monkeypatch):
        """When output_path is omitted, it defaults to 'schema/' under cwd."""
        monkeypatch.chdir(tmp_path)

        csv_path = tmp_path / "raw" / "users.csv"
        _write_csv(csv_path, ["uid", "email"], [(1, "a@x"), (2, "b@y")])

        config_path = _write_config(tmp_path, [{
            "name": "users",
            "path": str(csv_path),
            "format": "csv",
        }])

        SchemaDiscoveryPipeline(config_path).run_inference(sampling_ratio=1.0)

        assert (tmp_path / "schema" / "users.yaml").exists()

    @pytest.mark.local
    def test_run_inference_injects_metadata_columns_from_naming_pattern(
        self, tmp_path, monkeypatch
    ):
        """Named groups in naming_pattern must appear as STRING columns in schema."""
        monkeypatch.chdir(tmp_path)

        csv_path = tmp_path / "raw" / "sales_2023.csv"
        _write_csv(csv_path, ["id", "amount"], [(1, 100), (2, 200)])

        config_path = _write_config(tmp_path, [{
            "name": "sales",
            "path": str(csv_path),
            "format": "csv",
            "naming_pattern": r"sales_(?P<year>\d{4})\.csv",
        }])

        result = SchemaDiscoveryPipeline(config_path).run_inference(
            output_path=str(tmp_path / "schema_out"), sampling_ratio=1.0,
        )

        schema = result["tables"][0]["schema"]
        assert "year" in schema
        assert schema["year"] == "STRING"
        # Original columns still present
        assert "id" in schema and "amount" in schema

    @pytest.mark.local
    def test_run_inference_injects_partition_by_columns(self, tmp_path, monkeypatch):
        """partition_by columns missing from data should be added as STRING."""
        monkeypatch.chdir(tmp_path)

        csv_path = tmp_path / "raw" / "events.csv"
        _write_csv(csv_path, ["id", "value"], [(1, "x"), (2, "y")])

        config_path = _write_config(tmp_path, [{
            "name": "events",
            "path": str(csv_path),
            "format": "csv",
            "partition_by": ["region"],
        }])

        result = SchemaDiscoveryPipeline(config_path).run_inference(
            output_path=str(tmp_path / "schema_out"), sampling_ratio=1.0,
        )

        schema = result["tables"][0]["schema"]
        assert schema.get("region") == "STRING"

    @pytest.mark.local
    def test_run_inference_skips_missing_paths_keeps_siblings(
        self, tmp_path, monkeypatch
    ):
        """Tables with missing source paths must not abort discovery for siblings.

        The config loader warns and drops missing tables at construction time, so
        run_inference should silently process whatever tables remain.
        """
        monkeypatch.chdir(tmp_path)

        good_csv = tmp_path / "raw" / "good.csv"
        _write_csv(good_csv, ["id", "name"], [(1, "a"), (2, "b")])

        config_path = _write_config(tmp_path, [
            {
                "name": "missing_table",
                "path": str(tmp_path / "raw" / "does_not_exist.csv"),
                "format": "csv",
            },
            {
                "name": "good_table",
                "path": str(good_csv),
                "format": "csv",
            },
        ])

        result = SchemaDiscoveryPipeline(config_path).run_inference(
            output_path=str(tmp_path / "schema_out"), sampling_ratio=1.0,
        )

        names = [t["name"] for t in result["tables"]]
        assert "good_table" in names
        assert "missing_table" not in names

    @pytest.mark.local
    def test_run_inference_records_schema_in_table_manifest(self, tmp_path, monkeypatch):
        """Per-table manifest should track files + schema path after discovery."""
        monkeypatch.chdir(tmp_path)

        csv_path = tmp_path / "raw" / "tracked.csv"
        _write_csv(csv_path, ["id", "label"], [(1, "x"), (2, "y")])

        config_path = _write_config(tmp_path, [{
            "name": "tracked",
            "path": str(csv_path),
            "format": "csv",
        }])

        pipeline = SchemaDiscoveryPipeline(config_path)
        pipeline.run_inference(output_path=str(tmp_path / "schema_out"), sampling_ratio=1.0)

        table_manifest = pipeline.manifest_store.get_table_manifest("tracked")
        # Schema recording is the contract; expose via public API if available,
        # otherwise inspect the manifest data dict directly.
        manifest_data = getattr(table_manifest, "data", None) or getattr(table_manifest, "_data", {})
        assert manifest_data, "Expected manifest data after schema recording"
