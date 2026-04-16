"""Gap tests: known-vulnerability scenarios that require real pipeline runs.

manifest_corruption: a corrupted manifest JSON must not abort ingestion.
transformer_collision: two scripts with the same basename must stay isolated.
"""
import yaml
import pytest


@pytest.mark.local
def test_manifest_corruption_graceful_recovery(tmp_path, monkeypatch):
    """Corrupted manifest JSON must not crash the pipeline (requires B4 fix)."""
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "raw" / "data.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text("id\n1\n")

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "corrupt_test", "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [{"name": "data", "path": str(csv_path), "format": "csv"}],
    }))

    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    (manifest_dir / "data.json").write_text("{ corrupt !!}")

    from fairway.pipeline import IngestionPipeline
    IngestionPipeline(str(config_path)).run(skip_summary=True)

    curated = tmp_path / "data" / "curated" / "data.parquet"
    assert curated.exists(), "Pipeline failed after manifest corruption"


@pytest.mark.local
def test_transformer_name_collision_uses_correct_module(tmp_path, monkeypatch):
    """Two transformer scripts with same basename stay isolated (requires G1 fix)."""
    monkeypatch.chdir(tmp_path)

    for subdir in ["scripts_a", "scripts_b"]:
        script_dir = tmp_path / subdir
        script_dir.mkdir()
        (script_dir / "transform.py").write_text(
            f"import pandas as pd\n"
            f"class MyTransformer:\n"
            f"    def __init__(self, df): self.df = df\n"
            f"    def transform(self):\n"
            f"        self.df['from_{subdir}'] = True\n"
            f"        return self.df\n"
        )

    from fairway.transformations.registry import (
        load_transformer, add_allowed_directory, clear_allowed_directories,
    )
    import pandas as pd

    add_allowed_directory(str(tmp_path / "scripts_a"))
    add_allowed_directory(str(tmp_path / "scripts_b"))
    try:
        t_a = load_transformer(str(tmp_path / "scripts_a" / "transform.py"))
        t_b = load_transformer(str(tmp_path / "scripts_b" / "transform.py"))
    finally:
        clear_allowed_directories()

    result_a = t_a(pd.DataFrame({"id": [1]})).transform()
    result_b = t_b(pd.DataFrame({"id": [1]})).transform()

    assert "from_scripts_a" in result_a.columns
    assert "from_scripts_b" in result_b.columns
    assert "from_scripts_a" not in result_b.columns, "Collision: A leaked into B"
