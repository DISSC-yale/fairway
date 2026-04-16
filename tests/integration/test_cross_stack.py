"""Gap tests: known-vulnerability scenarios that require real pipeline runs.

manifest_corruption: a corrupted manifest JSON must not abort ingestion.
transformer_collision: two scripts with the same basename must stay isolated.
mock_enrichment_guard: pipeline must refuse geocode enrichment without explicit opt-in.
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
    """Two transformer scripts with same basename stay isolated in sys.modules.

    Before G1: registry.py keyed sys.modules by basename, so the second load
    overwrote the first. After G1: each script gets a unique hash-based key,
    so both modules coexist.

    Validates via t_a.__module__ != t_b.__module__ and verifies both keys
    still resolve in sys.modules after both loads.
    """
    monkeypatch.chdir(tmp_path)

    for subdir in ["scripts_a", "scripts_b"]:
        script_dir = tmp_path / subdir
        script_dir.mkdir()
        (script_dir / "transform.py").write_text(
            f"import pandas as pd\n"
            f"MARKER = 'from_{subdir}'\n"
            f"class MyTransformer:\n"
            f"    def __init__(self, df): self.df = df\n"
            f"    def transform(self):\n"
            f"        self.df[MARKER] = True\n"
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

    import sys as _sys

    assert t_a.__module__ != t_b.__module__, (
        f"Both transformers got the same sys.modules key: {t_a.__module__!r}. "
        "G1 fix should give each script a unique hash-based key."
    )
    assert t_a.__module__ in _sys.modules, "First module evicted from sys.modules"
    assert t_b.__module__ in _sys.modules, "Second module evicted from sys.modules"

    result_a = t_a(pd.DataFrame({"id": [1]})).transform()
    result_b = t_b(pd.DataFrame({"id": [1]})).transform()
    assert "from_scripts_a" in result_a.columns
    assert "from_scripts_b" in result_b.columns
    assert "from_scripts_a" not in result_b.columns, "Collision: A leaked into B"


@pytest.mark.local
def test_pipeline_refuses_mock_enrichment_without_opt_in(tmp_path, monkeypatch):
    """enrichment.geocode without allow_mock must raise — prevents mock data in prod."""
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "raw" / "addresses.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text("id,address\n1,1 A St\n2,2 B St\n")

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "mock_guard", "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "enrichment": {"geocode": True},
        "tables": [{"name": "addresses", "path": str(csv_path), "format": "csv"}],
    }))

    from fairway.pipeline import IngestionPipeline
    with pytest.raises((ValueError, RuntimeError), match="allow_mock"):
        IngestionPipeline(str(config_path)).run(skip_summary=True)


@pytest.mark.local
def test_pipeline_allows_mock_enrichment_with_opt_in(tmp_path, monkeypatch):
    """enrichment.geocode + allow_mock: true runs and produces deterministic columns."""
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "raw" / "addresses.csv"
    csv_path.parent.mkdir(parents=True)
    csv_path.write_text("id,address\n1,1 A St\n2,2 B St\n")

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "mock_ok", "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "enrichment": {"geocode": True, "allow_mock": True},
        "tables": [{"name": "addresses", "path": str(csv_path), "format": "csv"}],
    }))

    from fairway.pipeline import IngestionPipeline
    IngestionPipeline(str(config_path)).run(skip_summary=True)

    from tests.helpers import read_as_df, read_curated
    df = read_curated(tmp_path, "addresses")
    assert {"latitude", "longitude", "h3_index"}.issubset(df.columns)
    assert ((df["latitude"] >= -90) & (df["latitude"] <= 90)).all()
    assert ((df["longitude"] >= -180) & (df["longitude"] <= 180)).all()

    # Pin the enrichment-only persist fix: modified df must be written to
    # the processed layer, not only surfaced via curated's atomic copy.
    processed = tmp_path / "data" / "processed" / "addresses_processed.parquet"
    assert processed.exists(), "Enrichment-only run did not persist to processed layer"
    proc_df = read_as_df(processed)
    assert {"latitude", "longitude", "h3_index"}.issubset(proc_df.columns)
