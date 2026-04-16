import pytest
import yaml
import os

@pytest.mark.local
def test_phase0_failure_in_one_table_does_not_abort_siblings(tmp_path, monkeypatch):
    """A preprocessing failure for table 1 must not block table 2's full ingestion."""
    monkeypatch.chdir(tmp_path)

    good_csv = tmp_path / "raw" / "good.csv"
    good_csv.parent.mkdir(parents=True)
    good_csv.write_text("id,val\n1,a\n2,b\n")

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "isolation_test",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [
            {
                "name": "broken",
                "path": str(tmp_path / "raw" / "does_not_exist.zip"),
                "format": "csv",
                "preprocess": {"action": "unzip", "scope": "per_file"},
            },
            {
                "name": "good",
                "path": str(good_csv),
                "format": "csv",
            },
        ],
    }))

    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))

    # run() should complete (possibly raising RuntimeError listing failures)
    # but the "good" table should be fully ingested regardless
    try:
        pipeline.run(skip_summary=True)
    except RuntimeError as e:
        # Expected — the "broken" table failed
        assert "broken" in str(e)

    # Verify "good" table was successfully ingested
    curated_path = tmp_path / "data" / "curated" / "good.parquet"
    processed_path = tmp_path / "data" / "processed" / "good"
    assert curated_path.exists() or processed_path.exists(), (
        "Phase 0 failure in 'broken' table prevented 'good' table from being ingested"
    )
