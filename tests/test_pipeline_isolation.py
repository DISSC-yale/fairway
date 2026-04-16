import pytest
import yaml

@pytest.mark.local
def test_phase0_failure_in_one_table_does_not_abort_siblings(tmp_path, monkeypatch):
    """A preprocessing failure for table 1 must not abort table 2's preprocessing."""
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

    # Run preprocessing for all tables; broken should fail, good should proceed
    preprocessed = {}
    for table in pipeline.config.tables:
        try:
            result = pipeline._preprocess(table)
            preprocessed[table['name']] = result
        except Exception:
            preprocessed[table['name']] = None

    assert preprocessed.get('good') is not None, (
        "Phase 0 failure in 'broken' table aborted 'good' table preprocessing"
    )
