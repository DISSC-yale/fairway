import zipfile, os, pytest

@pytest.mark.local
def test_zip_slip_blocked_in_process_file_unzip(tmp_path):
    """A zip with path traversal entries must not escape the output directory."""
    malicious_zip = tmp_path / "evil.zip"
    with zipfile.ZipFile(malicious_zip, "w") as zf:
        zf.writestr("../escape.txt", "this should not land outside output_dir")

    import yaml
    # Use a unique table name derived from tmp_path so each test run gets a
    # fresh batch_dir (pipeline.py derives batch_dir from table['name']).
    unique_name = f"evil_{tmp_path.name}"
    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump({
        "dataset_name": "sec_test",
        "engine": "duckdb",
        "storage": {"root": str(tmp_path / "data")},
        "tables": [{
            "name": unique_name,
            "path": str(malicious_zip),
            "format": "csv",
            "preprocess": {"action": "unzip", "scope": "per_file"},
        }],
    }))

    from fairway.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(str(config_path))
    table = pipeline.config.tables[0]

    raised = False
    try:
        pipeline._preprocess(table)
    except (ValueError, zipfile.BadZipFile):
        raised = True  # Raising is the expected safe behaviour

    # The guard must raise — the traversal entry must be rejected explicitly
    assert raised, (
        "Zip Slip: process_file unzip did not raise ValueError for traversal entry"
    )

    escape_target = tmp_path / "escape.txt"
    assert not escape_target.exists(), (
        "Zip Slip: traversal entry was extracted outside output_dir"
    )
