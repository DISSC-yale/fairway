"""
TDD Test: Table root path resolution

Bug: When a table has 'root' and 'path' defined, the root is being ignored
during ingestion. The path should be combined as root + path.
"""
import os
import sys
import tempfile
import shutil

# Ensure source is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
from unittest.mock import patch, MagicMock


def test_preprocess_returns_full_path_when_root_defined():
    """
    When a table has 'root' defined and no preprocessing is needed,
    _preprocess() should return the full path (root + path) not just path.
    """
    from fairway.pipeline import IngestionPipeline

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create directory structure
        root_dir = os.path.join(tmpdir, "data_root")
        subdir = os.path.join(root_dir, "item_table")
        os.makedirs(subdir)

        # Create a test parquet file
        test_file = os.path.join(subdir, "test.parquet")
        with open(test_file, 'w') as f:
            f.write("dummy")  # Just need file to exist

        # Create config
        config_path = os.path.join(tmpdir, "fairway.yaml")
        with open(config_path, 'w') as f:
            f.write(f"""
dataset_name: "test_root_path"
engine: "duckdb"
storage:
  root: "{tmpdir}/output"
  format: "parquet"
tables:
  - name: "item"
    root: "{root_dir}"
    path: "item_table/*.parquet"
    format: "parquet"
""")

        pipeline = IngestionPipeline(config_path)

        # Get the table config
        table = pipeline.config.tables[0]

        # Call _preprocess - should return full path since no preprocessing needed
        result_path = pipeline._preprocess(table)

        # The bug: currently returns "item_table/*.parquet" without root
        # Expected: should return "{root_dir}/item_table/*.parquet"
        expected_path = os.path.join(root_dir, "item_table/*.parquet")

        assert result_path == expected_path, \
            f"Expected path with root: {expected_path}, but got: {result_path}"


def test_engine_ingest_receives_full_path_with_root():
    """
    When running the pipeline, engine.ingest() should receive the full path
    (root + path) not just the relative path.
    """
    from fairway.pipeline import IngestionPipeline
    import uuid

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create directory structure
        root_dir = os.path.join(tmpdir, "data_root")
        subdir = os.path.join(root_dir, "item_table")
        os.makedirs(subdir)

        # Create a test parquet file with valid content
        test_file = os.path.join(subdir, "test.parquet")

        # Create minimal valid parquet
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.table({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
        pq.write_table(table, test_file)

        # Create output dir
        output_dir = os.path.join(tmpdir, "output")
        os.makedirs(output_dir)

        # Use unique dataset name to avoid manifest caching
        unique_name = f"test_root_path_{uuid.uuid4().hex[:8]}"

        # Create config
        config_path = os.path.join(tmpdir, "fairway.yaml")
        with open(config_path, 'w') as f:
            f.write(f"""
dataset_name: "{unique_name}"
engine: "duckdb"
storage:
  root: "{output_dir}"
  format: "parquet"
tables:
  - name: "item"
    root: "{root_dir}"
    path: "item_table/*.parquet"
    format: "parquet"
""")

        pipeline = IngestionPipeline(config_path)

        # Mock the engine.ingest to capture the input_path
        captured_paths = []
        original_ingest = pipeline.engine.ingest

        def mock_ingest(input_path, *args, **kwargs):
            captured_paths.append(input_path)
            return original_ingest(input_path, *args, **kwargs)

        # Mock manifest.should_process to always return True (bypass caching)
        with patch.object(pipeline.engine, 'ingest', side_effect=mock_ingest):
            # Get the table manifest and mock should_process
            table_manifest = pipeline.manifest_store.get_table_manifest("item")
            with patch.object(table_manifest, 'should_process', return_value=True):
                pipeline.run()

        # Verify the path passed to ingest includes root
        assert len(captured_paths) > 0, "engine.ingest was not called"

        input_path = captured_paths[0]
        expected_prefix = root_dir

        # The bug: currently passes "item_table/*.parquet" without root
        # Expected: should pass "{root_dir}/item_table/*.parquet"
        assert input_path.startswith(expected_prefix), \
            f"Expected input_path to start with root '{expected_prefix}', but got: {input_path}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
