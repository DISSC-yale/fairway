"""Integration tests for partition-aware batching in the pipeline."""
import pytest
import os
import shutil
import yaml
import glob


class TestPartitionAwarePipeline:
    """End-to-end tests for partition-aware batching."""

    def _create_csv_files(self, data_dir):
        """Create test CSV files named by state/year convention."""
        files = {
            "CT_2023_01.csv": "id,amount\n1,100\n2,200\n",
            "CT_2023_02.csv": "id,amount\n3,300\n",
            "NY_2023_01.csv": "id,amount\n4,400\n5,500\n",
            "NY_2024_01.csv": "id,amount\n6,600\n",
            "MA_2024_01.csv": "id,amount\n7,700\n8,800\n",
            "MA_2024_02.csv": "id,amount\n9,900\n",
        }
        data_dir.mkdir(parents=True, exist_ok=True)
        for name, content in files.items():
            (data_dir / name).write_text(content)
        return files

    def _create_config(self, tmp_path, data_dir, batch_strategy="partition_aware", extra_table_config=None):
        """Create a fairway config YAML."""
        intermediate = tmp_path / "intermediate"
        final = tmp_path / "final"
        intermediate.mkdir(exist_ok=True)
        final.mkdir(exist_ok=True)

        table_config = {
            'name': 'claims',
            'path': str(data_dir / "*.csv"),
            'format': 'csv',
            'naming_pattern': r'(?P<state>[A-Z]{2})_(?P<year>\d{4})',
            'partition_by': ['state', 'year'],
            'batch_strategy': batch_strategy,
        }
        if extra_table_config:
            table_config.update(extra_table_config)

        config = {
            'dataset_name': 'test_batching',
            'engine': 'duckdb',
            'storage': {
                'processed': str(intermediate),
                'curated': str(final),
            },
            'tables': [table_config],
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return str(config_path)

    def test_partition_aware_creates_correct_output_structure(self, tmp_path):
        """6 CSV files across 3 partitions should produce correct Hive-style dirs."""
        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)
        config_path = self._create_config(tmp_path, data_dir)

        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(config_path)
        pipeline.run()

        intermediate = tmp_path / "intermediate" / "claims"
        # Check that partition directories were created
        assert (intermediate / "state=CT" / "year=2023").exists()
        assert (intermediate / "state=NY" / "year=2023").exists()
        assert (intermediate / "state=NY" / "year=2024").exists()
        assert (intermediate / "state=MA" / "year=2024").exists()

    def test_partition_aware_writes_data_to_correct_partitions(self, tmp_path):
        """Verify data lands in the right partition directories."""
        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)
        config_path = self._create_config(tmp_path, data_dir)

        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(config_path)
        pipeline.run()

        import duckdb
        con = duckdb.connect()

        # CT/2023 should have 3 rows (2 files: 2+1 rows)
        ct_2023_path = str(tmp_path / "intermediate" / "claims" / "state=CT" / "year=2023" / "*.parquet")
        ct_files = glob.glob(ct_2023_path)
        assert len(ct_files) > 0, f"No parquet files at {ct_2023_path}"
        df = con.execute(f"SELECT * FROM read_parquet('{ct_files[0]}')").fetchdf()
        assert len(df) == 3

        # NY/2024 should have 1 row
        ny_2024_path = str(tmp_path / "intermediate" / "claims" / "state=NY" / "year=2024" / "*.parquet")
        ny_files = glob.glob(ny_2024_path)
        assert len(ny_files) > 0
        df = con.execute(f"SELECT * FROM read_parquet('{ny_files[0]}')").fetchdf()
        assert len(df) == 1

    def test_manifest_tracks_processed_files(self, tmp_path):
        """After partition-aware run, manifest should record all files."""
        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)
        config_path = self._create_config(tmp_path, data_dir)

        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(config_path)
        pipeline.run()

        # Check manifest state
        manifest = pipeline.manifest_store.get_table_manifest("claims")
        for f in data_dir.glob("*.csv"):
            assert manifest.should_process(str(f)) is False, \
                f"File {f.name} should be marked as processed in manifest"

    def test_rerun_skips_already_processed(self, tmp_path, caplog):
        """Second run should skip all files (manifest says already done)."""
        import logging

        # Enable propagation so caplog can capture
        fairway_logger = logging.getLogger("fairway")
        original_propagate = fairway_logger.propagate
        fairway_logger.propagate = True
        caplog.set_level(logging.INFO)

        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)
        config_path = self._create_config(tmp_path, data_dir)

        try:
            from fairway.pipeline import IngestionPipeline
            pipeline = IngestionPipeline(config_path)
            pipeline.run()

            # Run again
            caplog.clear()
            pipeline2 = IngestionPipeline(config_path)
            pipeline2.run()

            log_output = caplog.text.lower()
            assert "already processed" in log_output or "skipping" in log_output
        finally:
            fairway_logger.propagate = original_propagate

    def test_bulk_strategy_uses_existing_path(self, tmp_path):
        """With batch_strategy=bulk, existing pipeline behavior preserved."""
        # Clean up shared manifest to avoid cross-test pollution
        manifest_file = os.path.join("manifest", "bulk_claims.json")
        if os.path.exists(manifest_file):
            os.remove(manifest_file)

        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)

        # Use a unique table name to avoid manifest collisions with other tests
        intermediate = tmp_path / "intermediate"
        final = tmp_path / "final"
        intermediate.mkdir(exist_ok=True)
        final.mkdir(exist_ok=True)

        config = {
            'dataset_name': 'test_bulk',
            'engine': 'duckdb',
            'storage': {
                'processed': str(intermediate),
                'curated': str(final),
            },
            'tables': [{
                'name': 'bulk_claims',
                'path': str(data_dir / "*.csv"),
                'format': 'csv',
                'batch_strategy': 'bulk',
            }],
        }
        config_path = tmp_path / "config_bulk.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        from fairway.pipeline import IngestionPipeline
        pipeline = IngestionPipeline(str(config_path))
        pipeline.run()

        # Bulk mode writes to intermediate/ (not partitioned subdirs)
        results = list(intermediate.rglob("*.parquet"))
        assert len(results) > 0

    def test_unmatched_files_logged_as_warning(self, tmp_path, caplog):
        """Files not matching naming_pattern are warned about."""
        import logging

        # Enable propagation so caplog can capture
        fairway_logger = logging.getLogger("fairway")
        original_propagate = fairway_logger.propagate
        fairway_logger.propagate = True
        caplog.set_level(logging.WARNING)

        data_dir = tmp_path / "data"
        self._create_csv_files(data_dir)
        # Add a file that won't match the naming pattern
        (data_dir / "readme.csv").write_text("note\nhello\n")

        config_path = self._create_config(tmp_path, data_dir)

        try:
            from fairway.pipeline import IngestionPipeline
            pipeline = IngestionPipeline(config_path)
            pipeline.run()

            log_output = caplog.text.lower()
            assert "didn't match" in log_output or "match" in log_output or "naming_pattern" in log_output
        finally:
            fairway_logger.propagate = original_propagate
