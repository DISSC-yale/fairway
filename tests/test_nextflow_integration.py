"""Integration tests for Nextflow orchestration.

These tests run the actual Nextflow workflows to verify end-to-end functionality.
Skip if Nextflow is not installed.
"""
import pytest
import os
import subprocess
import shutil
from pathlib import Path
import yaml
import json


def nextflow_available():
    """Check if Nextflow is installed."""
    try:
        result = subprocess.run(['nextflow', '-version'], capture_output=True, timeout=10)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# Skip all tests in this module if Nextflow is not available
pytestmark = pytest.mark.skipif(
    not nextflow_available(),
    reason="Nextflow not installed"
)


@pytest.fixture
def nextflow_project(tmp_path):
    """Create a complete project structure for Nextflow testing.

    Structure:
        tmp_path/
            config/
                fairway.yaml
            data/
                raw/
                    file1.csv (columns: id, name, amount)
                    file2.csv (columns: id, name, amount, extra)
                    file3.csv (columns: id, name, amount)
                intermediate/
                final/
            main.nf
            fairway_processes.nf
            nextflow.config
            schema/  (created by workflow)
            .fairway/work/  (created by workflow)
    """
    project = tmp_path / "project"
    project.mkdir()

    # Create directories
    config_dir = project / "config"
    config_dir.mkdir()

    data_raw = project / "data" / "raw"
    data_raw.mkdir(parents=True)
    (project / "data" / "intermediate").mkdir()
    (project / "data" / "final").mkdir()

    # Create test CSV files with different columns to test schema merging
    (data_raw / "file1.csv").write_text("id,name,amount\n1,Alice,100.50\n")
    (data_raw / "file2.csv").write_text("id,name,amount,extra\n2,Bob,200.75,bonus\n")
    (data_raw / "file3.csv").write_text("id,name,amount\n3,Charlie,300.00\n")

    # Create fairway config
    config_content = f"""
dataset_name: test_nextflow
engine: duckdb

orchestration:
  batch_size: 2
  work_dir: .fairway/work

storage:
  raw_dir: data/raw
  intermediate_dir: data/intermediate
  final_dir: data/final

tables:
  - name: test_table
    root: {data_raw}
    path: "*.csv"
    format: csv
"""
    (config_dir / "fairway.yaml").write_text(config_content)

    # Copy Nextflow files from package
    nf_source = Path(__file__).parent.parent / "src" / "fairway" / "data"
    shutil.copy(nf_source / "main.nf", project / "main.nf")
    shutil.copy(nf_source / "fairway_processes.nf", project / "fairway_processes.nf")
    shutil.copy(nf_source / "nextflow.config", project / "nextflow.config")

    return project


class TestNextflowSchemaWorkflow:
    """Test the Nextflow schema discovery workflow."""

    def test_schema_workflow_creates_schema_file(self, nextflow_project):
        """Running 'nextflow run main.nf -entry schema' creates schema/{table}.yaml."""
        # Run Nextflow schema workflow
        result = subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=nextflow_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        # Check Nextflow succeeded
        assert result.returncode == 0, f"Nextflow failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

        # Verify schema file was created
        schema_file = nextflow_project / "schema" / "test_table.yaml"
        assert schema_file.exists(), f"Schema file not created at {schema_file}"

        # Verify schema content
        with open(schema_file) as f:
            schema_data = yaml.safe_load(f)

        assert schema_data['name'] == 'test_table'
        assert 'schema' in schema_data

        # Should have merged columns from all files (including 'extra' from file2)
        schema = schema_data['schema']
        assert 'id' in schema
        assert 'name' in schema
        assert 'amount' in schema
        assert 'extra' in schema, "Schema should include 'extra' column from file2.csv"

    def test_schema_workflow_creates_batch_schemas(self, nextflow_project):
        """Schema workflow creates intermediate batch schema files."""
        # Run Nextflow schema workflow
        subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=nextflow_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        # Verify batch schema files were created
        work_dir = nextflow_project / ".fairway" / "work" / "test_table"

        # With 3 files and batch_size=2, we should have 2 batches
        batch_0_schema = work_dir / "batch_0" / "schema_0.json"
        batch_1_schema = work_dir / "batch_1" / "schema_1.json"

        assert batch_0_schema.exists(), f"Batch 0 schema not found at {batch_0_schema}"
        assert batch_1_schema.exists(), f"Batch 1 schema not found at {batch_1_schema}"

        # Verify unified schema
        unified_schema = work_dir / "unified_schema.json"
        assert unified_schema.exists(), f"Unified schema not found at {unified_schema}"

        with open(unified_schema) as f:
            unified = json.load(f)
        assert 'id' in unified
        assert 'extra' in unified

    def test_schema_workflow_merges_columns_correctly(self, nextflow_project):
        """Schema merge correctly combines columns from files with different schemas."""
        subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=nextflow_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        schema_file = nextflow_project / "schema" / "test_table.yaml"
        with open(schema_file) as f:
            schema_data = yaml.safe_load(f)

        schema = schema_data['schema']

        # Verify all expected columns are present
        expected_columns = {'id', 'name', 'amount', 'extra'}
        actual_columns = set(schema.keys())

        assert expected_columns == actual_columns, \
            f"Expected columns {expected_columns}, got {actual_columns}"

        # Verify types are reasonable
        assert schema['id'] in ('BIGINT', 'INTEGER', 'INT64')
        assert schema['name'] == 'STRING'
        assert schema['amount'] == 'DOUBLE'
        assert schema['extra'] == 'STRING'


class TestNextflowPathResolution:
    """Test that paths resolve correctly in Nextflow context."""

    def test_relative_paths_resolve_to_project_root(self, nextflow_project):
        """Relative paths in config resolve to project root, not Nextflow work dir."""
        subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=nextflow_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        # Schema should be in project/schema/, not in Nextflow's work directory
        schema_in_project = nextflow_project / "schema" / "test_table.yaml"
        assert schema_in_project.exists(), \
            "Schema should be in project root's schema/ directory"

        # Work files should be in project/.fairway/work/
        work_in_project = nextflow_project / ".fairway" / "work" / "test_table"
        assert work_in_project.exists(), \
            "Work directory should be in project root's .fairway/work/"


class TestNextflowParallelExecution:
    """Test that Nextflow runs batches in parallel."""

    def test_multiple_batches_processed(self, nextflow_project):
        """All batches are processed by Nextflow."""
        result = subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=nextflow_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        assert result.returncode == 0, f"Nextflow failed:\n{result.stderr}"

        # Strip ANSI codes for easier assertion
        import re
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        output = ansi_escape.sub('', result.stdout + result.stderr)

        # With 3 files and batch_size=2, expect 2 batches
        assert '2 batches' in output or 'batch jobs: 2' in output.lower()

        # Verify final schema is in project/schema/ folder
        schema_file = nextflow_project / "schema" / "test_table.yaml"
        assert schema_file.exists(), f"Final schema should be at {schema_file}"

        # Verify intermediate batch schemas exist in work dir
        work_dir = nextflow_project / ".fairway" / "work" / "test_table"
        batch_dirs = list(work_dir.glob("batch_*"))
        assert len(batch_dirs) == 2, f"Expected 2 batch directories, found {len(batch_dirs)}"


class TestNextflowZipHandling:
    """Test schema discovery with zip file preprocessing."""

    @pytest.fixture
    def zip_project(self, tmp_path):
        """Create project with zip files for testing."""
        import zipfile

        project = tmp_path / "zip_project"
        project.mkdir()

        config_dir = project / "config"
        config_dir.mkdir()

        data_raw = project / "data" / "raw"
        data_raw.mkdir(parents=True)
        (project / "data" / "intermediate").mkdir()
        (project / "data" / "final").mkdir()

        # Create zip files containing CSVs
        for i in range(2):
            csv_content = f"id,name,value\n{i},item{i},{i*100}\n"
            zip_path = data_raw / f"data_{i}.zip"
            with zipfile.ZipFile(zip_path, 'w') as zf:
                zf.writestr(f"data_{i}.csv", csv_content)

        config_content = f"""
dataset_name: test_zip
engine: duckdb

orchestration:
  batch_size: 1
  work_dir: .fairway/work

temp_location: {tmp_path}/temp

storage:
  raw_dir: data/raw
  intermediate_dir: data/intermediate
  final_dir: data/final

tables:
  - name: zip_table
    root: {data_raw}
    path: "*.zip"
    format: csv
    preprocess:
      action: unzip
      scope: per_file
"""
        (config_dir / "fairway.yaml").write_text(config_content)

        # Copy Nextflow files
        nf_source = Path(__file__).parent.parent / "src" / "fairway" / "data"
        shutil.copy(nf_source / "main.nf", project / "main.nf")
        shutil.copy(nf_source / "fairway_processes.nf", project / "fairway_processes.nf")
        shutil.copy(nf_source / "nextflow.config", project / "nextflow.config")

        return project

    def test_zip_files_extracted_and_scanned(self, zip_project):
        """Zip files are extracted and their contents scanned for schema."""
        result = subprocess.run(
            [
                'nextflow', 'run', 'main.nf',
                '-entry', 'schema',
                '-profile', 'standard',
                '--config', 'config/fairway.yaml',
                '--work_dir', '.fairway/work'
            ],
            cwd=zip_project,
            capture_output=True,
            text=True,
            timeout=300
        )

        assert result.returncode == 0, f"Nextflow failed:\n{result.stderr}"

        # Verify schema was created
        schema_file = zip_project / "schema" / "zip_table.yaml"
        assert schema_file.exists(), "Schema should be created from zip contents"

        with open(schema_file) as f:
            schema_data = yaml.safe_load(f)

        # Should have columns from the CSV inside the zip
        assert 'id' in schema_data['schema']
        assert 'name' in schema_data['schema']
        assert 'value' in schema_data['schema']
