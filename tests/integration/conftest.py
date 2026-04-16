"""Shared fixtures for integration tests.

simple_project: single-table DuckDB project with 3 people rows.
partitioned_project: multi-file sales CSV, partitioned by year/month.
"""
import pytest
from pathlib import Path

FIXTURE_CONFIGS = Path(__file__).parent.parent / "fixtures" / "configs"


@pytest.fixture
def simple_project(tmp_path):
    """Minimal project: single table, no partitions, DuckDB engine."""
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    (tmp_path / "manifest").mkdir()

    (raw_dir / "people.csv").write_text(
        "id,name,age\n1,alice,30\n2,bob,25\n3,carol,35\n"
    )

    template = (FIXTURE_CONFIGS / "simple_duckdb.yaml").read_text()
    config_text = template.replace("PLACEHOLDER_ROOT", str(tmp_path / "data"))
    config_path = tmp_path / "config" / "fairway.yaml"
    config_path.parent.mkdir()
    config_path.write_text(config_text)
    return tmp_path


@pytest.fixture
def partitioned_project(tmp_path):
    """Project with year/month partitioned CSV data."""
    raw_dir = tmp_path / "data" / "raw" / "sales"
    raw_dir.mkdir(parents=True)
    (tmp_path / "data" / "processed").mkdir(parents=True)
    (tmp_path / "data" / "curated").mkdir(parents=True)
    (tmp_path / "manifest").mkdir()

    (raw_dir / "2023_01.csv").write_text(
        "id,amount,year,month\n1,100.0,2023,1\n2,200.0,2023,1\n"
    )
    (raw_dir / "2023_02.csv").write_text(
        "id,amount,year,month\n3,150.0,2023,2\n4,250.0,2023,2\n"
    )

    template = (FIXTURE_CONFIGS / "partitioned_duckdb.yaml").read_text()
    config_text = template.replace("PLACEHOLDER_ROOT", str(tmp_path / "data"))
    config_path = tmp_path / "config" / "fairway.yaml"
    config_path.parent.mkdir()
    config_path.write_text(config_text)
    return tmp_path
