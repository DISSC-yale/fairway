"""Tests for :mod:`fairway.batcher` — partition grouping + pre-scan."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from fairway.batcher import PartitionBatcher, ShardSpec, expand_and_validate
from fairway.config import load_config


# ---------------------------------------------------------------------------
# PartitionBatcher.extract_partition_values
# ---------------------------------------------------------------------------

class TestExtractPartitionValues:
    """Filename-regex extraction returns tuples in ``partition_by`` order."""

    def test_single_partition_key(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_claims_01.csv",
            r"(?P<state>[A-Z]{2})_claims",
            ["state"]
        )
        assert result == ("CT",)

    def test_multiple_partition_keys(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert result == ("CT", "2023")

    def test_partition_by_order_determines_tuple_order(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["year", "state"]
        )
        assert result == ("2023", "CT")

    def test_regex_applied_to_basename(self):
        result = PartitionBatcher.extract_partition_values(
            "/very/deep/path/NY_2024_data.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert result == ("NY", "2024")

    def test_no_match_returns_none(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/readme.txt",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert result is None

    def test_partial_match_missing_partition_key_returns_none(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_data.csv",
            r"(?P<state>[A-Z]{2})_data",
            ["state", "year"]  # year not in regex groups
        )
        assert result is None

    def test_regex_with_extra_groups_beyond_partition_by(self):
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})_(?P<seq>\d+)",
            ["state", "year"]
        )
        assert result == ("CT", "2023")


# ---------------------------------------------------------------------------
# PartitionBatcher.group_files
# ---------------------------------------------------------------------------

class TestGroupFiles:
    """In-process clustering used by :mod:`fairway.pipeline`."""

    def test_group_multiple_files_into_batches(self):
        files = [
            "/data/CT_2023_01.csv",
            "/data/CT_2023_02.csv",
            "/data/NY_2023_01.csv",
            "/data/NY_2024_01.csv",
        ]
        batches = PartitionBatcher.group_files(
            files, r"(?P<state>[A-Z]{2})_(?P<year>\d{4})", ["state", "year"]
        )
        assert ("CT", "2023") in batches
        assert ("NY", "2023") in batches
        assert ("NY", "2024") in batches
        assert len(batches[("CT", "2023")]) == 2

    def test_unmatched_files_grouped_under_none(self):
        files = [
            "/data/CT_2023_01.csv",
            "/data/readme.txt",
            "/data/metadata.json",
        ]
        batches = PartitionBatcher.group_files(
            files, r"(?P<state>[A-Z]{2})_(?P<year>\d{4})", ["state", "year"]
        )
        assert ("CT", "2023") in batches
        assert None in batches
        assert len(batches[None]) == 2

    def test_empty_file_list_returns_empty_dict(self):
        assert PartitionBatcher.group_files(
            [], r"(?P<state>[A-Z]{2})_(?P<year>\d{4})", ["state", "year"]
        ) == {}

    def test_all_files_match_same_partition(self):
        files = ["/data/CT_2023_01.csv", "/data/CT_2023_02.csv", "/data/CT_2023_03.csv"]
        batches = PartitionBatcher.group_files(
            files, r"(?P<state>[A-Z]{2})_(?P<year>\d{4})", ["state", "year"]
        )
        assert len(batches) == 1
        assert len(batches[("CT", "2023")]) == 3

    def test_single_partition_key_grouping(self):
        files = [
            "/data/CT_claims_01.csv",
            "/data/NY_claims_01.csv",
            "/data/CT_claims_02.csv",
        ]
        batches = PartitionBatcher.group_files(
            files, r"(?P<state>[A-Z]{2})_claims", ["state"]
        )
        assert len(batches) == 2
        assert len(batches[("CT",)]) == 2
        assert len(batches[("NY",)]) == 1


# ---------------------------------------------------------------------------
# expand_and_validate (Step 9.3 pre-scan gate)
# ---------------------------------------------------------------------------

@pytest.fixture
def _config_factory(tmp_path: Path):
    """Build a minimal Config pointing at ``tmp_path`` inputs."""

    def _make(
        source_glob: str,
        naming_pattern: str = r"(?P<state>[A-Z]+)_(?P<year>\d{4})\.csv",
    ) -> object:
        py = tmp_path / "ds.py"
        py.write_text(
            "from fairway.defaults import default_ingest\n"
            "def transform(con, ctx):\n"
            "    return default_ingest(con, ctx)\n",
            encoding="utf-8",
        )
        yaml_path = tmp_path / "ds.yaml"
        yaml_path.write_text(yaml.safe_dump({
            "dataset_name": "ev_demo",
            "python": str(py),
            "storage_root": str(tmp_path / "store"),
            "source_glob": source_glob,
            "naming_pattern": naming_pattern,
            "partition_by": ["state", "year"],
        }), encoding="utf-8")
        return load_config(yaml_path)

    return _make


class TestExpandAndValidate:
    """Pre-scan glob expansion + naming-pattern validation."""

    def test_clean_glob_returns_all_shards_no_unmatched(
        self, tmp_path: Path, _config_factory
    ) -> None:
        inputs = tmp_path / "in"
        inputs.mkdir()
        for n in ("CT_2023.csv", "MA_2023.csv", "NY_2024.csv"):
            (inputs / n).write_text("id\n1\n", encoding="utf-8")
        config = _config_factory(str(inputs / "*.csv"))

        shards, unmatched = expand_and_validate(config)
        assert unmatched == []
        assert len(shards) == 3
        assert all(isinstance(s, ShardSpec) for s in shards)
        states = {s.partition_values["state"] for s in shards}
        assert states == {"CT", "MA", "NY"}
        # Sorted by shard_id is deterministic.
        ids = [s.shard_id for s in shards]
        assert ids == sorted(ids)

    def test_dirty_glob_returns_partial_match_plus_unmatched(
        self, tmp_path: Path, _config_factory
    ) -> None:
        inputs = tmp_path / "in"
        inputs.mkdir()
        (inputs / "CT_2023.csv").write_text("x", encoding="utf-8")
        # These don't match the naming_pattern (no year, or wrong shape).
        (inputs / "readme.csv").write_text("x", encoding="utf-8")
        (inputs / "lower_case.csv").write_text("x", encoding="utf-8")
        config = _config_factory(str(inputs / "*.csv"))

        shards, unmatched = expand_and_validate(config)
        assert len(shards) == 1
        assert shards[0].partition_values == {"state": "CT", "year": "2023"}
        assert {p.name for p in unmatched} == {"readme.csv", "lower_case.csv"}

    def test_empty_glob_returns_empty_lists(
        self, tmp_path: Path, _config_factory
    ) -> None:
        inputs = tmp_path / "empty"
        inputs.mkdir()
        config = _config_factory(str(inputs / "*.csv"))
        shards, unmatched = expand_and_validate(config)
        assert shards == []
        assert unmatched == []

    def test_multiple_files_share_partition_cluster_into_one_shard(
        self, tmp_path: Path, _config_factory
    ) -> None:
        inputs = tmp_path / "in"
        inputs.mkdir()
        # Three monthly files share (state=CT, year=2023); a separate NY
        # file lands in its own shard. The regex captures only state+year,
        # leaving the month suffix outside the named groups.
        for month in ("jan", "feb", "mar"):
            (inputs / f"CT_2023_{month}.csv").write_text("x", encoding="utf-8")
        (inputs / "NY_2024_jan.csv").write_text("x", encoding="utf-8")
        config = _config_factory(
            str(inputs / "*.csv"),
            naming_pattern=r"(?P<state>[A-Z]+)_(?P<year>\d{4})_[a-z]+\.csv",
        )

        shards, unmatched = expand_and_validate(config)
        assert unmatched == []
        ct_shards = [s for s in shards
                     if s.partition_values == {"state": "CT", "year": "2023"}]
        ny_shards = [s for s in shards
                     if s.partition_values == {"state": "NY", "year": "2024"}]
        assert len(ct_shards) == 1
        assert len(ct_shards[0].input_paths) == 3
        assert len(ny_shards) == 1
        assert len(ny_shards[0].input_paths) == 1
