"""Tests for PartitionBatcher — partition-aware file grouping."""
import pytest
from fairway.batcher import PartitionBatcher


class TestExtractPartitionValues:
    """Tests for extracting partition key values from file paths."""

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
        """partition_by order controls tuple order, not regex group order."""
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["year", "state"]
        )
        assert result == ("2023", "CT")

    def test_regex_applied_to_basename(self):
        """Regex should match against the file basename, not full path."""
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
        """If regex matches but a partition_by key is missing from groups, return None."""
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_data.csv",
            r"(?P<state>[A-Z]{2})_data",
            ["state", "year"]  # year not in regex groups
        )
        assert result is None

    def test_s3_path(self):
        result = PartitionBatcher.extract_partition_values(
            "s3://my-bucket/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert result == ("CT", "2023")

    def test_regex_with_extra_groups_beyond_partition_by(self):
        """Regex may have more named groups than partition_by — only extract what's needed."""
        result = PartitionBatcher.extract_partition_values(
            "/data/raw/CT_2023_01.csv",
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})_(?P<seq>\d+)",
            ["state", "year"]
        )
        assert result == ("CT", "2023")


class TestGroupFiles:
    """Tests for grouping files into partition-aware batches."""

    def test_group_multiple_files_into_batches(self):
        files = [
            "/data/CT_2023_01.csv",
            "/data/CT_2023_02.csv",
            "/data/NY_2023_01.csv",
            "/data/NY_2024_01.csv",
        ]
        batches = PartitionBatcher.group_files(
            files,
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )

        assert ("CT", "2023") in batches
        assert ("NY", "2023") in batches
        assert ("NY", "2024") in batches
        assert len(batches[("CT", "2023")]) == 2
        assert len(batches[("NY", "2023")]) == 1
        assert len(batches[("NY", "2024")]) == 1

    def test_unmatched_files_grouped_under_none(self):
        files = [
            "/data/CT_2023_01.csv",
            "/data/readme.txt",
            "/data/metadata.json",
        ]
        batches = PartitionBatcher.group_files(
            files,
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )

        assert ("CT", "2023") in batches
        assert None in batches
        assert len(batches[None]) == 2

    def test_empty_file_list_returns_empty_dict(self):
        batches = PartitionBatcher.group_files(
            [],
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert batches == {}

    def test_all_files_match_same_partition(self):
        files = [
            "/data/CT_2023_01.csv",
            "/data/CT_2023_02.csv",
            "/data/CT_2023_03.csv",
        ]
        batches = PartitionBatcher.group_files(
            files,
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert len(batches) == 1
        assert len(batches[("CT", "2023")]) == 3

    def test_preserves_file_order_within_batch(self):
        files = [
            "/data/CT_2023_03.csv",
            "/data/CT_2023_01.csv",
            "/data/CT_2023_02.csv",
        ]
        batches = PartitionBatcher.group_files(
            files,
            r"(?P<state>[A-Z]{2})_(?P<year>\d{4})",
            ["state", "year"]
        )
        assert batches[("CT", "2023")] == files

    def test_single_partition_key_grouping(self):
        files = [
            "/data/CT_claims_01.csv",
            "/data/NY_claims_01.csv",
            "/data/CT_claims_02.csv",
        ]
        batches = PartitionBatcher.group_files(
            files,
            r"(?P<state>[A-Z]{2})_claims",
            ["state"]
        )
        assert len(batches) == 2
        assert len(batches[("CT",)]) == 2
        assert len(batches[("NY",)]) == 1


class TestGetOutputSubpath:
    """Tests for Hive-style output subpath generation."""

    def test_single_partition(self):
        result = PartitionBatcher.get_output_subpath(
            ["state"], ("CT",)
        )
        assert result == "state=CT"

    def test_multiple_partitions(self):
        result = PartitionBatcher.get_output_subpath(
            ["state", "year"], ("CT", "2023")
        )
        assert result == "state=CT/year=2023"

    def test_three_partitions(self):
        result = PartitionBatcher.get_output_subpath(
            ["state", "year", "month"], ("CT", "2023", "01")
        )
        assert result == "state=CT/year=2023/month=01"

    def test_uses_forward_slash_separator(self):
        """Always use forward slash for Hive-style paths (platform-independent)."""
        result = PartitionBatcher.get_output_subpath(
            ["a", "b"], ("x", "y")
        )
        assert "/" in result
        assert "\\" not in result

    def test_sanitizes_path_traversal(self):
        """Partition values with path separators are sanitized."""
        result = PartitionBatcher.get_output_subpath(
            ["state"], ("../../etc",)
        )
        assert ".." not in result
        assert result == "state=____etc"

    def test_sanitizes_slashes_in_values(self):
        result = PartitionBatcher.get_output_subpath(
            ["name"], ("foo/bar",)
        )
        assert "foo/bar" not in result
        assert result == "name=foo_bar"
