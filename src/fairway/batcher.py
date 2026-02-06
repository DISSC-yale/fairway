"""Partition-aware file batching for shuffle-free ingestion."""
import os
import re
import hashlib


class PartitionBatcher:
    """Groups files into partition-aware batches using regex extraction.

    Instead of reading all files at once (causing Spark shuffles),
    files are pre-grouped by their partition values so each batch
    can be written directly to the correct output partition.
    """

    @staticmethod
    def extract_partition_values(file_path, naming_pattern, partition_by, compiled_re=None):
        """Extract partition key values from a file path.

        Applies naming_pattern regex to the file basename.
        Returns tuple of values in partition_by order, or None if no match.
        """
        basename = os.path.basename(file_path)
        if compiled_re:
            match = compiled_re.search(basename)
        else:
            match = re.search(naming_pattern, basename)
        if not match:
            return None

        groups = match.groupdict()

        # All partition_by keys must be present in the regex match
        values = []
        for key in partition_by:
            if key not in groups:
                return None
            values.append(groups[key])

        return tuple(values)

    @staticmethod
    def group_files(file_paths, naming_pattern, partition_by):
        """Group files into batches keyed by partition values.

        Returns dict where:
          - key = tuple of partition values (e.g., ("CT", "2023"))
          - value = list of file paths belonging to that partition

        Files that don't match naming_pattern are grouped under None key.
        """
        if not file_paths:
            return {}

        compiled = re.compile(naming_pattern)
        batches = {}
        for path in file_paths:
            partition_values = PartitionBatcher.extract_partition_values(
                path, naming_pattern, partition_by, compiled_re=compiled
            )
            if partition_values not in batches:
                batches[partition_values] = []
            batches[partition_values].append(path)

        return batches

    @staticmethod
    def get_output_subpath(partition_by, partition_values):
        """Generate Hive-style output subpath.

        Example:
            get_output_subpath(["state", "year"], ("CT", "2023"))
            -> "state=CT/year=2023"

        Partition values are sanitized to prevent path traversal.
        """
        parts = []
        for key, val in zip(partition_by, partition_values):
            # Sanitize: remove path separators and traversal sequences
            safe_val = val.replace("/", "_").replace("\\", "_").replace("..", "_")
            parts.append(f"{key}={safe_val}")
        return "/".join(parts)

    @staticmethod
    def generate_batch_id(table_name, partition_key, file_paths):
        """Generate a deterministic batch ID.

        Format: {table}_{partition_key_sanitized}_{hash[:8]}

        The hash is computed from sorted file paths to ensure determinism
        regardless of input order.

        Args:
            table_name: Name of the table being processed
            partition_key: Partition key string (e.g., "state=CT/year=2023")
            file_paths: List of file paths in this batch

        Returns:
            Deterministic batch ID string
        """
        # Sanitize partition key for use in ID (replace special chars)
        sanitized_partition = partition_key.replace("/", "_").replace("=", "-")

        # Sort files for determinism
        sorted_files = sorted(file_paths)

        # Create hash of sorted file paths
        content = "\n".join(sorted_files)
        file_hash = hashlib.sha256(content.encode()).hexdigest()[:8]

        return f"{table_name}_{sanitized_partition}_{file_hash}"
