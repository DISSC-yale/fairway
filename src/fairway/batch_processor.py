"""Batch processor for Nextflow fan-out/fan-in orchestration.

This module provides the core logic for batch operations, enabling
parallel processing of large datasets through Nextflow.
"""
import math
import os
import glob

from .config_loader import Config


class BatchProcessor:
    """Core logic for batch-aware file processing.

    Handles file discovery, batching, and coordination with Nextflow
    for parallel execution patterns.
    """

    def __init__(self, config_path: str, table: str, batch_size: int | None = None):
        """Initialize BatchProcessor.

        Args:
            config_path: Path to fairway.yaml config file
            table: Name of table to process
            batch_size: Override batch size (defaults to config value)
        """
        self.config_path = config_path
        self.config = Config(config_path)
        self.table_name = table

        # Get table config
        self.table_config = self.config.get_table_by_name(table)
        if not self.table_config:
            raise ValueError(f"Table '{table}' not found in config")

        # Get orchestration settings
        orchestration = self.config.data.get('orchestration', {})
        config_batch_size = orchestration.get('batch_size', 100)
        self.batch_size = batch_size if batch_size is not None else config_batch_size

        # Work directory - used as-is (relative to CWD, matching Nextflow behavior)
        self.work_dir = orchestration.get('work_dir', '.fairway/work')

        # Cache for discovered files (sorted for determinism)
        self._files_cache: list[str] | None = None

    def _discover_files(self) -> list[str]:
        """Discover all files for the table.

        File resolution:
            - If root is absolute (e.g., /gpfs/data): use root directly
            - If root is relative: resolve from config file directory
            - path is always joined to root (or config dir if no root)

        Returns sorted list for deterministic batch assignment.
        """
        if self._files_cache is not None:
            return self._files_cache

        table_path = self.table_config.get('path', '')
        table_root = self.table_config.get('root')
        config_dir = os.path.dirname(os.path.abspath(self.config_path))

        # Resolve root: absolute paths used as-is, relative resolved from config dir
        if table_root:
            if os.path.isabs(table_root):
                resolved_root = table_root
            else:
                resolved_root = os.path.join(config_dir, table_root)
            search_path = os.path.join(resolved_root, table_path.lstrip(os.sep))
        elif os.path.isabs(table_path):
            search_path = table_path
        else:
            search_path = os.path.join(config_dir, table_path)

        # Glob for files matching pattern
        if '*' in search_path:
            files = glob.glob(search_path, recursive=True)
        elif os.path.isdir(search_path):
            files = glob.glob(os.path.join(search_path, '*'))
        else:
            files = [search_path] if os.path.exists(search_path) else []

        # Filter to files only (exclude directories)
        files = [f for f in files if os.path.isfile(f)]

        # Sort for deterministic batch assignment
        self._files_cache = sorted(files)
        return self._files_cache

    def get_file_count(self) -> int:
        """Get total number of files for the table."""
        return len(self._discover_files())

    def get_batch_count(self) -> int:
        """Calculate number of batches needed.

        Uses ceiling division to ensure all files are covered.
        """
        file_count = self.get_file_count()
        if file_count == 0:
            return 0
        return math.ceil(file_count / self.batch_size)

    def get_files_for_batch(self, batch: int) -> list[str]:
        """Get files for a specific batch.

        Args:
            batch: Batch number (0-indexed)

        Returns:
            List of file paths for this batch

        Raises:
            ValueError: If batch number is invalid
        """
        batch_count = self.get_batch_count()
        if batch < 0 or batch >= batch_count:
            raise ValueError(
                f"Invalid batch {batch}. Valid range: 0-{batch_count - 1}"
            )

        files = self._discover_files()
        start_idx = batch * self.batch_size
        end_idx = start_idx + self.batch_size

        return files[start_idx:end_idx]

    def get_batch_dir(self, batch: int) -> str:
        """Get work directory path for a specific batch.

        Args:
            batch: Batch number (0-indexed)

        Returns:
            Path to batch work directory
        """
        return os.path.join(self.work_dir, self.table_name, f"batch_{batch}")
