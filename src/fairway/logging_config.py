"""Structured logging configuration for Fairway.

Provides:
- JSON Lines log format for machine-readable logs
- Human-readable console output
- BatchLogger context manager for batch-scoped logging
- Log archival (rotates existing log files on startup)
"""
import logging
import json
import os
from datetime import datetime
from typing import Optional


# Thread-local storage for batch context
_batch_context = {}


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON Lines for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON line."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Add extra fields from record (batch_id, partition_key, file_count, etc.)
        extra_fields = ["batch_id", "partition_key", "file_count", "table", "error"]
        for field in extra_fields:
            value = getattr(record, field, None)
            if value is not None:
                log_entry[field] = value

        # Add batch context if available
        for key, value in _batch_context.items():
            if key not in log_entry or log_entry[key] is None:
                log_entry[key] = value

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class ConsoleFormatter(logging.Formatter):
    """Human-readable format for console output."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console."""
        # Format: [LEVEL] message
        base = f"[{record.levelname}] {record.getMessage()}"

        # Add batch context if present
        batch_id = getattr(record, "batch_id", None) or _batch_context.get("batch_id")
        if batch_id:
            base = f"[{record.levelname}] [{batch_id}] {record.getMessage()}"

        return base


class BatchLogger:
    """Context manager that attaches batch context to all log entries.

    Usage:
        with BatchLogger(logger, batch_id="b1", partition_key="state=CT", file_count=10):
            logger.info("Processing")  # Will include batch_id, partition_key, file_count
    """

    def __init__(
        self,
        logger: logging.Logger,
        batch_id: str,
        partition_key: Optional[str] = None,
        file_count: Optional[int] = None,
    ):
        self.logger = logger
        self.batch_id = batch_id
        self.partition_key = partition_key
        self.file_count = file_count
        self._previous_context = {}

    def __enter__(self):
        """Set batch context for all log entries."""
        global _batch_context
        # Save previous context
        self._previous_context = _batch_context.copy()

        # Set new context
        _batch_context = {
            "batch_id": self.batch_id,
            "partition_key": self.partition_key,
            "file_count": self.file_count,
        }
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clear batch context."""
        global _batch_context
        _batch_context = self._previous_context
        return False  # Don't suppress exceptions


def _archive_existing_log(log_file: str) -> None:
    """Archive existing log file by renaming with datetime suffix.

    If logs/fairway.jsonl exists, rename to logs/fairway_2026-02-05T10-30-00.jsonl
    """
    if not os.path.exists(log_file):
        return

    log_dir = os.path.dirname(log_file)
    base_name = os.path.basename(log_file)
    name_without_ext = os.path.splitext(base_name)[0]
    ext = os.path.splitext(base_name)[1]

    # Generate timestamp for archive name (use dashes instead of colons for filesystem compatibility)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    archive_name = f"{name_without_ext}_{timestamp}{ext}"
    archive_path = os.path.join(log_dir, archive_name)

    os.rename(log_file, archive_path)


def setup_logging(
    log_file: Optional[str] = None,
    level: str = "INFO",
    console: bool = True,
) -> logging.Logger:
    """Configure the fairway logger with structured logging.

    Args:
        log_file: Path to JSON Lines log file. If None, no file logging.
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        console: Whether to enable console output.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("fairway")

    # Clear any existing handlers (important for re-initialization in tests)
    logger.handlers.clear()

    # Set level
    logger.setLevel(getattr(logging, level.upper()))

    # Console handler with human-readable format
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        logger.addHandler(console_handler)

    # File handler with JSON format
    if log_file:
        # Create log directory if needed
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Archive existing log file
        _archive_existing_log(log_file)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)

    # Prevent propagation to root logger (avoids duplicate logs)
    logger.propagate = False

    return logger
