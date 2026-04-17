"""Tests for structured logging configuration."""
import pytest
import json
import os
import logging
from datetime import datetime


class TestSetupLogging:
    """Tests for setup_logging() function."""

    def test_setup_logging_returns_configured_logger(self, tmp_path):
        """setup_logging() should return a configured logger."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        assert logger is not None
        assert logger.name == "fairway"
        assert logger.level == logging.INFO

    def test_setup_logging_default_level_is_info(self, tmp_path):
        """Default log level should be INFO."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        assert logger.level == logging.INFO

    def test_setup_logging_custom_level(self, tmp_path):
        """Custom log level should be respected."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file), level="DEBUG")

        assert logger.level == logging.DEBUG

    def test_setup_logging_no_file_when_none(self):
        """No file handler when log_file=None."""
        from fairway.logging_config import setup_logging

        logger = setup_logging(log_file=None)

        # Should only have console handler (StreamHandler)
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0

    def test_setup_logging_creates_log_directory(self, tmp_path):
        """setup_logging should create the log directory if it doesn't exist."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "nested" / "logs" / "fairway.jsonl"
        setup_logging(log_file=str(log_file))

        assert log_file.parent.exists()


class TestJSONFormatter:
    """Tests for JSON log output format."""

    def test_json_file_handler_writes_valid_json_lines(self, tmp_path):
        """JSON file handler should write valid JSON lines."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        logger.info("Test message")

        # Flush handlers
        for handler in logger.handlers:
            handler.flush()

        # Read and parse the log file
        content = log_file.read_text().strip()
        assert content, "Log file should not be empty"

        log_entry = json.loads(content)
        assert "timestamp" in log_entry
        assert "level" in log_entry
        assert "message" in log_entry
        assert log_entry["level"] == "INFO"
        assert log_entry["message"] == "Test message"

    def test_json_formatter_includes_extra_fields(self, tmp_path):
        """JSON formatter should include extra fields like batch_id."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        logger.info("Processing batch", extra={
            "batch_id": "claims_CT_2023_abc12345",
            "partition_key": "state=CT/year=2023",
            "file_count": 24
        })

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text().strip()
        log_entry = json.loads(content)

        assert log_entry["batch_id"] == "claims_CT_2023_abc12345"
        assert log_entry["partition_key"] == "state=CT/year=2023"
        assert log_entry["file_count"] == 24

    def test_json_formatter_timestamp_is_iso_format(self, tmp_path):
        """Timestamp should be in ISO format."""
        from fairway.logging_config import setup_logging

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        logger.info("Test")

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text().strip()
        log_entry = json.loads(content)

        # Should be parseable as ISO datetime
        timestamp = log_entry["timestamp"]
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))


class TestConsoleHandler:
    """Tests for console (human-readable) output."""

    def test_console_handler_human_readable_format(self, tmp_path, capsys):
        """Console handler should output human-readable format."""
        from fairway.logging_config import setup_logging

        # Setup with no file to avoid file handler interference
        logger = setup_logging(log_file=None)

        logger.info("Test message for console")

        captured = capsys.readouterr()
        # Should contain level and message in human-readable format
        assert "INFO" in captured.err or "INFO" in captured.out
        assert "Test message for console" in captured.err or "Test message for console" in captured.out


class TestBatchLogger:
    """Tests for BatchLogger context manager."""

    def test_batch_logger_attaches_context(self, tmp_path):
        """BatchLogger should attach batch context to all log entries."""
        from fairway.logging_config import setup_logging, BatchLogger

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        with BatchLogger(logger, batch_id="batch_001", partition_key="state=CT/year=2023", file_count=10):
            logger.info("Processing started")
            logger.info("Processing completed")

        for handler in logger.handlers:
            handler.flush()

        lines = log_file.read_text().strip().split("\n")
        assert len(lines) == 2

        for line in lines:
            entry = json.loads(line)
            assert entry["batch_id"] == "batch_001"
            assert entry["partition_key"] == "state=CT/year=2023"
            assert entry["file_count"] == 10

    def test_batch_logger_context_cleared_after_exit(self, tmp_path):
        """BatchLogger context should be cleared after exiting."""
        from fairway.logging_config import setup_logging, BatchLogger

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        with BatchLogger(logger, batch_id="batch_001", partition_key="state=CT", file_count=5):
            logger.info("Inside context")

        logger.info("Outside context")

        for handler in logger.handlers:
            handler.flush()

        lines = log_file.read_text().strip().split("\n")
        assert len(lines) == 2

        inside_entry = json.loads(lines[0])
        outside_entry = json.loads(lines[1])

        assert inside_entry.get("batch_id") == "batch_001"
        # Outside context should not have batch_id (or it should be None)
        assert outside_entry.get("batch_id") is None or "batch_id" not in outside_entry

    def test_batch_logger_handles_exceptions(self, tmp_path):
        """BatchLogger should properly clean up even if exception raised."""
        from fairway.logging_config import setup_logging, BatchLogger

        log_file = tmp_path / "logs" / "fairway.jsonl"
        logger = setup_logging(log_file=str(log_file))

        try:
            with BatchLogger(logger, batch_id="batch_001", partition_key="state=CT", file_count=5):
                logger.info("Before exception")
                raise ValueError("Test error")
        except ValueError:
            pass

        logger.info("After exception")

        for handler in logger.handlers:
            handler.flush()

        lines = log_file.read_text().strip().split("\n")
        outside_entry = json.loads(lines[-1])

        # Context should be cleared
        assert outside_entry.get("batch_id") is None or "batch_id" not in outside_entry


class TestLogArchival:
    """Tests for log file archival on startup."""

    def test_log_archive_on_startup(self, tmp_path, monkeypatch):
        """Existing fairway.jsonl should be renamed with datetime suffix on startup."""
        from fairway.logging_config import setup_logging

        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "fairway.jsonl"

        # Create an existing log file
        log_file.write_text('{"timestamp": "2026-02-04T10:00:00", "level": "INFO", "message": "old log"}\n')

        # Mock datetime to get predictable archive name
        import fairway.logging_config as logging_config
        original_now = datetime.now

        class MockDatetime:
            @classmethod
            def now(cls):
                return datetime(2026, 2, 5, 10, 30, 0)

            @classmethod
            def fromisoformat(cls, s):
                return datetime.fromisoformat(s)

        monkeypatch.setattr(logging_config, "datetime", MockDatetime)

        # Setup logging - should archive the existing file
        setup_logging(log_file=str(log_file))

        # Check that the old file was archived
        archived_files = list(log_dir.glob("fairway_*.jsonl"))
        assert len(archived_files) == 1
        assert "2026-02-05T10-30-00" in archived_files[0].name

        # New log file should exist
        assert log_file.exists()

    def test_log_archive_preserves_old_content(self, tmp_path, monkeypatch):
        """Archived log should contain the original content."""
        from fairway.logging_config import setup_logging

        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "fairway.jsonl"

        original_content = '{"timestamp": "2026-02-04T10:00:00", "level": "INFO", "message": "old log"}\n'
        log_file.write_text(original_content)

        import fairway.logging_config as logging_config

        class MockDatetime:
            @classmethod
            def now(cls):
                return datetime(2026, 2, 5, 10, 30, 0)

            @classmethod
            def fromisoformat(cls, s):
                return datetime.fromisoformat(s)

        monkeypatch.setattr(logging_config, "datetime", MockDatetime)

        setup_logging(log_file=str(log_file))

        archived_files = list(log_dir.glob("fairway_*.jsonl"))
        archived_content = archived_files[0].read_text()
        assert archived_content == original_content

    def test_no_archive_when_no_existing_file(self, tmp_path):
        """No archive created when no existing log file."""
        from fairway.logging_config import setup_logging

        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "fairway.jsonl"

        # Don't create existing file
        setup_logging(log_file=str(log_file))

        archived_files = list(log_dir.glob("fairway_*.jsonl"))
        assert len(archived_files) == 0


class TestCLILoggingIntegration:
    """Tests for CLI logging integration."""

    def test_cli_run_has_log_file_option(self):
        """CLI run command should have --log-file option."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ['run', '--help'])

        assert '--log-file' in result.output
        assert result.exit_code == 0

    def test_cli_run_has_log_level_option(self):
        """CLI run command should have --log-level option."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ['run', '--help'])

        assert '--log-level' in result.output
        assert result.exit_code == 0

    def test_cli_run_creates_log_file(self, tmp_path):
        """CLI run should create JSONL log file when --log-file specified."""
        from click.testing import CliRunner
        from fairway.cli import main

        # Config resolves paths relative to its own directory (Phase 3:
        # config-dir-only resolution). Put `data/raw` inside config_dir so
        # the glob actually hits it.
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = config_dir / "data" / "raw"
        data_dir.mkdir(parents=True)
        (data_dir / "sample.csv").write_text("a,b\n1,2\n")
        (config_dir / "fairway.yaml").write_text("""
dataset_name: test
engine: duckdb
storage:
  root: data
tables:
  - name: test_table
    root: data/raw
    path: "*.csv"
    format: csv
""")

        log_file = tmp_path / "logs" / "test.jsonl"

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(main, [
                'run',
                '--config', str(config_dir / "fairway.yaml"),
                '--log-file', str(log_file),
                '--log-level', 'DEBUG',
                '--skip-summary',
            ])

        assert log_file.exists(), f"Log file not created. CLI output: {result.output}"


class TestCLILogsCommand:
    """Tests for `fairway logs` CLI command."""

    def test_cli_logs_command_exists(self):
        """CLI should have a logs command."""
        from click.testing import CliRunner
        from fairway.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ['logs', '--help'])

        assert result.exit_code == 0
        assert 'logs' in result.output.lower()

    def test_cli_logs_reads_jsonl_file(self, tmp_path):
        """logs command should read and display JSONL entries."""
        from click.testing import CliRunner
        from fairway.cli import main
        import json

        # Create test log file
        log_file = tmp_path / "test.jsonl"
        entries = [
            {"timestamp": "2026-02-06T10:00:00", "level": "INFO", "message": "Test message 1"},
            {"timestamp": "2026-02-06T10:00:01", "level": "ERROR", "message": "Test error"},
        ]
        with open(log_file, 'w') as f:
            for entry in entries:
                f.write(json.dumps(entry) + '\n')

        runner = CliRunner()
        result = runner.invoke(main, ['logs', '--file', str(log_file)])

        assert result.exit_code == 0
        assert 'Test message 1' in result.output
        assert 'Test error' in result.output

    def test_cli_logs_filter_by_level(self, tmp_path):
        """logs command should filter by log level."""
        from click.testing import CliRunner
        from fairway.cli import main
        import json

        log_file = tmp_path / "test.jsonl"
        entries = [
            {"timestamp": "2026-02-06T10:00:00", "level": "INFO", "message": "Info message"},
            {"timestamp": "2026-02-06T10:00:01", "level": "ERROR", "message": "Error message"},
            {"timestamp": "2026-02-06T10:00:02", "level": "DEBUG", "message": "Debug message"},
        ]
        with open(log_file, 'w') as f:
            for entry in entries:
                f.write(json.dumps(entry) + '\n')

        runner = CliRunner()
        result = runner.invoke(main, ['logs', '--file', str(log_file), '--level', 'ERROR'])

        assert result.exit_code == 0
        assert 'Error message' in result.output
        assert 'Info message' not in result.output
        assert 'Debug message' not in result.output

    def test_cli_logs_filter_by_batch_id(self, tmp_path):
        """logs command should filter by batch_id."""
        from click.testing import CliRunner
        from fairway.cli import main
        import json

        log_file = tmp_path / "test.jsonl"
        entries = [
            {"timestamp": "2026-02-06T10:00:00", "level": "INFO", "message": "Batch 1", "batch_id": "batch_001"},
            {"timestamp": "2026-02-06T10:00:01", "level": "INFO", "message": "Batch 2", "batch_id": "batch_002"},
            {"timestamp": "2026-02-06T10:00:02", "level": "INFO", "message": "No batch"},
        ]
        with open(log_file, 'w') as f:
            for entry in entries:
                f.write(json.dumps(entry) + '\n')

        runner = CliRunner()
        result = runner.invoke(main, ['logs', '--file', str(log_file), '--batch', 'batch_001'])

        assert result.exit_code == 0
        assert 'Batch 1' in result.output
        assert 'Batch 2' not in result.output

    def test_cli_logs_last_n_lines(self, tmp_path):
        """logs command should support --last N option."""
        from click.testing import CliRunner
        from fairway.cli import main
        import json

        log_file = tmp_path / "test.jsonl"
        entries = [
            {"timestamp": "2026-02-06T10:00:00", "level": "INFO", "message": f"Message {i}"}
            for i in range(10)
        ]
        with open(log_file, 'w') as f:
            for entry in entries:
                f.write(json.dumps(entry) + '\n')

        runner = CliRunner()
        result = runner.invoke(main, ['logs', '--file', str(log_file), '--last', '3'])

        assert result.exit_code == 0
        assert 'Message 7' in result.output
        assert 'Message 8' in result.output
        assert 'Message 9' in result.output
        assert 'Message 0' not in result.output


class TestPySparkLoggingLevels:
    """Tests for PySpark engine logging levels."""

    def test_max_records_warning_is_debug_level(self):
        """maxRecordsPerFile high value message should be DEBUG, not WARNING."""
        import logging
        from io import StringIO

        # Capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.DEBUG)

        logger = logging.getLogger("fairway.engines.pyspark")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            # Import after setting up logging to capture the message
            from fairway.engines.pyspark_engine import PySparkEngine

            # Check the source code for the log level used
            import inspect
            source = inspect.getsource(PySparkEngine.ingest)

            # The maxRecordsPerFile message should use logger.debug, not logger.warning
            assert 'logger.warning("maxRecordsPerFile' not in source, \
                "maxRecordsPerFile should use DEBUG level, not WARNING"

        finally:
            logger.removeHandler(handler)
