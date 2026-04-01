"""Additional CLI tests to increase coverage."""
import pytest
import json
import os
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from fairway.cli import (
    main, _validate_slurm_param, _validate_slurm_time,
    _validate_slurm_mem, discover_config,
)
import click


@pytest.fixture
def runner():
    return CliRunner()


class TestValidateSlurm:
    """Tests for Slurm parameter validation helpers."""

    def test_validate_slurm_param_none(self):
        assert _validate_slurm_param(None, 'test', r'^[a-z]+$') is None

    def test_validate_slurm_param_valid(self):
        assert _validate_slurm_param('abc', 'test', r'^[a-z]+$') == 'abc'

    def test_validate_slurm_param_too_long(self):
        with pytest.raises(click.ClickException, match="maximum length"):
            _validate_slurm_param('a' * 100, 'test', r'^[a-z]+$', max_length=10)

    def test_validate_slurm_param_invalid_chars(self):
        with pytest.raises(click.ClickException, match="disallowed characters"):
            _validate_slurm_param('abc;rm', 'test', r'^[a-z]+$')

    def test_validate_slurm_time_valid(self):
        assert _validate_slurm_time('12:00:00') == '12:00:00'

    def test_validate_slurm_time_with_days(self):
        assert _validate_slurm_time('2-12:00:00') == '2-12:00:00'

    def test_validate_slurm_time_short(self):
        assert _validate_slurm_time('1:30') == '1:30'

    def test_validate_slurm_time_invalid(self):
        with pytest.raises(click.ClickException, match="Invalid time format"):
            _validate_slurm_time('invalid')

    def test_validate_slurm_mem_valid_gb(self):
        assert _validate_slurm_mem('16G') == '16G'

    def test_validate_slurm_mem_valid_mb(self):
        assert _validate_slurm_mem('1024M') == '1024M'

    def test_validate_slurm_mem_bare_number(self):
        assert _validate_slurm_mem('1024') == '1024'

    def test_validate_slurm_mem_invalid(self):
        with pytest.raises(click.ClickException, match="Invalid memory format"):
            _validate_slurm_mem('16GB')


class TestDiscoverConfig:
    """Tests for config auto-discovery."""

    def test_no_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(click.ClickException, match="No config/ directory"):
            discover_config()

    def test_empty_config_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config").mkdir()
        with pytest.raises(click.ClickException, match="No config files"):
            discover_config()

    def test_single_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "fairway.yaml").write_text("dataset_name: test")
        result = discover_config()
        assert result == os.path.join("config", "fairway.yaml")

    def test_multiple_configs(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "a.yaml").write_text("test")
        (config_dir / "b.yaml").write_text("test")
        with pytest.raises(click.ClickException, match="Multiple"):
            discover_config()

    def test_excludes_schema_and_spark_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "fairway.yaml").write_text("dataset_name: test")
        (config_dir / "test_schema.yaml").write_text("schema")
        (config_dir / "spark.yaml").write_text("spark config")
        result = discover_config()
        assert result == os.path.join("config", "fairway.yaml")


class TestEjectCommand:
    """Tests for the eject command."""

    def test_eject_all(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])
            assert result.exit_code == 0
            assert "Ejecting container files" in result.output
            assert "Ejecting Slurm/HPC scripts" in result.output
            assert os.path.exists('Apptainer.def')
            assert os.path.exists('Dockerfile')
            assert os.path.exists('scripts/driver.sh')

    def test_eject_scripts_only(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '--scripts'])
            assert result.exit_code == 0
            assert os.path.exists('scripts/driver.sh')
            assert not os.path.exists('Apptainer.def')

    def test_eject_container_only(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '--container'])
            assert result.exit_code == 0
            assert os.path.exists('Apptainer.def')
            assert not os.path.exists('scripts/driver.sh')

    def test_eject_skip_existing(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('scripts', exist_ok=True)
            with open('Apptainer.def', 'w') as f:
                f.write('existing')
            result = runner.invoke(main, ['eject'])
            assert "Skipping (exists)" in result.output

    def test_eject_force_overwrite(self, runner):
        with runner.isolated_filesystem():
            with open('Apptainer.def', 'w') as f:
                f.write('existing')
            result = runner.invoke(main, ['eject', '--force'])
            assert result.exit_code == 0
            assert "Created" in result.output

    def test_eject_custom_output(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '-o', 'custom'])
            assert result.exit_code == 0
            assert os.path.exists('custom/Apptainer.def')

    def test_eject_all_exist_no_force(self, runner):
        with runner.isolated_filesystem():
            # First eject to create all files
            runner.invoke(main, ['eject'])
            # Second eject should skip all
            result = runner.invoke(main, ['eject'])
            assert "No files created" in result.output


class TestLogsCommand:
    """Tests for the logs command."""

    def test_logs_missing_file(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['logs', '--file', 'nonexistent.log'])
            assert result.exit_code != 0
            assert "not found" in result.output

    def test_logs_basic(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({"timestamp": "2024-01-01T00:00:00", "level": "INFO", "message": "test msg"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log'])
            assert result.exit_code == 0
            assert "test msg" in result.output

    def test_logs_filter_level(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({"level": "INFO", "message": "info msg"}) + "\n")
                f.write(json.dumps({"level": "ERROR", "message": "error msg"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--level', 'ERROR'])
            assert "error msg" in result.output
            assert "info msg" not in result.output

    def test_logs_errors_shortcut(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({"level": "INFO", "message": "info"}) + "\n")
                f.write(json.dumps({"level": "ERROR", "message": "err"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--errors'])
            assert "err" in result.output
            assert "info" not in result.output

    def test_logs_json_output(self, runner):
        with runner.isolated_filesystem():
            entry = {"timestamp": "2024-01-01T00:00:00", "level": "INFO", "message": "test"}
            with open('fairway.log', 'w') as f:
                f.write(json.dumps(entry) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--json'])
            assert result.exit_code == 0
            parsed = json.loads(result.output.strip())
            assert parsed["message"] == "test"

    def test_logs_last_n(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                for i in range(5):
                    f.write(json.dumps({"level": "INFO", "message": f"msg{i}"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--last', '2'])
            assert "msg3" in result.output
            assert "msg4" in result.output
            assert "msg0" not in result.output

    def test_logs_no_matches(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({"level": "INFO", "message": "test"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--level', 'ERROR'])
            assert "No matching" in result.output

    def test_logs_filter_batch_id(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({"level": "INFO", "message": "a", "batch_id": "batch_001"}) + "\n")
                f.write(json.dumps({"level": "INFO", "message": "b", "batch_id": "batch_002"}) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log', '--batch', 'batch_001'])
            assert "batch_001" in result.output
            assert "batch_002" not in result.output

    def test_logs_with_batch_id_in_output(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write(json.dumps({
                    "timestamp": "2024-01-01T00:00:00",
                    "level": "WARNING",
                    "message": "warn",
                    "batch_id": "b001"
                }) + "\n")
            result = runner.invoke(main, ['logs', '--file', 'fairway.log'])
            assert "b001" in result.output

    def test_logs_malformed_lines_skipped(self, runner):
        with runner.isolated_filesystem():
            with open('fairway.log', 'w') as f:
                f.write("not json\n")
                f.write(json.dumps({"level": "INFO", "message": "valid"}) + "\n")
                f.write("\n")  # empty line
            result = runner.invoke(main, ['logs', '--file', 'fairway.log'])
            assert "valid" in result.output


class TestInitCommand:
    """Tests for the init command."""

    def test_init_duckdb(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['init', 'myproject', '--engine', 'duckdb'])
            assert result.exit_code == 0
            assert "initialized successfully" in result.output
            assert os.path.isdir('myproject/config')
            assert os.path.isdir('myproject/data/raw')
            assert os.path.isdir('myproject/scripts')
            assert os.path.exists('myproject/config/fairway.yaml')
            assert os.path.exists('myproject/Makefile')
            assert os.path.exists('myproject/README.md')
            assert os.path.exists('myproject/scripts/driver.sh')

    def test_init_spark(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['init', 'sparkproj', '--engine', 'spark'])
            assert result.exit_code == 0
            assert os.path.exists('sparkproj/config/spark.yaml')
            content = open('sparkproj/config/fairway.yaml').read()
            assert 'pyspark' in content

    def test_init_creates_executable_scripts(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['init', 'proj', '--engine', 'duckdb'])
            assert result.exit_code == 0
            assert os.access('proj/scripts/driver.sh', os.X_OK)

    def test_init_creates_docs(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['init', 'proj', '--engine', 'duckdb'])
            assert result.exit_code == 0
            assert os.path.exists('proj/docs/getting-started.md')


class TestGenerateSchemaCommand:
    """Tests for generate-schema command."""

    def test_generate_schema_from_csv(self, runner):
        with runner.isolated_filesystem():
            with open('data.csv', 'w') as f:
                f.write("id,name,amount\n1,alice,10.5\n2,bob,20.0\n")
            os.makedirs('config', exist_ok=True)
            result = runner.invoke(main, ['generate-schema', 'data.csv'])
            assert result.exit_code == 0
            assert "Schema written" in result.output
            assert os.path.exists('config/data.csv_schema.yaml')

    def test_generate_schema_custom_output(self, runner):
        with runner.isolated_filesystem():
            with open('data.csv', 'w') as f:
                f.write("x,y\n1,2\n")
            result = runner.invoke(main, ['generate-schema', 'data.csv', '--output', 'my_schema.yaml'])
            assert result.exit_code == 0
            assert os.path.exists('my_schema.yaml')

    def test_generate_schema_file_not_found(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['generate-schema', 'nonexistent.csv'])
            assert "not found" in result.output or "Error" in result.output

    def test_generate_schema_no_args(self, runner):
        with runner.isolated_filesystem():
            result = runner.invoke(main, ['generate-schema'])
            assert "Must provide" in result.output or result.exit_code != 0

    def test_generate_schema_partitioned_dir(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('data/state=CT', exist_ok=True)
            with open('data/state=CT/part.csv', 'w') as f:
                f.write("id,val\n1,10\n")
            os.makedirs('config', exist_ok=True)
            result = runner.invoke(main, ['generate-schema', 'data'])
            assert result.exit_code == 0
            assert "partition" in result.output.lower() or "Schema written" in result.output


class TestRunCommand:
    """Tests for the run command."""

    def test_run_with_config(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('config')
            with open('config/test.yaml', 'w') as f:
                f.write("dataset_name: test\nengine: duckdb\n")

            with patch('fairway.pipeline.IngestionPipeline') as MockPipeline:
                result = runner.invoke(main, ['run', '--config', 'config/test.yaml'])
                assert result.exit_code == 0
                MockPipeline.assert_called_once()

    def test_run_auto_discover(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('config')
            with open('config/fairway.yaml', 'w') as f:
                f.write("dataset_name: test\nengine: duckdb\n")

            with patch('fairway.pipeline.IngestionPipeline') as MockPipeline:
                result = runner.invoke(main, ['run'])
                assert result.exit_code == 0

    def test_run_dry_run(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('config')
            with open('config/test.yaml', 'w') as f:
                f.write("dataset_name: test\nengine: duckdb\n")

            with patch('fairway.pipeline.IngestionPipeline') as MockPipeline:
                result = runner.invoke(main, ['run', '--config', 'config/test.yaml', '--dry-run'])
                assert result.exit_code == 0
                assert "DRY RUN" in result.output

    def test_run_skip_summary(self, runner):
        with runner.isolated_filesystem():
            os.makedirs('config')
            with open('config/test.yaml', 'w') as f:
                f.write("dataset_name: test\nengine: duckdb\n")

            with patch('fairway.pipeline.IngestionPipeline') as MockPipeline:
                result = runner.invoke(main, ['run', '--config', 'config/test.yaml', '--skip-summary'])
                assert result.exit_code == 0
                assert "skip" in result.output.lower()
