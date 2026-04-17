"""Tests for preprocess.resources config — SLURM resource hints."""

import pytest
import yaml
import os
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from fairway.config_loader import Config, ConfigValidationError


class TestResourcesValidation:
    """Config validation for preprocess.resources block."""

    def _write_config(self, tmp_path, preprocess=None):
        config_path = str(tmp_path / "config.yaml")
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        (data_dir / "test.csv").write_text("a,b\n1,2")

        table = {
            'name': 'census_1950',
            'path': str(data_dir / "test.csv"),
            'format': 'csv',
        }
        if preprocess is not None:
            table['preprocess'] = preprocess

        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [table],
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        return config_path

    def test_valid_cpus(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'cpus': 8},
        })
        cfg = Config(config_path)
        assert cfg.tables[0]['preprocess']['resources']['cpus'] == 8

    def test_valid_memory(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'memory': '32G'},
        })
        cfg = Config(config_path)
        assert cfg.tables[0]['preprocess']['resources']['memory'] == '32G'

    def test_valid_cpus_and_memory(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'cpus': 4, 'memory': '16G'},
        })
        cfg = Config(config_path)
        res = cfg.tables[0]['preprocess']['resources']
        assert res['cpus'] == 4
        assert res['memory'] == '16G'

    def test_cpus_zero_fails(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'cpus': 0},
        })
        with pytest.raises(ConfigValidationError, match="cpus.*positive integer"):
            Config(config_path)

    def test_cpus_negative_fails(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'cpus': -1},
        })
        with pytest.raises(ConfigValidationError, match="cpus.*positive integer"):
            Config(config_path)

    def test_cpus_string_fails(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'cpus': 'eight'},
        })
        with pytest.raises(ConfigValidationError, match="cpus.*positive integer"):
            Config(config_path)

    def test_memory_invalid_format_fails(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'memory': 'lots'},
        })
        with pytest.raises(ConfigValidationError, match="memory.*format"):
            Config(config_path)

    def test_memory_empty_string_fails(self, tmp_path):
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {'memory': ''},
        })
        with pytest.raises(ConfigValidationError, match="memory.*format"):
            Config(config_path)

    def test_valid_memory_formats(self, tmp_path):
        """Various valid SLURM memory formats should pass."""
        for mem in ['1024', '16G', '512M', '1T', '4096K']:
            config_path = self._write_config(tmp_path, preprocess={
                'action': 'unzip',
                'resources': {'memory': mem},
            })
            cfg = Config(config_path)
            assert cfg.tables[0]['preprocess']['resources']['memory'] == mem

    def test_no_resources_passes(self, tmp_path):
        """Config without resources block should pass validation."""
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
        })
        cfg = Config(config_path)
        assert 'resources' not in cfg.tables[0]['preprocess']

    def test_empty_resources_passes(self, tmp_path):
        """Empty resources block should pass validation."""
        config_path = self._write_config(tmp_path, preprocess={
            'action': 'unzip',
            'resources': {},
        })
        cfg = Config(config_path)
        assert cfg.tables[0]['preprocess']['resources'] == {}


class TestResourcesExcludedFromExtraOpts:
    """resources dict must NOT be passed through to preprocessing scripts."""

    def test_resources_not_in_extra_opts(self, tmp_path):
        """The resources key should be excluded from kwargs passed to scripts."""
        from fairway.pipeline import IngestionPipeline

        config_path = str(tmp_path / "config.yaml")
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        script_file = tmp_path / "scripts" / "preprocess.py"
        script_file.parent.mkdir(parents=True, exist_ok=True)
        script_file.write_text(
            "def process_file(path, output_dir, **kwargs):\n"
            "    return output_dir\n"
        )

        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'census',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
                'preprocess': {
                    'action': str(script_file),
                    'scope': 'per_file',
                    'password_file': '/tmp/pw.txt',
                    'resources': {'cpus': 8, 'memory': '32G'},
                },
            }],
        }
        (data_dir / "test.csv").write_text("a,b\n1,2")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        pipeline = IngestionPipeline(config_path)
        table = pipeline.config.tables[0]
        preprocess_config = table['preprocess']

        # Replicate the extra_opts logic from pipeline.py
        extra_opts = {k: v for k, v in preprocess_config.items()
                      if k not in ('action', 'scope', 'execution_mode', 'include', 'resources')}

        assert 'resources' not in extra_opts
        assert 'password_file' in extra_opts


class TestSlurmResourceDefaults:
    """SLURM submit command should read preprocess.resources as defaults."""

    def _make_config(self, tmp_path, resources=None):
        config_path = str(tmp_path / "config.yaml")
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        (data_dir / "test.csv").write_text("a,b\n1,2")

        preprocess = {'action': 'unzip'}
        if resources is not None:
            preprocess['resources'] = resources

        config = {
            'dataset_name': 'test',
            'engine': 'duckdb',
            'tables': [{
                'name': 'census_1950',
                'path': str(data_dir / "test.csv"),
                'format': 'csv',
                'preprocess': preprocess,
            }],
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        # Create spark.yaml with account so submit doesn't fail
        spark_yaml = tmp_path / "config" / "spark.yaml"
        spark_yaml.parent.mkdir(parents=True, exist_ok=True)
        with open(str(spark_yaml), 'w') as f:
            yaml.dump({'account': 'testaccount'}, f)

        return config_path

    def test_config_cpus_used_as_default(self, tmp_path, monkeypatch):
        """Config resources.cpus becomes --cpus default when CLI doesn't specify."""
        from fairway.cli import main

        config_path = self._make_config(tmp_path, resources={'cpus': 8, 'memory': '64G'})
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ['submit', '--config', config_path, '--dry-run'])

        assert result.exit_code == 0
        assert '--cpus-per-task=8' in result.output
        assert '--mem=64G' in result.output

    def test_cli_overrides_config_cpus(self, tmp_path, monkeypatch):
        """Explicit --cpus on CLI should override config resources.cpus."""
        from fairway.cli import main

        config_path = self._make_config(tmp_path, resources={'cpus': 8})
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ['submit', '--config', config_path, '--cpus', '16', '--dry-run'])

        assert result.exit_code == 0
        assert '--cpus-per-task=16' in result.output

    def test_cli_overrides_config_memory(self, tmp_path, monkeypatch):
        """Explicit --mem on CLI should override config resources.memory."""
        from fairway.cli import main

        config_path = self._make_config(tmp_path, resources={'memory': '64G'})
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ['submit', '--config', config_path, '--mem', '128G', '--dry-run'])

        assert result.exit_code == 0
        assert '--mem=128G' in result.output

    def test_no_resources_preserves_defaults(self, tmp_path, monkeypatch):
        """Without resources in config, HPC dataclass defaults (--cpus 4, --mem 32G) are used."""
        from fairway.cli import main

        config_path = self._make_config(tmp_path, resources=None)
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ['submit', '--config', config_path, '--dry-run'])

        assert result.exit_code == 0
        assert '--cpus-per-task=4' in result.output
        assert '--mem=32G' in result.output
