"""Tests for Apptainer/container functionality."""

import pytest
import os
from unittest.mock import patch, MagicMock
from click.testing import CliRunner


class TestSlurmSparkManagerApptainerDetection:
    """Tests for SlurmSparkManager._detect_apptainer_mode()."""

    def test_detect_apptainer_with_sif_file(self, tmp_path, monkeypatch):
        """Detects Apptainer mode when fairway.sif exists."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")

        manager = SlurmSparkManager({})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is True
        assert sif_path == str(tmp_path / "fairway.sif")

    def test_detect_apptainer_no_sif_file(self, tmp_path, monkeypatch):
        """Falls back to bare-metal when no sif file."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)

        manager = SlurmSparkManager({})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is False
        assert sif_path is None

    def test_detect_apptainer_from_env_var(self, tmp_path, monkeypatch):
        """Detects Apptainer mode from FAIRWAY_SIF env var."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        sif_file = tmp_path / "custom.sif"
        sif_file.write_text("fake sif")
        monkeypatch.setenv("FAIRWAY_SIF", str(sif_file))
        monkeypatch.chdir(tmp_path)

        manager = SlurmSparkManager({})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is True
        assert sif_path == str(sif_file)

    def test_detect_apptainer_config_override_true(self, tmp_path, monkeypatch):
        """Config use_apptainer=True forces Apptainer mode."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")

        manager = SlurmSparkManager({"use_apptainer": True})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is True

    def test_detect_apptainer_config_override_false(self, tmp_path, monkeypatch):
        """Config use_apptainer=False forces bare-metal mode."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")

        manager = SlurmSparkManager({"use_apptainer": False})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is False
        assert sif_path is None

    def test_detect_apptainer_config_true_no_sif_fallback(self, tmp_path, monkeypatch, capsys):
        """Config use_apptainer=True but no sif falls back with warning."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        # No sif file exists

        manager = SlurmSparkManager({"use_apptainer": True})
        use_apptainer, sif_path = manager._detect_apptainer_mode()

        assert use_apptainer is False
        assert sif_path is None


class TestTemplateLoading:
    """Tests for template loading from scripts/ subdirectory."""

    def test_load_spark_start_template(self):
        """SPARK_START_TEMPLATE loads correctly."""
        from fairway.templates import SPARK_START_TEMPLATE

        assert SPARK_START_TEMPLATE is not None
        assert "fairway-spark-start" in SPARK_START_TEMPLATE
        assert "FAIRWAY_SIF" in SPARK_START_TEMPLATE

    def test_load_driver_template(self):
        """DRIVER_TEMPLATE loads correctly."""
        from fairway.templates import DRIVER_TEMPLATE

        assert DRIVER_TEMPLATE is not None
        assert "fairway_driver" in DRIVER_TEMPLATE
        assert "USE_APPTAINER" in DRIVER_TEMPLATE

    def test_load_all_templates(self):
        """All templates load without error."""
        from fairway import templates

        # These should all be non-empty strings
        assert templates.HPC_SCRIPT
        assert templates.APPTAINER_DEF
        assert templates.DOCKERFILE_TEMPLATE
        assert templates.DOCKERIGNORE
        assert templates.MAKEFILE_TEMPLATE
        assert templates.CONFIG_TEMPLATE
        assert templates.SPARK_YAML_TEMPLATE
        assert templates.TRANSFORM_TEMPLATE
        assert templates.DRIVER_TEMPLATE
        assert templates.DRIVER_SCHEMA_TEMPLATE
        assert templates.SPARK_START_TEMPLATE
        assert templates.README_TEMPLATE
        assert templates.DOCS_TEMPLATE


class TestApptainerDefContent:
    """Tests for Apptainer.def content correctness."""

    def test_apptainer_def_no_nextflow(self):
        """Apptainer.def should not contain Nextflow."""
        from fairway.templates import APPTAINER_DEF

        assert "nextflow" not in APPTAINER_DEF.lower()

    def test_apptainer_def_has_spark(self):
        """Apptainer.def should install Spark."""
        from fairway.templates import APPTAINER_DEF

        assert "SPARK_VERSION" in APPTAINER_DEF
        assert "/opt/spark" in APPTAINER_DEF

    def test_apptainer_def_has_venv(self):
        """Apptainer.def should use venv for isolation."""
        from fairway.templates import APPTAINER_DEF

        assert "/opt/venv" in APPTAINER_DEF
        assert "PYSPARK_PYTHON" in APPTAINER_DEF

    def test_apptainer_def_has_test_section(self):
        """Apptainer.def should have %test section."""
        from fairway.templates import APPTAINER_DEF

        assert "%test" in APPTAINER_DEF
        assert "fairway --version" in APPTAINER_DEF

    def test_apptainer_def_git_ref_param(self):
        """Apptainer.def should support FAIRWAY_GIT_REF."""
        from fairway.templates import APPTAINER_DEF

        assert "FAIRWAY_GIT_REF" in APPTAINER_DEF


class TestDockerfileContent:
    """Tests for Dockerfile content correctness."""

    def test_dockerfile_no_nextflow(self):
        """Dockerfile should not contain Nextflow."""
        from fairway.templates import DOCKERFILE_TEMPLATE

        assert "nextflow" not in DOCKERFILE_TEMPLATE.lower()

    def test_dockerfile_has_spark(self):
        """Dockerfile should install Spark."""
        from fairway.templates import DOCKERFILE_TEMPLATE

        assert "SPARK_VERSION" in DOCKERFILE_TEMPLATE
        assert "/opt/spark" in DOCKERFILE_TEMPLATE

    def test_dockerfile_has_venv(self):
        """Dockerfile should use venv for isolation."""
        from fairway.templates import DOCKERFILE_TEMPLATE

        assert "/opt/venv" in DOCKERFILE_TEMPLATE

    def test_dockerfile_git_ref_arg(self):
        """Dockerfile should support FAIRWAY_GIT_REF build arg."""
        from fairway.templates import DOCKERFILE_TEMPLATE

        assert "FAIRWAY_GIT_REF" in DOCKERFILE_TEMPLATE
        assert "ARG FAIRWAY_GIT_REF" in DOCKERFILE_TEMPLATE


class TestEjectCommand:
    """Tests for fairway eject command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_eject_creates_apptainer_def(self, runner):
        """fairway eject creates Apptainer.def."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0
            assert os.path.exists('Apptainer.def')

            with open('Apptainer.def') as f:
                content = f.read()
            assert "Bootstrap: docker" in content

    def test_eject_creates_dockerfile(self, runner):
        """fairway eject creates Dockerfile."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0
            assert os.path.exists('Dockerfile')

            with open('Dockerfile') as f:
                content = f.read()
            assert "FROM python" in content

    def test_eject_creates_dockerignore(self, runner):
        """fairway eject creates .dockerignore."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0
            assert os.path.exists('.dockerignore')


class TestGetApptainerBinds:
    """Tests for _get_apptainer_binds() function."""

    def test_binds_include_storage_dirs(self, tmp_path):
        """Bind paths include configured storage directories."""
        from fairway.cli import _get_apptainer_binds
        from unittest.mock import MagicMock

        # Create directories
        raw_dir = tmp_path / "data" / "raw"
        processed_dir = tmp_path / "data" / "processed"
        raw_dir.mkdir(parents=True)
        processed_dir.mkdir(parents=True)

        cfg = MagicMock()
        cfg.raw_dir = str(raw_dir)
        cfg.processed_dir = str(processed_dir)
        cfg.curated_dir = None
        cfg.tables = []

        binds = _get_apptainer_binds(cfg)

        assert str(raw_dir) in binds
        assert str(processed_dir) in binds

    def test_binds_include_table_paths(self, tmp_path):
        """Bind paths include table file directories."""
        from fairway.cli import _get_apptainer_binds
        from unittest.mock import MagicMock

        # Create a table file
        table_dir = tmp_path / "tables"
        table_dir.mkdir()
        table_file = table_dir / "data.csv"
        table_file.write_text("a,b,c")

        cfg = MagicMock()
        cfg.raw_dir = None
        cfg.processed_dir = None
        cfg.curated_dir = None
        cfg.tables = [{"path": str(table_file)}]

        binds = _get_apptainer_binds(cfg)

        # Should include the directory containing the file
        assert str(table_dir) in binds

    def test_binds_skip_nonexistent_paths(self, tmp_path):
        """Bind paths skip directories that don't exist."""
        from fairway.cli import _get_apptainer_binds
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.raw_dir = str(tmp_path / "nonexistent")
        cfg.processed_dir = None
        cfg.curated_dir = None
        cfg.tables = []

        binds = _get_apptainer_binds(cfg)

        assert str(tmp_path / "nonexistent") not in binds


class TestSparkStartScript:
    """Tests for fairway-spark-start.sh content."""

    def test_script_supports_container_mode(self):
        """Script supports FAIRWAY_SIF container mode."""
        from fairway.templates import SPARK_START_TEMPLATE

        assert "FAIRWAY_SIF" in SPARK_START_TEMPLATE
        assert "USE_CONTAINER" in SPARK_START_TEMPLATE
        assert "apptainer exec" in SPARK_START_TEMPLATE

    def test_script_supports_baremetal_mode(self):
        """Script supports bare-metal mode."""
        from fairway.templates import SPARK_START_TEMPLATE

        assert "Bare-metal mode" in SPARK_START_TEMPLATE

    def test_script_validates_slurm_env(self):
        """Script validates Slurm environment variables."""
        from fairway.templates import SPARK_START_TEMPLATE

        assert "SLURM_JOB_ID" in SPARK_START_TEMPLATE
        assert "SLURM_CPUS_PER_TASK" in SPARK_START_TEMPLATE

    def test_script_starts_workers(self):
        """Script starts Spark workers via srun."""
        from fairway.templates import SPARK_START_TEMPLATE

        assert "srun" in SPARK_START_TEMPLATE
        assert "sparkworker.sh" in SPARK_START_TEMPLATE
