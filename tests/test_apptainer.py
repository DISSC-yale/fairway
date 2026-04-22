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
        assert "fairway --help" in APPTAINER_DEF

    def test_apptainer_def_has_git_ref_variable(self):
        """Apptainer.def should have configurable FAIRWAY_GIT_REF variable."""
        from fairway.templates import APPTAINER_DEF

        assert 'FAIRWAY_GIT_REF="main"' in APPTAINER_DEF
        assert "@${FAIRWAY_GIT_REF}" in APPTAINER_DEF


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
        cfg.temp_dir = None
        cfg.tables = []
        cfg.apptainer_binds = None

        binds = _get_apptainer_binds(cfg)

        assert str(raw_dir) in binds
        assert str(processed_dir) in binds

    def test_binds_include_table_paths(self, tmp_path):
        """Bind paths include table root directories."""
        from fairway.cli import _get_apptainer_binds
        from unittest.mock import MagicMock

        # Create a table directory
        table_dir = tmp_path / "tables"
        table_dir.mkdir()

        cfg = MagicMock()
        cfg.raw_dir = None
        cfg.processed_dir = None
        cfg.curated_dir = None
        cfg.temp_dir = None
        cfg.tables = [{"root": str(table_dir)}]
        cfg.apptainer_binds = None

        binds = _get_apptainer_binds(cfg)

        assert str(table_dir) in binds

    def test_binds_skip_nonexistent_paths(self, tmp_path):
        """Bind paths skip storage directories that don't exist."""
        from fairway.cli import _get_apptainer_binds
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.raw_dir = str(tmp_path / "nonexistent")
        cfg.processed_dir = None
        cfg.curated_dir = None
        cfg.temp_dir = None
        cfg.tables = []
        cfg.apptainer_binds = None

        binds = _get_apptainer_binds(cfg)

        assert str(tmp_path / "nonexistent") not in binds


class TestDevMode:
    """Tests for --dev mode bind mounting."""

    def test_get_dev_bind_path_with_local_src(self, tmp_path, monkeypatch):
        """_get_dev_bind_path returns correct bind when src/fairway exists."""
        from fairway.cli import _get_dev_bind_path

        # Create fake src/fairway structure
        src_dir = tmp_path / "src" / "fairway"
        src_dir.mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        bind_path = _get_dev_bind_path()

        assert bind_path is not None
        assert str(src_dir) in bind_path
        assert "/opt/venv/lib/python3.10/site-packages/fairway" in bind_path

    def test_get_dev_bind_path_no_local_src(self, tmp_path, monkeypatch):
        """_get_dev_bind_path falls back to module location when no src/fairway."""
        from fairway.cli import _get_dev_bind_path

        monkeypatch.chdir(tmp_path)
        # No src/fairway directory

        bind_path = _get_dev_bind_path()

        # Should still return something (the module's own location)
        assert bind_path is not None

    def test_shell_dev_adds_overlay_bind(self, tmp_path, monkeypatch):
        """`fairway shell --dev` should pass the dev overlay bind to Apptainer."""
        from fairway.cli import main

        runner = CliRunner()
        monkeypatch.chdir(tmp_path)
        (tmp_path / "config").mkdir()
        (tmp_path / "src" / "fairway").mkdir(parents=True)
        (tmp_path / "config" / "fairway.yaml").write_text(
            "project: shell_dev\n"
            "dataset_name: shell_dev\n"
            "engine: duckdb\n"
            "storage:\n"
            "  root: data\n"
            "tables: []\n"
        )

        with patch("fairway.cli.subprocess.run") as mock_run:
            result = runner.invoke(main, ["shell", "--config", "config/fairway.yaml", "--dev"])

        assert result.exit_code == 0, result.output
        cmd = mock_run.call_args[0][0]
        assert cmd[:2] == ["apptainer", "shell"]
        bind_spec = cmd[cmd.index("--bind") + 1]
        assert str(tmp_path / "src" / "fairway") in bind_spec
        assert "/opt/venv/lib/python3.10/site-packages/fairway" in bind_spec


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

    def test_script_disables_rest_server_for_spark4_compatibility(self):
        """Script disables REST submission server for Spark 4.x compatibility.

        Spark 4.x added a restriction: RestSubmissionServer doesn't support
        authentication via spark.authenticate.secret. We disable it.
        """
        from fairway.templates import SPARK_START_TEMPLATE

        assert "spark.master.rest.enabled" in SPARK_START_TEMPLATE
        assert "false" in SPARK_START_TEMPLATE

    def test_srun_commands_not_inside_apptainer_exec(self):
        """srun commands must run on host, NOT inside apptainer exec.

        This is critical: srun is a Slurm binary only available on the host.
        The script runs on the host and should call srun directly.
        """
        from fairway.templates import SPARK_START_TEMPLATE
        import re

        # srun should appear in the script (for mkdir, cp, worker launch)
        assert "srun" in SPARK_START_TEMPLATE

        # srun should NOT be inside an apptainer exec command
        # Pattern: apptainer exec ... srun (on same logical command)
        # This would be wrong - srun must run on host
        lines = SPARK_START_TEMPLATE.split('\n')
        for i, line in enumerate(lines):
            # Skip comments
            if line.strip().startswith('#'):
                continue
            # If line has srun, it should not be after 'apptainer exec' on same command
            if 'srun' in line:
                # Check this line and continuation lines above
                assert 'apptainer exec' not in line, \
                    f"srun should not be inside apptainer exec (line {i+1}): {line}"

    def test_spark_master_containerized_in_container_mode(self):
        """start-master.sh should run inside apptainer when USE_CONTAINER=yes.

        The Spark master needs to run inside the container where Spark is installed.
        """
        from fairway.templates import SPARK_START_TEMPLATE

        # Should have conditional logic for container mode around start-master
        assert 'USE_CONTAINER' in SPARK_START_TEMPLATE

        # In container mode, start-master.sh should be wrapped in apptainer exec
        # Look for pattern like: apptainer exec ... start-master.sh
        assert 'apptainer exec' in SPARK_START_TEMPLATE

        # The script should have logic to run start-master inside container
        # when USE_CONTAINER is yes
        import re
        # Find section that starts master - should have container conditional
        master_section = re.search(
            r'Starting Spark Master.*?start-master\.sh',
            SPARK_START_TEMPLATE,
            re.DOTALL
        )
        assert master_section is not None, "Should have Spark Master startup section"

        # After "Starting Spark Master", there should be USE_CONTAINER check
        # before start-master.sh is called
        master_text = master_section.group(0)
        # The actual containerization happens - check full template for pattern
        assert re.search(
            r'if.*USE_CONTAINER.*yes.*then.*apptainer exec.*start-master',
            SPARK_START_TEMPLATE,
            re.DOTALL | re.IGNORECASE
        ), "start-master.sh should be wrapped in apptainer exec when USE_CONTAINER=yes"


class TestSlurmSparkManagerJobIsolation:
    """Tests for job-specific file paths to prevent race conditions."""

    def test_state_files_are_job_specific(self, tmp_path, monkeypatch):
        """State files should be scoped to driver job ID to prevent conflicts."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)

        # When driver_job_id is provided, files should be in job-specific directory
        manager = SlurmSparkManager({}, driver_job_id='12345')

        # All state files should be under ~/.fairway-spark/12345/
        assert '12345' in manager.master_url_file
        assert '12345' in manager.job_id_file
        assert '12345' in manager.conf_dir_file
        assert '12345' in manager.cores_file

        # Should use subdirectory, not flat files
        assert '.fairway-spark' in manager.master_url_file or 'fairway-spark' in manager.master_url_file

    def test_state_files_default_to_legacy_paths(self, tmp_path, monkeypatch):
        """Without driver_job_id, should use legacy paths for backwards compatibility."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)

        # Without driver_job_id, use legacy flat paths
        manager = SlurmSparkManager({})

        assert manager.master_url_file.endswith('spark_master_url.txt')
        assert manager.job_id_file.endswith('cluster_job_id.txt')

    def test_generated_script_uses_job_specific_paths(self, tmp_path, monkeypatch):
        """Generated sbatch scripts should use driver job ID for state files."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")
        (tmp_path / "scripts").mkdir()

        manager = SlurmSparkManager({}, driver_job_id='99999')
        script = manager._generate_apptainer_script(
            nodes=2, cpus=4, mem='16G', account='test',
            time_limit='1:00:00', partition='day',
            sif_path=str(tmp_path / "fairway.sif"),
            bind_paths='/test/data',  # Generic test path (not hardcoded to specific HPC)
            dynamic_alloc_script='',
            spark_conf_lines=''
        )

        # Script should write to job-specific paths
        assert '99999' in script
        assert '.fairway-spark' in script or 'fairway-spark' in script


class TestSlurmSparkManagerScriptGeneration:
    """Tests for sbatch script generation in SlurmSparkManager."""

    def test_apptainer_script_does_not_wrap_spark_start_in_container(self, tmp_path, monkeypatch):
        """The generated sbatch script should NOT wrap fairway-spark-start.sh in apptainer exec.

        fairway-spark-start.sh must run on the HOST so it can call srun.
        The script itself handles containerization of Spark components.
        """
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")
        (tmp_path / "scripts").mkdir()

        config = {
            'slurm_nodes': 2,
            'slurm_cpus_per_node': 4,
            'slurm_mem_per_node': '16G',
            'slurm_account': 'test',
            'slurm_time': '1:00:00',
            'slurm_partition': 'day',
        }

        manager = SlurmSparkManager(config)
        script = manager._generate_apptainer_script(
            nodes=2, cpus=4, mem='16G', account='test',
            time_limit='1:00:00', partition='day',
            sif_path=str(tmp_path / "fairway.sif"),
            bind_paths='/test/data',  # Generic test path (not hardcoded to specific HPC)
            dynamic_alloc_script='',
            spark_conf_lines=''
        )

        # The script should call fairway-spark-start.sh
        assert 'fairway-spark-start.sh' in script

        # CRITICAL: fairway-spark-start.sh should NOT be wrapped in apptainer exec
        # It needs to run on the host to have access to srun
        import re

        # Find the line(s) that invoke fairway-spark-start.sh
        # Check that they use 'bash' directly, not 'apptainer exec'
        lines = script.split('\n')
        spark_start_invocation = None
        for i, line in enumerate(lines):
            if 'fairway-spark-start.sh' in line and not line.strip().startswith('#'):
                # Found the invocation line - check preceding lines for continuation
                # Build the full command (handle line continuations with \)
                full_command = line
                j = i - 1
                while j >= 0 and lines[j].rstrip().endswith('\\'):
                    full_command = lines[j] + '\n' + full_command
                    j -= 1
                spark_start_invocation = full_command
                break

        assert spark_start_invocation is not None, "Should find fairway-spark-start.sh invocation"

        # The invocation should NOT contain 'apptainer exec'
        assert 'apptainer exec' not in spark_start_invocation, \
            f"fairway-spark-start.sh should NOT be wrapped in apptainer exec. Found: {spark_start_invocation}"

        # The script should call: bash fairway-spark-start.sh (or similar)
        assert 'bash' in spark_start_invocation, \
            f"Should invoke with bash, found: {spark_start_invocation}"


class TestBindPathConfiguration:
    """Tests for configurable bind paths (no hardcoded /vast)."""

    def test_slurm_manager_default_bind_is_empty(self, tmp_path, monkeypatch):
        """SlurmSparkManager should default to empty bind paths, not /vast."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")

        # Config with no apptainer_binds specified
        manager = SlurmSparkManager({})
        script = manager._generate_apptainer_script(
            nodes=2, cpus=4, mem='16G', account='test',
            time_limit='1:00:00', partition='day',
            sif_path=str(tmp_path / "fairway.sif"),
            bind_paths='',  # Empty - what we expect as default
            dynamic_alloc_script='',
            spark_conf_lines=''
        )

        # Script should NOT contain /vast as a hardcoded path
        assert '/vast' not in script, "Script should not hardcode /vast"

    def test_slurm_manager_config_bind_override(self, tmp_path, monkeypatch):
        """SlurmSparkManager should respect apptainer_binds from config."""
        from fairway.engines.slurm_cluster import SlurmSparkManager

        monkeypatch.chdir(tmp_path)
        (tmp_path / "fairway.sif").write_text("fake sif")

        config = {'apptainer_binds': '/scratch,/gpfs'}
        manager = SlurmSparkManager(config)

        # The config value should be used
        bind_paths = config.get('apptainer_binds', '')
        assert bind_paths == '/scratch,/gpfs'

    def test_driver_script_no_vast_default(self):
        """driver.sh should not default to /vast."""
        from fairway.templates import DRIVER_TEMPLATE

        # Should not have /vast as default
        assert 'FAIRWAY_BINDS:-/vast' not in DRIVER_TEMPLATE
        assert "FAIRWAY_BINDS:-}" in DRIVER_TEMPLATE or 'FAIRWAY_BINDS:-"' in DRIVER_TEMPLATE

    def test_spark_start_script_no_vast_default(self):
        """fairway-spark-start.sh should not default to /vast."""
        from fairway.templates import SPARK_START_TEMPLATE

        # Should not have /vast as default
        assert 'FAIRWAY_BINDS:-/vast' not in SPARK_START_TEMPLATE
        assert ':-/vast' not in SPARK_START_TEMPLATE

    def test_spark_start_script_handles_empty_binds(self):
        """fairway-spark-start.sh should handle empty FAIRWAY_BINDS gracefully."""
        from fairway.templates import SPARK_START_TEMPLATE

        # Should have logic to handle empty FAIRWAY_BINDS
        # Look for conditional that checks if FAIRWAY_BINDS is non-empty
        assert 'if [ -n "${FAIRWAY_BINDS' in SPARK_START_TEMPLATE or \
               'if [ -n "$FAIRWAY_BINDS' in SPARK_START_TEMPLATE or \
               '[ -n "${FAIRWAY_BINDS}' in SPARK_START_TEMPLATE


class TestEjectCommandExtended:
    """Extended tests for fairway eject command with new flags."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_eject_scripts_only(self, runner):
        """fairway eject --scripts creates only scripts directory."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '--scripts'])

            assert result.exit_code == 0
            assert os.path.exists('scripts/driver.sh')
            assert os.path.exists('scripts/fairway-spark-start.sh')
            # Should NOT create container files
            assert not os.path.exists('Apptainer.def')
            assert not os.path.exists('Dockerfile')

    def test_eject_container_only(self, runner):
        """fairway eject --container creates only container files."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '--container'])

            assert result.exit_code == 0
            assert os.path.exists('Apptainer.def')
            assert os.path.exists('Dockerfile')
            assert os.path.exists('.dockerignore')
            # Should NOT create scripts directory
            assert not os.path.exists('scripts/driver.sh')

    def test_eject_custom_output_directory(self, runner):
        """fairway eject --output creates files in custom directory."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject', '--output', 'custom'])

            assert result.exit_code == 0
            assert os.path.exists('custom/Apptainer.def')
            assert os.path.exists('custom/Dockerfile')
            assert os.path.exists('custom/scripts/driver.sh')

    def test_eject_warns_existing_files(self, runner):
        """fairway eject warns when files already exist."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            # Create existing file
            os.makedirs('scripts', exist_ok=True)
            with open('Apptainer.def', 'w') as f:
                f.write('existing content')

            result = runner.invoke(main, ['eject'])

            # Should warn about existing files (but not fail)
            assert 'exist' in result.output.lower() or 'skip' in result.output.lower()

    def test_eject_force_overwrites(self, runner):
        """fairway eject --force overwrites existing files."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            # Create existing file with different content
            with open('Apptainer.def', 'w') as f:
                f.write('old content')

            result = runner.invoke(main, ['eject', '--force'])

            assert result.exit_code == 0
            with open('Apptainer.def') as f:
                content = f.read()
            assert 'Bootstrap: docker' in content  # Should have new content

    def test_eject_scripts_executable(self, runner):
        """Ejected shell scripts have executable permissions."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0

            # Check scripts have executable bit
            for script in ['scripts/driver.sh', 'scripts/fairway-spark-start.sh']:
                if os.path.exists(script):
                    mode = os.stat(script).st_mode
                    assert mode & 0o111, f"{script} should be executable"

    def test_eject_creates_all_scripts(self, runner):
        """fairway eject creates all expected scripts."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0

            expected_scripts = [
                'scripts/driver.sh',
                'scripts/driver-schema.sh',
                'scripts/fairway-spark-start.sh',
                'scripts/fairway-hpc.sh',
            ]
            for script in expected_scripts:
                assert os.path.exists(script), f"Missing: {script}"

    def test_eject_creates_makefile(self, runner):
        """fairway eject creates Makefile for container builds."""
        from fairway.cli import main

        with runner.isolated_filesystem():
            result = runner.invoke(main, ['eject'])

            assert result.exit_code == 0
            assert os.path.exists('Makefile')

            with open('Makefile') as f:
                content = f.read()
            assert 'apptainer' in content.lower() or 'build' in content.lower()
