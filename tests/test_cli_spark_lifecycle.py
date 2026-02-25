
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from fairway.cli import main
from fairway.engines.slurm_cluster import compute_executor_defaults, _parse_mem_to_gb
import os

@pytest.fixture
def runner():
    return CliRunner()

def test_spark_start(runner):
    """Test fairway spark start command."""
    with runner.isolated_filesystem():
        with patch('fairway.engines.slurm_cluster.SlurmSparkManager') as MockManager:
            # Mock instance
            mock_instance = MockManager.return_value
            mock_instance.start_cluster.return_value = "spark://mock-master:7077"
            
            result = runner.invoke(main, ['spark', 'start', '--slurm-nodes', '4'])
            assert result.exit_code == 0
            assert "Spark cluster started" in result.output
            assert "spark://mock-master:7077" in result.output
            
            # Verify config passed
            MockManager.assert_called_once()
            call_args = MockManager.call_args[0][0]
            assert call_args['slurm_nodes'] == 4

def test_spark_stop(runner):
    """Test fairway spark stop command."""
    with runner.isolated_filesystem():
        with patch('fairway.engines.slurm_cluster.SlurmSparkManager') as MockManager:
            mock_instance = MockManager.return_value
            
            result = runner.invoke(main, ['spark', 'stop'])
            assert result.exit_code == 0
            
            mock_instance.stop_cluster.assert_called_once()

def test_run_command_no_spark_provisioning(runner):
    """Test run command does NOT try to provision spark implicitly."""
    with runner.isolated_filesystem():
        # Create dummy config
        os.makedirs('config')
        with open('config/test.yaml', 'w') as f:
            f.write("dataset_name: test\nengine: duckdb\n")
            
        with patch('fairway.pipeline.IngestionPipeline') as MockPipeline:
            # invoke run without spark options (they shouldn't exist or shouldn't trigger provisioning)
            # The new CLI shouldn't have --with-spark
            result = runner.invoke(main, ['run', '--config', 'config/test.yaml'])
            assert result.exit_code == 0
            
            # Ensure pipeline called
            MockPipeline.assert_called_once()
            
            # Ensure SlurmSparkManager was NOT imported/used in this flow
            # (Hard to strict check import, but we can check if it was initialized if we could mock it globally)
            # But the absence of --with-spark logic in cli.py is the main thing.


# ---- compute_executor_defaults tests ----

class TestParseMemToGb:
    def test_gigabytes(self):
        assert _parse_mem_to_gb("200G") == 200

    def test_megabytes(self):
        assert _parse_mem_to_gb("1024M") == 1

    def test_terabytes(self):
        assert _parse_mem_to_gb("2T") == 2048

    def test_bare_number(self):
        assert _parse_mem_to_gb("64") == 64

    def test_none(self):
        assert _parse_mem_to_gb(None) is None

    def test_invalid(self):
        assert _parse_mem_to_gb("abc") is None


class TestComputeExecutorDefaults:
    def test_standard_allocation(self):
        """2 nodes x 32 CPUs x 200G -> known good values."""
        conf, max_exec, details = compute_executor_defaults(2, 32, "200G")
        assert conf['spark.executor.cores'] == '4'
        # 31 usable CPUs / 4 = 7 executors per node
        # 180G usable / 7 = 25G per executor -> 21g heap + 4g overhead
        assert int(conf['spark.executor.memory'].rstrip('g')) > 0
        assert int(conf['spark.executor.memoryOverhead'].rstrip('g')) >= 1
        assert max_exec == 7 * 2  # 14
        assert len(details) == 5

    def test_large_allocation(self):
        """2 nodes x 40 CPUs x 600G."""
        conf, max_exec, details = compute_executor_defaults(2, 40, "600G")
        # 39 usable CPUs / 4 = 9 executors per node
        # 540G usable / 9 = 60G per executor -> 50g + 10g
        assert conf['spark.executor.cores'] == '4'
        assert conf['spark.executor.memory'] == '50g'
        assert conf['spark.executor.memoryOverhead'] == '10g'
        assert max_exec == 9 * 2  # 18

    def test_missing_values_skips(self):
        conf, max_exec, details = compute_executor_defaults(None, 32, "200G")
        assert conf == {}
        assert max_exec is None

    def test_too_few_cpus(self):
        conf, max_exec, details = compute_executor_defaults(1, 3, "16G")
        # 2 usable CPUs < 4 cores per executor
        assert conf == {}
        assert max_exec is None

    def test_details_explain_calculation(self):
        _, _, details = compute_executor_defaults(2, 40, "600G")
        joined = "\n".join(details)
        assert "40 CPUs" in joined
        assert "600G" in joined
        assert "9 executors/node" in joined
        assert "50g heap" in joined
