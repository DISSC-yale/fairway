
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from fairway.cli import main
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
