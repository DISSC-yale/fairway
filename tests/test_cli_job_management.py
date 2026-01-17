
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from fairway.cli import main
import os

@pytest.fixture
def runner():
    return CliRunner()

def test_status_command_no_spark_cluster(runner):
    """Test fairway status when no Spark cluster is active."""
    with runner.isolated_filesystem():
        with patch('subprocess.run') as mock_run:
            result = runner.invoke(main, ['status'])
            assert result.exit_code == 0
            
            # Should look for 'squeue'
            args, _ = mock_run.call_args
            assert args[0][0] == 'squeue'
            
            # Should have --user flag by default
            assert '--user' in args[0]

def test_status_command_with_spark_cluster(runner):
    """Test fairway status displays Spark cluster info if files exist."""
    with runner.isolated_filesystem():
        # Create mock cluster info files
        with open("spark_master_url.txt", "w") as f:
            f.write("spark://master:7077")
        with open("cluster_job_id.txt", "w") as f:
            f.write("12345")
            
        with patch('os.path.expanduser', side_effect=lambda x: x.replace("~", os.getcwd())):
            with patch('subprocess.run') as mock_run:
                result = runner.invoke(main, ['status'])
                assert result.exit_code == 0
                
                assert "Found active Spark cluster" in result.output
                assert "spark://master:7077" in result.output
                assert "12345" in result.output

def test_kill_command(runner):
    """Test fairway kill command."""
    with runner.isolated_filesystem():
        with patch('subprocess.run') as mock_run:
            # Test killing specific job
            result = runner.invoke(main, ['kill', '12345'])
            assert result.exit_code == 0
            mock_run.assert_called_with(['scancel', '12345'], check=True)

def test_kill_all_command(runner):
    """Test fairway kill --all command."""
    with runner.isolated_filesystem():
        with patch('subprocess.run') as mock_run:
            with patch('getpass.getuser', return_value='testuser'):
                # Decline confirmation
                result = runner.invoke(main, ['kill', '--all'], input='n\n')
                assert result.exit_code == 0
                mock_run.assert_not_called()
                
                # Accept confirmation
                result = runner.invoke(main, ['kill', '--all'], input='y\n')
                assert result.exit_code == 0
                mock_run.assert_called_with(['scancel', '--user', 'testuser'], check=True)
