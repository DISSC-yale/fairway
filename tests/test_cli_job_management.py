
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

def test_status_command_ignores_legacy_cluster_files(runner):
    """Status is a thin squeue wrapper and should ignore stale CWD files."""
    with runner.isolated_filesystem():
        with open("spark_master_url.txt", "w") as f:
            f.write("spark://master:7077")
        with open("cluster_job_id.txt", "w") as f:
            f.write("12345")

        with patch('subprocess.run') as mock_run:
            result = runner.invoke(main, ['status'])
            assert result.exit_code == 0
            assert "Found active Spark cluster" not in result.output
            args, _ = mock_run.call_args
            assert args[0][0] == 'squeue'

def test_cancel_command(runner):
    """Test fairway cancel command."""
    with runner.isolated_filesystem():
        with patch('subprocess.run') as mock_run:
            # Test cancelling specific job
            result = runner.invoke(main, ['cancel', '12345'])
            assert result.exit_code == 0
            mock_run.assert_called_with(['scancel', '12345'], check=True)

def test_cancel_all_command(runner):
    """Test fairway cancel --all command."""
    with runner.isolated_filesystem():
        with patch('subprocess.run') as mock_run:
            with patch('getpass.getuser', return_value='testuser'):
                # Decline confirmation
                result = runner.invoke(main, ['cancel', '--all'], input='n\n')
                assert result.exit_code == 0
                mock_run.assert_not_called()

                # Accept confirmation
                result = runner.invoke(main, ['cancel', '--all'], input='y\n')
                assert result.exit_code == 0
                mock_run.assert_called_with(['scancel', '--user', 'testuser'], check=True)
