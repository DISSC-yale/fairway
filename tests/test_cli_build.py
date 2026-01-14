
import pytest
import os
import subprocess
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from fairway.cli import main

def test_build_no_def_files(runner):
    """Test build fails when no definition files exist."""
    with runner.isolated_filesystem():
        result = runner.invoke(main, ['build'])
        assert result.exit_code != 0
        assert "No container definition found" in result.output

def test_build_apptainer(runner):
    """Test Apptainer build triggers correctly."""
    with runner.isolated_filesystem():
        with open('Apptainer.def', 'w') as f:
            f.write("Bootstrap: docker\nFrom: alpine")
            
        with patch('subprocess.run') as mock_run:
            result = runner.invoke(main, ['build'])
            
            assert result.exit_code == 0
            # Check subprocess call
            mock_run.assert_called_with(
                ["apptainer", "build", "fairway.sif", "Apptainer.def"], 
                check=True
            )
            assert "Build complete: fairway.sif" in result.output

def test_build_docker(runner):
    """Test Docker build triggers correctly when Apptainer.def missing."""
    with runner.isolated_filesystem():
        with open('Dockerfile', 'w') as f:
            f.write("FROM alpine")
            
        with patch('subprocess.run') as mock_run:
            result = runner.invoke(main, ['build'])
            
            assert result.exit_code == 0
            # Check subprocess call
            mock_run.assert_called_with(
                ["docker", "build", "-t", "fairway", "."], 
                check=True
            )
            assert "Build complete: fairway:latest" in result.output

def test_build_force_overwrite(runner):
    """Test overwrite confirmation and force flag."""
    with runner.isolated_filesystem():
        with open('Apptainer.def', 'w') as f:
            f.write("Bootstrap: docker\nFrom: alpine")
        with open('fairway.sif', 'w') as f:
            f.write("fake image")
            
        with patch('subprocess.run') as mock_run:
            # 1. No force, decline overwrite
            result = runner.invoke(main, ['build'], input='n\n')
            assert result.exit_code == 0 # Aborted safely
            assert "Overwrite?" in result.output
            mock_run.assert_not_called()
            
            # 2. No force, accept overwrite
            result = runner.invoke(main, ['build'], input='y\n')
            assert result.exit_code == 0
            mock_run.assert_called()
            
            # 3. Force
            mock_run.reset_mock()
            result = runner.invoke(main, ['build', '--force'])
            assert result.exit_code == 0
            assert "Overwriting existing fairway.sif" in result.output
            mock_run.assert_called()

@pytest.fixture
def runner():
    return CliRunner()
