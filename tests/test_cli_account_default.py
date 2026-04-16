import pytest
from click.testing import CliRunner

def test_spark_start_without_account_does_not_use_borzekowski(tmp_path):
    """Confirm borzekowski is not used as a fallback account anywhere."""
    # Just check the source — grep is fastest
    import subprocess
    result = subprocess.run(
        ['grep', '-r', 'borzekowski', 'src/'],
        capture_output=True, text=True, cwd='/workspace'
    )
    assert result.stdout == '', (
        f"Found 'borzekowski' in source code:\n{result.stdout}"
    )
