import os
import pytest
import shutil

@pytest.mark.local
def test_interrupted_swap_preserves_old_backup(tmp_path):
    """If temp_final is missing during recovery, old_backup must NOT be deleted."""
    final_path = tmp_path / "curated.parquet"
    old_backup = str(final_path) + ".tmp_old"
    temp_final = str(final_path) + ".tmp_new"

    # Simulate: final_path was renamed to old_backup, temp_final was never created
    final_path.write_text("good data")
    shutil.move(str(final_path), old_backup)
    # temp_final does NOT exist — interrupted mid-swap

    from fairway.pipeline import _recover_atomic_swap
    _recover_atomic_swap(str(final_path), old_backup, temp_final)

    # Recovery must restore old_backup → final_path
    assert final_path.exists(), "Recovery failed: curated data was lost"
    assert not os.path.exists(old_backup), "old_backup should be cleaned up after successful restore"
