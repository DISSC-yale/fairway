"""Focused tests for Slurm CLI validation helpers (v0.2).

The validators were inlined in the v0.2 ``cli.py``; the v0.3 rewrite
(Step 9.1) drops them in favour of a tighter command surface.
``submit``-level argument validation re-lands when Step 9.2 wires the
sbatch invocation. Skipped at module level until then so the whole
file collects cleanly without resurrecting deleted symbols.
"""
from __future__ import annotations

import pytest

pytest.skip(
    "v0.2 cli helpers removed in rewrite/v0.3 Step 9.1; "
    "submit-level validation lands in Step 9.2.",
    allow_module_level=True,
)


def test_validate_slurm_param_accepts_none():
    assert _validate_slurm_param(None, "test", r"^[a-z]+$") is None


def test_validate_slurm_param_accepts_valid_value():
    assert _validate_slurm_param("abc", "test", r"^[a-z]+$") == "abc"


def test_validate_slurm_param_rejects_too_long_value():
    with pytest.raises(click.ClickException, match="maximum length"):
        _validate_slurm_param("a" * 100, "test", r"^[a-z]+$", max_length=10)


def test_validate_slurm_param_rejects_disallowed_chars():
    with pytest.raises(click.ClickException, match="disallowed characters"):
        _validate_slurm_param("abc;rm", "test", r"^[a-z]+$")


def test_validate_slurm_time_accepts_standard_format():
    assert _validate_slurm_time("12:00:00") == "12:00:00"


def test_validate_slurm_time_accepts_days_format():
    assert _validate_slurm_time("2-12:00:00") == "2-12:00:00"


def test_validate_slurm_time_accepts_short_format():
    assert _validate_slurm_time("1:30") == "1:30"


def test_validate_slurm_time_rejects_invalid_format():
    with pytest.raises(click.ClickException, match="Invalid time format"):
        _validate_slurm_time("invalid")


def test_validate_slurm_mem_accepts_gigabytes():
    assert _validate_slurm_mem("16G") == "16G"


def test_validate_slurm_mem_accepts_megabytes():
    assert _validate_slurm_mem("1024M") == "1024M"


def test_validate_slurm_mem_accepts_bare_number():
    assert _validate_slurm_mem("1024") == "1024"


def test_validate_slurm_mem_rejects_invalid_format():
    with pytest.raises(click.ClickException, match="Invalid memory format"):
        _validate_slurm_mem("16GB")
