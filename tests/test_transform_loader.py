"""Tests for :func:`fairway.transform_loader.load_transform`."""
from __future__ import annotations

from pathlib import Path

import pytest

from fairway.transform_loader import load_transform


def test_load_transform_happy_path(tmp_path: Path) -> None:
    """A user .py with `def transform(con, ctx)` loads as a callable."""
    plugin = tmp_path / "my_dataset.py"
    plugin.write_text(
        "def transform(con, ctx):\n"
        "    return None\n"
    )
    fn = load_transform(plugin)
    assert callable(fn)
    assert fn(None, None) is None


def test_load_transform_missing_file_raises(tmp_path: Path) -> None:
    """A missing plugin path raises FileNotFoundError."""
    missing = tmp_path / "does_not_exist.py"
    with pytest.raises(FileNotFoundError):
        load_transform(missing)


def test_load_transform_no_transform_attr_raises(tmp_path: Path) -> None:
    """A plugin file with no top-level `transform` raises AttributeError."""
    plugin = tmp_path / "no_transform.py"
    plugin.write_text(
        "def something_else(con, ctx):\n"
        "    return None\n"
    )
    with pytest.raises(AttributeError):
        load_transform(plugin)
