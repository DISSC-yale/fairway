"""Shared pytest configuration for Fairway tests (v0.3).

Three responsibilities:

1. Repo-root leak detector. Snapshot ``os.listdir(repo_root)`` at
   ``pytest_configure`` and diff again at ``pytest_terminal_summary``;
   anything new (excluding sanctioned build/cache dirs) is flagged so
   we catch tests that scatter manifest / logs / cache into the repo.
2. Per-test ``FAIRWAY_HOME`` / ``FAIRWAY_SCRATCH`` isolation via an
   autouse fixture; opt out with ``@pytest.mark.no_fairway_home`` for
   resolver-level tests that exercise unset-env paths directly.
3. Per-session basetemp cleanup. The ``--basetemp=build/test-tmp``
   addopts setting isolates pytest temp dirs; this conftest removes the
   directory at session teardown so successive runs don't accumulate
   stale per-test scratch trees.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest


_repo_root_baseline: "set[str] | None" = None


def pytest_configure(config):
    """Bootstrap ``build/coverage`` and snapshot repo root for leak detection."""
    global _repo_root_baseline
    repo_root = Path(__file__).parent.parent
    (repo_root / "build" / "coverage").mkdir(parents=True, exist_ok=True)
    try:
        _repo_root_baseline = set(os.listdir(repo_root))
    except OSError:
        _repo_root_baseline = None


def pytest_sessionfinish(session, exitstatus):
    """Remove ``build/test-tmp`` after each pytest session.

    The basetemp directory is created fresh per session at
    ``--basetemp=build/test-tmp``; without explicit cleanup pytest keeps
    the last 3 runs' worth of per-test directories around. We delete
    the entire tree so a re-run starts clean and disk usage stays
    bounded.
    """
    repo_root = Path(__file__).parent.parent
    basetemp = repo_root / "build" / "test-tmp"
    if basetemp.exists():
        shutil.rmtree(basetemp, ignore_errors=True)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Flag any new repo-root entries created during the test session."""
    if _repo_root_baseline is None:
        return
    repo_root = Path(__file__).parent.parent
    ignored = {
        "build", ".pytest_cache", ".ruff_cache", ".venv",
        "__pycache__", ".reports", "htmlcov",
        "coverage.xml", "coverage.json",
    }
    try:
        after = set(os.listdir(repo_root))
    except OSError:
        return
    leaked = sorted((after - _repo_root_baseline) - ignored)
    if leaked:
        terminalreporter.write_line(
            f"TEST LEAK DETECTED — new repo-root entries after session: {leaked}",
            red=True,
        )


@pytest.fixture(autouse=True)
def _fairway_home(request, tmp_path, monkeypatch):
    """Point FAIRWAY_HOME and FAIRWAY_SCRATCH at per-test tmp dirs.

    Autouse so every test gets an isolated state root; opt out with
    ``@pytest.mark.no_fairway_home`` for resolver tests that need to
    exercise the env-unset / platformdirs fallback path.
    """
    if request.node.get_closest_marker("no_fairway_home"):
        yield
        return
    state = tmp_path / "_state"
    scratch = tmp_path / "_scratch"
    state.mkdir()
    scratch.mkdir()
    monkeypatch.setenv("FAIRWAY_HOME", str(state))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(scratch))
    yield
