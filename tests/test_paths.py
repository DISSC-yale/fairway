"""Unit tests for fairway.paths — PathResolver, env resolution, run_id."""
from __future__ import annotations

import datetime as _dt
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from fairway.paths import (
    PathResolver,
    PathResolverError,
    _resolve_env_or_home,
    _scratch_root,
    _slurm_run_id,
    _state_root,
    generate_run_id,
)


# All tests in this module exercise resolver behavior directly, so
# opt out of the autouse FAIRWAY_HOME fixture and manage env per-test.
pytestmark = pytest.mark.no_fairway_home


@pytest.fixture
def _clean_env(monkeypatch):
    """Strip every FAIRWAY_* and HOME/USERPROFILE so tests set what they need."""
    for var in list(os.environ):
        if var.startswith("FAIRWAY_") or var.startswith("SLURM_"):
            monkeypatch.delenv(var, raising=False)
    # Give HOME a default so most tests don't have to. Tests that need
    # HOME unset override explicitly.
    monkeypatch.setenv("HOME", "/home/testuser")
    monkeypatch.delenv("USERPROFILE", raising=False)
    # Clear the once-per-process warning set.
    from fairway import paths as _paths
    _paths._WARNED_EMPTY_ENVS.clear()
    yield


# --- PathResolver construction ---

def test_valid_project_name(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("voters_2024")
    assert r.project == "voters_2024"
    assert r.manifest_dir == tmp_path / "state" / "projects" / "voters_2024" / "manifest"
    assert r.cache_dir == tmp_path / "scratch" / "projects" / "voters_2024" / "cache"


@pytest.mark.parametrize("bad", ["", "  ", "bad name", "-foo", "Foo", "café", "A" * 65, ".dotfile"])
def test_invalid_project_names(_clean_env, monkeypatch, tmp_path, bad):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    with pytest.raises(PathResolverError):
        PathResolver(bad)


def test_from_config_without_project_attr(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    class Cfg: pass
    with pytest.raises(PathResolverError, match="Missing required project"):
        PathResolver.from_config(Cfg())


def test_from_config_whitespace_project(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    class Cfg:
        project = "   "
    with pytest.raises(PathResolverError, match="Missing required project"):
        PathResolver.from_config(Cfg())


# --- env resolution ---

def test_state_root_with_fairway_home(_clean_env, monkeypatch, tmp_path):
    target = tmp_path / "state"
    monkeypatch.setenv("FAIRWAY_HOME", str(target))
    assert _state_root() == target.resolve()


def test_state_root_empty_string_treated_as_unset(_clean_env, monkeypatch, capsys):
    monkeypatch.setenv("FAIRWAY_HOME", "")
    # HOME is still set from _clean_env → falls back to platformdirs
    p = _state_root()
    assert p.is_absolute()
    # Warning was emitted to stderr
    captured = capsys.readouterr()
    assert "FAIRWAY_HOME='' is ambiguous" in captured.err


def test_state_root_raises_when_home_unset(_clean_env, monkeypatch):
    monkeypatch.delenv("FAIRWAY_HOME", raising=False)
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    with pytest.raises(PathResolverError, match="Cannot determine fairway directory"):
        _state_root()


def test_scratch_root_raises_when_home_unset(_clean_env, monkeypatch):
    """Scratch is symmetric with state — no silent /tmp fallback."""
    monkeypatch.delenv("FAIRWAY_SCRATCH", raising=False)
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    with pytest.raises(PathResolverError, match="Cannot determine fairway directory"):
        _scratch_root()


def test_resolve_env_or_home_returns_none_for_platformdirs_fallback(_clean_env):
    # HOME is set, FAIRWAY_HOME unset → None
    assert _resolve_env_or_home("FAIRWAY_HOME") is None


def test_resolve_env_or_home_returns_value_when_set(_clean_env, monkeypatch):
    monkeypatch.setenv("FAIRWAY_HOME", "/custom")
    assert _resolve_env_or_home("FAIRWAY_HOME") == "/custom"


# --- resolve_config_path ---

def test_resolve_relative_config_path():
    out = PathResolver.resolve_config_path("./rel/sub", Path("/etc/fairway"))
    assert out == Path("/etc/fairway/rel/sub")


def test_resolve_absolute_config_path():
    out = PathResolver.resolve_config_path("/abs/path", Path("/etc/fairway"))
    assert out == Path("/abs/path")


# --- immutability ---

def test_env_capture_invariant(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state1"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch1"))
    (tmp_path / "state1").mkdir()
    (tmp_path / "scratch1").mkdir()
    r = PathResolver("p")
    captured = r.manifest_dir
    # Mutate env after construction
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state2"))
    (tmp_path / "state2").mkdir()
    assert r.manifest_dir == captured


def test_immutability_setattr_blocked(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p")
    with pytest.raises(PathResolverError, match="immutable"):
        r.project = "other"
    with pytest.raises(PathResolverError, match="immutable"):
        r.run_id = "new"


def test_immutability_delattr_blocked(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p")
    with pytest.raises(PathResolverError, match="immutable"):
        del r.run_id


def test_two_resolvers_are_independent(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    a = PathResolver("alpha")
    b = PathResolver("bravo")
    assert a.project_state_dir != b.project_state_dir
    assert a.cache_dir != b.cache_dir


# --- log_file / log_file_for ---

def test_log_file_without_run_id_raises(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p")
    with pytest.raises(PathResolverError, match="run_id not set"):
        _ = r.log_file


def test_log_file_for_works_without_run_id(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p")
    out = r.log_file_for("12345")
    assert out.name == "run_12345.jsonl"
    # shard is YYYY-MM
    assert out.parent.name.count("-") == 1


# --- with_run_id ---

def test_with_run_id_clones(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p")
    r2 = r.with_run_id("abc123")
    assert r.run_id is None
    assert r2.run_id == "abc123"
    assert r2.project == r.project
    # Shares resolved roots (env not re-read)
    assert r2._state_root_path == r._state_root_path


def test_with_run_id_does_not_reread_env(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state1"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch1"))
    r = PathResolver("p")
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state2"))
    r2 = r.with_run_id("xyz")
    assert r2._state_root_path == (tmp_path / "state1").resolve()


def test_with_run_id_is_also_immutable(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("p").with_run_id("abc")
    with pytest.raises(PathResolverError, match="immutable"):
        r.run_id = "changed"


# --- generate_run_id precedence ---

def test_run_id_prefers_fairway_run_id(_clean_env, monkeypatch):
    monkeypatch.setenv("FAIRWAY_RUN_ID", "preset-id")
    monkeypatch.setenv("SLURM_JOB_ID", "99")
    assert generate_run_id() == "preset-id"


def test_run_id_array_job(_clean_env, monkeypatch):
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "123")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "7")
    assert generate_run_id() == "123_7"


def test_run_id_single_slurm_job(_clean_env, monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "9876")
    assert generate_run_id() == "9876"


def test_run_id_ulid_fallback(_clean_env):
    rid = generate_run_id()
    assert len(rid) == 26  # ULID is 26 chars
    assert rid.isalnum()


# --- _month_shard ---

def test_month_shard_from_ulid():
    from ulid import ULID
    # Build a ULID at a known timestamp: 2023-06-15 12:00:00 UTC
    t = _dt.datetime(2023, 6, 15, 12, tzinfo=_dt.timezone.utc)
    u = ULID.from_timestamp(t.timestamp())
    assert PathResolver._month_shard(str(u)) == "2023-06"


def test_month_shard_from_fairway_run_month(_clean_env, monkeypatch):
    monkeypatch.setenv("FAIRWAY_RUN_MONTH", "2026-04")
    # Non-ULID id → falls through to FAIRWAY_RUN_MONTH
    assert PathResolver._month_shard("123_7") == "2026-04"


def test_month_shard_fallback_to_now(_clean_env):
    # No ULID, no FAIRWAY_RUN_MONTH → current UTC month
    now = _dt.datetime(2030, 11, 20, tzinfo=_dt.timezone.utc)
    assert PathResolver._month_shard("slurm_123", _clock_utcnow=now) == "2030-11"


# --- Slurm-derived id helper ---

def test_slurm_run_id_none_when_no_env(_clean_env):
    assert _slurm_run_id() is None


def test_slurm_run_id_array_preferred(_clean_env, monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", "1000")
    monkeypatch.setenv("SLURM_ARRAY_JOB_ID", "2000")
    monkeypatch.setenv("SLURM_ARRAY_TASK_ID", "3")
    assert _slurm_run_id() == "2000_3"


# --- Path layout sanity ---

def test_state_layout(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("proj", run_id="abc")
    base = tmp_path / "state" / "projects" / "proj"
    assert r.project_state_dir == base
    assert r.manifest_dir == base / "manifest"
    assert r.log_dir == base / "logs"
    assert r.slurm_log_dir == base / "slurm_logs"
    assert r.spark_coordination_dir == base / "spark"
    assert r.lock_dir == base / "locks"


def test_scratch_layout(_clean_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FAIRWAY_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FAIRWAY_SCRATCH", str(tmp_path / "scratch"))
    r = PathResolver("proj")
    base = tmp_path / "scratch" / "projects" / "proj"
    assert r.project_scratch_dir == base
    assert r.cache_dir == base / "cache"
    assert r.temp_dir == base / "temp"
