"""Single source of truth for fairway path resolution.

Construct one PathResolver per Config at load time. Multiple resolvers
per process are supported; each captures env at __init__. Mutating env
afterward does not affect an existing resolver — callers must pass the
resolver through rather than re-reading env from arbitrary code.

See notes/plans/2026-04-17-fairway-home.md v4.1 for the full design.
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import sys
from pathlib import Path
from typing import Optional

import platformdirs


_PROJECT_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


class PathResolverError(RuntimeError):
    pass


# Best-effort once-per-process warning set. Single-threaded CLI is the
# expected caller; set.add under GIL is atomic enough for the purpose.
_WARNED_EMPTY_ENVS: set[str] = set()


def _resolve_env_or_home(env_name: str) -> Optional[str]:
    """Return the explicit env value, or None meaning "fall back to
    platformdirs." Raises if neither the env var nor HOME is set.

    Empty string is treated as unset (documented design); warns once
    per env name so users know why their FAIRWAY_HOME="" does nothing.
    """
    raw = os.environ.get(env_name)
    if raw:
        return raw
    if raw == "" and env_name not in _WARNED_EMPTY_ENVS:
        _WARNED_EMPTY_ENVS.add(env_name)
        print(
            f"warning: {env_name}='' is ambiguous; treating as unset. "
            f"Unset the variable or give a path.",
            file=sys.stderr,
        )
    home = os.environ.get("HOME") or os.environ.get("USERPROFILE") or ""
    if not home:
        raise PathResolverError(
            f"Cannot determine fairway directory. "
            f"HOME={os.environ.get('HOME')!r}, {env_name} unset. "
            f"Set {env_name} to an absolute path, e.g.:\n"
            f"  export {env_name}=/scratch/$USER/fairway"
        )
    return None


def _state_root() -> Path:
    explicit = _resolve_env_or_home("FAIRWAY_HOME")
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    return Path(platformdirs.user_data_dir(
        "fairway", appauthor=False, roaming=False
    )).resolve()


def _scratch_root() -> Path:
    explicit = _resolve_env_or_home("FAIRWAY_SCRATCH")
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    return Path(platformdirs.user_cache_dir(
        "fairway", appauthor=False
    )).resolve()


def _slurm_run_id() -> Optional[str]:
    aj = os.environ.get("SLURM_ARRAY_JOB_ID")
    at = os.environ.get("SLURM_ARRAY_TASK_ID")
    if aj and at:
        return f"{aj}_{at}"
    jid = os.environ.get("SLURM_JOB_ID")
    if jid:
        return jid
    return None


def generate_run_id() -> str:
    """Return run_id with precedence: FAIRWAY_RUN_ID → Slurm → ULID.

    FAIRWAY_RUN_ID lets the submit host pre-compute the id and pass it
    into the worker via sbatch --export, so driver and worker agree.
    """
    pre = os.environ.get("FAIRWAY_RUN_ID")
    if pre:
        return pre
    existing = _slurm_run_id()
    if existing:
        return existing
    from ulid import ULID
    return str(ULID())


class PathResolver:
    """All fairway path defaults for a given project.

    - Construct from a Config via ``from_config``; never call module
      helpers directly from unrelated code.
    - Each instance captures env at __init__.
    - Instances are immutable after construction (``__slots__`` +
      ``__setattr__`` override). Use ``with_run_id`` to obtain a
      clone bound to a specific run_id; env is not re-read.
    """

    __slots__ = (
        "project", "run_id",
        "_state_root_path", "_scratch_root_path",
        "_frozen",
    )

    def __init__(self, project: str, run_id: Optional[str] = None):
        cleaned = (project or "").strip()
        if not cleaned:
            raise PathResolverError(
                "Missing required project name. "
                "Add 'project: <name>' to your YAML."
            )
        if not _PROJECT_NAME_RE.fullmatch(cleaned):
            raise PathResolverError(
                f"Invalid project name: {project!r}. "
                "Must match ^[a-z][a-z0-9_-]{0,63}$ (lowercase ASCII, "
                "no dots, no leading dash, <=64 chars)."
            )
        object.__setattr__(self, "project", cleaned)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "_state_root_path", _state_root())
        object.__setattr__(self, "_scratch_root_path", _scratch_root())
        object.__setattr__(self, "_frozen", True)

    def __setattr__(self, name, value):
        if getattr(self, "_frozen", False):
            raise PathResolverError(
                f"PathResolver is immutable; cannot set {name!r}"
            )
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        raise PathResolverError("PathResolver is immutable")

    def with_run_id(self, run_id: str) -> "PathResolver":
        """Return a new resolver with run_id set.

        Shares the parent's resolved state/scratch roots intentionally;
        env is not re-read. Used by IngestionPipeline.__init__ after it
        has computed the run_id — keeps Config-level construction
        run_id-agnostic.
        """
        clone = PathResolver.__new__(PathResolver)
        object.__setattr__(clone, "project", self.project)
        object.__setattr__(clone, "run_id", run_id)
        object.__setattr__(clone, "_state_root_path", self._state_root_path)
        object.__setattr__(clone, "_scratch_root_path", self._scratch_root_path)
        object.__setattr__(clone, "_frozen", True)
        return clone

    @classmethod
    def from_config(cls, config, run_id: Optional[str] = None) -> "PathResolver":
        project = (getattr(config, "project", None) or "")
        return cls(project=project, run_id=run_id)

    # --- state (durable) ---
    @property
    def project_state_dir(self) -> Path:
        return self._state_root_path / "projects" / self.project

    @property
    def state_root(self) -> Path:
        return self._state_root_path

    @property
    def manifest_dir(self) -> Path:
        return self.project_state_dir / "manifest"

    @property
    def log_dir(self) -> Path:
        return self.project_state_dir / "logs"

    @staticmethod
    def _month_shard(run_id: str, _clock_utcnow=None) -> str:
        """YYYY-MM for sharding. Deterministic when possible.

        Precedence:
          1. ULID: decode embedded timestamp (survives month boundaries).
          2. FAIRWAY_RUN_MONTH env var: precomputed once by the pipeline
             at startup; ensures start-writer and end-finalizer land in
             the same shard even across UTC midnight.
          3. Fallback: current UTC month.
        """
        try:
            from ulid import ULID
            # python-ulid exposes .timestamp as seconds (float).
            t_sec = float(ULID.from_str(run_id).timestamp)
            return _dt.datetime.fromtimestamp(
                t_sec, tz=_dt.timezone.utc
            ).strftime("%Y-%m")
        except Exception:
            pass
        pinned = os.environ.get("FAIRWAY_RUN_MONTH")
        if pinned:
            return pinned
        now = _clock_utcnow or _dt.datetime.now(_dt.timezone.utc)
        return now.strftime("%Y-%m")

    def log_file_for(self, run_id: str) -> Path:
        """Per-run log file — sharded by YYYY-MM to cap Lustre fan-out."""
        return self.log_dir / self._month_shard(run_id) / f"run_{run_id}.jsonl"

    @property
    def log_file(self) -> Path:
        if not self.run_id:
            raise PathResolverError(
                "log_file requested but run_id not set; "
                "use log_file_for(run_id) or resolver.with_run_id(id)"
            )
        return self.log_file_for(self.run_id)

    def run_metadata_file_for(self, run_id: str) -> Path:
        return self.log_dir / self._month_shard(run_id) / f"run_{run_id}.json"

    @property
    def slurm_log_dir(self) -> Path:
        """Unstructured Slurm stderr/stdout — flat sibling of log_dir."""
        return self.project_state_dir / "slurm_logs"

    @property
    def spark_coordination_dir(self) -> Path:
        return self.project_state_dir / "spark"

    @property
    def lock_dir(self) -> Path:
        """Archive/extraction locks — live under state (survive scratch purge)."""
        return self.project_state_dir / "locks"

    # --- scratch (ephemeral) ---
    @property
    def project_scratch_dir(self) -> Path:
        return self._scratch_root_path / "projects" / self.project

    @property
    def scratch_root(self) -> Path:
        return self._scratch_root_path

    @property
    def cache_dir(self) -> Path:
        return self.project_scratch_dir / "cache"

    @property
    def temp_dir(self) -> Path:
        return self.project_scratch_dir / "temp"

    # --- config-dir-relative resolution ---
    @staticmethod
    def resolve_config_path(value, config_dir) -> Path:
        """Absolute → as-is. Relative → resolved against config_dir.
        CWD is never consulted.

        Tilde expansion uses the *current* HOME, so `~/data` resolves
        differently on submit host vs worker if HOMEs differ. Prefer
        absolute paths in shared configs.
        """
        p = Path(os.fspath(value)).expanduser()
        if p.is_absolute():
            return p
        return (Path(os.fspath(config_dir)) / p).resolve()
