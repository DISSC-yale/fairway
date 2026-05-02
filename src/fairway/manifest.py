"""Fragment-based manifest for Slurm job-array safe per-shard ingest tracking.

Two physical forms, eliminating the single-file JSON write race under concurrent
Slurm tasks:

* **Fragments** ``<root>/_fragments/<shard_id>.json`` — write-once per shard via
  :func:`write_fragment` (``.json.tmp`` + ``os.rename`` → POSIX-atomic).
* **Merged** ``<root>/manifest.json`` and ``<root>/schema_summary.json`` —
  produced by :func:`finalize`; never written by workers.

Pinned identifiers (see PLAN.md Locked architectural decisions):

* ``shard_id == task_id == sha256("/".join(f"__{k}={v}" for k, v in
  sorted(pv.items()))).hexdigest()[:16]``
* ``schema_fingerprint = sha256(json.dumps(sorted [{name, dtype}, ...],
  sort_keys=True)).hexdigest()[:16]``
* ``source_hash(p) = sha256(size_le_8 || first_4096 || last_4096).hexdigest()[:16]``;
  files <8 KB hashed in full. Best-effort content-change detection — will NOT
  detect mid-file edits that preserve file size (explicit perf tradeoff: full
  hashing of 50 GB files would dominate ingest cost; pass ``--force`` for
  bit-exact checks).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MANIFEST_VERSION = "0.3"
FRAGMENT_DIR_NAME = "_fragments"
MANIFEST_FILE_NAME = "manifest.json"
SCHEMA_SUMMARY_FILE_NAME = "schema_summary.json"
HEAD_TAIL_BYTES = 4096
SMALL_FILE_THRESHOLD = 8192

class DuplicateShardIdError(RuntimeError):
    """Raised by :func:`finalize` when two fragments share a ``shard_id``."""

def utc_now_iso() -> str:
    """ISO 8601 UTC timestamp string."""
    return datetime.now(tz=timezone.utc).isoformat()


def compute_task_id(partition_values: dict[str, str]) -> str:
    """Pinned ``task_id`` / ``shard_id`` formula (see module docstring)."""
    payload = "/".join(f"__{k}={v}" for k, v in sorted(partition_values.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def schema_fingerprint(schema: list[dict[str, str]]) -> str:
    """Deterministic 16-char hex schema fingerprint (see module docstring)."""
    sorted_schema = sorted(({"name": c["name"], "dtype": c["dtype"]} for c in schema), key=lambda c: c["name"])
    return hashlib.sha256(json.dumps(sorted_schema, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def source_hash(path: Path | str) -> str:
    """Best-effort content-change hash (see module docstring)."""
    p = Path(path)
    size = p.stat().st_size
    h = hashlib.sha256(struct.pack("<Q", size))
    with p.open("rb") as f:
        if size <= SMALL_FILE_THRESHOLD:
            h.update(f.read())
        else:
            h.update(f.read(HEAD_TAIL_BYTES))
            f.seek(-HEAD_TAIL_BYTES, os.SEEK_END)
            h.update(f.read(HEAD_TAIL_BYTES))
    return h.hexdigest()[:16]


def build_fragment(
    *,
    partition_values: dict[str, str],
    source_files: list[str],
    source_hashes: list[str],
    schema: list[dict[str, str]],
    row_count: int,
    status: str,
    error: str | None,
    encoding_used: str,
    ingest_ts: str | None = None,
) -> dict[str, Any]:
    """``shard_id`` and ``schema_fingerprint`` are derived deterministically."""
    if status not in ("ok", "error"):
        raise ValueError(f"status must be 'ok' or 'error', got {status!r}")
    return {
        "shard_id": compute_task_id(partition_values),
        "partition_values": dict(partition_values),
        "source_files": list(source_files),
        "source_hashes": list(source_hashes),
        "schema": [{"name": c["name"], "dtype": c["dtype"]} for c in schema],
        "schema_fingerprint": schema_fingerprint(schema),
        "row_count": int(row_count),
        "ingest_ts": ingest_ts or utc_now_iso(),
        "status": status,
        "error": error,
        "encoding_used": encoding_used,
    }


def _fragment_dir(root_dir: Path | str) -> Path:
    return Path(root_dir) / FRAGMENT_DIR_NAME

def fragment_path(root_dir: Path | str, shard_id: str) -> Path:
    """Final on-disk location of the fragment file for ``shard_id``."""
    return _fragment_dir(root_dir) / f"{shard_id}.json"


def _atomic_write_json(target: Path, payload: Any) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, target)


def write_fragment(fragment: dict[str, Any], root_dir: Path | str) -> None:
    """Atomically write ``fragment`` to ``<root_dir>/_fragments/<shard_id>.json``."""
    _atomic_write_json(fragment_path(root_dir, fragment["shard_id"]), fragment)


def _read_fragments(root_dir: Path) -> list[dict[str, Any]]:
    frag_dir = _fragment_dir(root_dir)
    if not frag_dir.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for entry in sorted(frag_dir.iterdir()):
        if not entry.is_file() or entry.suffix != ".json":
            continue
        try:
            with entry.open("r", encoding="utf-8") as f:
                out.append(json.load(f))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("skipping corrupt fragment %s: %s", entry, exc)
    return out


def _build_schema_summary(fragments: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate fragment schemas. Only ``status == "ok"`` fragments contribute."""
    distinct: dict[str, dict[str, Any]] = {}
    presence: dict[str, int] = {}
    dtypes: dict[str, set[str]] = {}
    total = 0
    for frag in fragments:
        if frag.get("status") != "ok":
            continue
        total += 1
        fp = frag.get("schema_fingerprint", "")
        d = distinct.setdefault(fp, {"fingerprint": fp, "schema": frag.get("schema", []), "shard_count": 0})
        d["shard_count"] += 1
        seen: set[str] = set()
        for col in frag.get("schema", []):
            name, dtype = col["name"], col["dtype"]
            if name not in seen:
                presence[name] = presence.get(name, 0) + 1
                seen.add(name)
            dtypes.setdefault(name, set()).add(dtype)
    return {
        "ok_shard_count": total,
        "distinct_schemas": list(distinct.values()),
        "column_presence": {n: round(c / total, 6) if total else 0.0 for n, c in presence.items()},
        "column_dtypes_seen": {n: sorted(d) for n, d in dtypes.items()},
    }


def finalize(root_dir: Path | str) -> dict[str, Any]:
    """Merge fragments → ``manifest.json`` + ``schema_summary.json``.

    Validates ``shard_id`` uniqueness (raises :class:`DuplicateShardIdError`);
    corrupt fragments are logged and skipped.
    """
    root = Path(root_dir)
    fragments = _read_fragments(root)
    seen: set[str] = set()
    for frag in fragments:
        sid = frag.get("shard_id", "")
        if sid in seen:
            raise DuplicateShardIdError(f"duplicate shard_id {sid!r} in {root}")
        seen.add(sid)
    merged: dict[str, Any] = {
        "version": MANIFEST_VERSION,
        "finalized_at": utc_now_iso(),
        "fragment_count": len(fragments),
        "fragments": fragments,
    }
    root.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(root / MANIFEST_FILE_NAME, merged)
    _atomic_write_json(root / SCHEMA_SUMMARY_FILE_NAME, _build_schema_summary(fragments))
    return merged


# Transitional shims — kept ONLY so ``pipeline.py`` and ``cli.py`` (still on the
# v0.2 manifest API) remain importable until they are migrated to the fragment
# API in Step 8 / Step 9. Any actual call raises NotImplementedError.
_REMOVED = "removed in v0.3 fragment manifest; use fairway.manifest.write_fragment / finalize"


def _get_file_hash_static(file_path: str, fast_check: bool = True) -> str | None:
    """Deprecated v0.2 shim. New code: :func:`source_hash`."""
    raise NotImplementedError(f"_get_file_hash_static {_REMOVED}")


class ManifestStore:
    """Deprecated v0.2 shim. New code: :func:`write_fragment` / :func:`finalize`."""

    def __init__(self, manifest_dir: str = "manifest") -> None:
        self.manifest_dir = manifest_dir

    def __getattr__(self, name: str) -> Any:
        raise NotImplementedError(f"ManifestStore.{name} {_REMOVED}")
