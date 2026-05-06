"""Per-table manifest as the source of truth for idempotent re-runs.

Layout: ``tables/<t>/manifest.json`` (gitignored). Workers write
fragments to ``tables/<t>/_state/fragments/<shard_id>.json``;
:func:`finalize` merges them in and clears the fragment dir.

Idempotency unit is the *leaf partition*, not the shard. Changing
``shard_by`` regroups compute but never invalidates a leaf — each leaf's
``source_hashes`` + ``schema_fingerprint`` decide skip/run.
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
MANIFEST_FILE_NAME = "manifest.json"
STATE_DIR_NAME = "_state"
FRAGMENT_DIR_NAME = "fragments"
HEAD_TAIL_BYTES = 4096
SMALL_FILE_THRESHOLD = 8192


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def manifest_path(table_dir: Path) -> Path:
    return table_dir / MANIFEST_FILE_NAME


def fragment_dir(table_dir: Path) -> Path:
    return table_dir / STATE_DIR_NAME / FRAGMENT_DIR_NAME


def fragment_path(table_dir: Path, shard_id: str) -> Path:
    return fragment_dir(table_dir) / f"{shard_id}.json"


def source_hash(path: Path | str) -> str:
    """Best-effort content hash: ``sha256(size || head_4KB || tail_4KB)``."""
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


def _atomic_write_json(target: Path, payload: Any) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)


def empty_manifest(table: str) -> dict[str, Any]:
    return {
        "version": MANIFEST_VERSION,
        "table": table,
        "finalized_at": None,
        "layers": {},
    }


def load_manifest(table_dir: Path) -> dict[str, Any]:
    """Load ``manifest.json``; return empty initialized structure if absent."""
    path = manifest_path(table_dir)
    table = table_dir.name
    if not path.is_file():
        return empty_manifest(table)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("could not read %s (%s); reinitializing", path, exc)
        return empty_manifest(table)


def save_manifest(manifest: dict[str, Any], table_dir: Path) -> None:
    """Atomic write to ``tables/<t>/manifest.json``."""
    _atomic_write_json(manifest_path(table_dir), manifest)


def _layer(manifest: dict, layer: str) -> dict[str, Any]:
    layers = manifest.setdefault("layers", {})
    return layers.setdefault(layer, {"partitions": {}, "last_run": None})


def is_leaf_valid(
    manifest: dict[str, Any],
    layer: str,
    leaf: str,
    source_files: list[str],
    source_hashes: list[str],
    schema_fp: str,
) -> bool:
    """Return True iff the manifest already has this leaf as ``status="ok"``
    with matching source hashes (file list + per-file content hash) and
    matching ``schema_fingerprint``.
    """
    layers = manifest.get("layers", {})
    parts = layers.get(layer, {}).get("partitions", {})
    entry = parts.get(leaf)
    if not entry or entry.get("status") != "ok":
        return False
    if list(entry.get("source_files") or []) != list(source_files):
        return False
    if list(entry.get("source_hashes") or []) != list(source_hashes):
        return False
    if entry.get("schema_fingerprint") != schema_fp:
        return False
    return True


def record_leaf(
    manifest: dict[str, Any],
    layer: str,
    leaf: str,
    *,
    source_files: list[str],
    source_hashes: list[str],
    schema_fingerprint: str,
    row_count: int,
    status: str,
    drift_events: dict | None = None,
    encoding_used: str | None = None,
    ingest_ts: str | None = None,
) -> None:
    """Upsert a leaf entry in memory. Caller writes via :func:`save_manifest`."""
    if status not in ("ok", "error"):
        raise ValueError(f"status must be 'ok' or 'error', got {status!r}")
    entry: dict[str, Any] = {
        "source_files": list(source_files),
        "source_hashes": list(source_hashes),
        "schema_fingerprint": schema_fingerprint,
        "row_count": int(row_count),
        "status": status,
        "ingest_ts": ingest_ts or utc_now_iso(),
    }
    if drift_events:
        entry["drift_events"] = drift_events
    if encoding_used:
        entry["encoding_used"] = encoding_used
    layer_entry = _layer(manifest, layer)
    layer_entry.setdefault("partitions", {})[leaf] = entry


def clear_last_run(manifest: dict[str, Any], layer: str = "processed") -> None:
    """Reset the layer's ``last_run`` block ahead of a fresh submit/run."""
    _layer(manifest, layer)["last_run"] = None


def set_last_run(
    manifest: dict[str, Any],
    layer: str,
    *,
    shard_by: list[str],
    shards_observed: list[str],
    failed_shards: list[dict[str, Any]],
) -> None:
    _layer(manifest, layer)["last_run"] = {
        "shard_grouping": {"shard_by": list(shard_by)},
        "shards_observed": list(shards_observed),
        "failed_shards": list(failed_shards),
    }


def write_fragment(
    table_dir: Path,
    shard_id: str,
    *,
    layer: str,
    leaves: dict[str, dict[str, Any]],
    failed: dict[str, Any] | None = None,
    shard_by: list[str] | None = None,
) -> Path:
    """Atomically write a fragment for one shard's outcome.

    A fragment carries either successful per-leaf entries (``leaves``) or
    a shard-level error record (``failed``). Both can be present when a
    shard partially succeeded before erroring.
    """
    payload: dict[str, Any] = {
        "shard_id": shard_id,
        "layer": layer,
        "leaves": leaves,
        "failed": failed,
        "shard_by": list(shard_by or []),
    }
    target = fragment_path(table_dir, shard_id)
    _atomic_write_json(target, payload)
    return target


def _read_fragments(table_dir: Path) -> list[dict[str, Any]]:
    fdir = fragment_dir(table_dir)
    if not fdir.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for entry in sorted(fdir.iterdir()):
        if not entry.is_file() or entry.suffix != ".json":
            continue
        try:
            out.append(json.loads(entry.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("skipping corrupt fragment %s: %s", entry, exc)
    return out


def finalize(
    table_dir: Path,
    *,
    layer: str = "processed",
    shard_by: list[str] | None = None,
) -> dict[str, Any]:
    """Merge fragments → manifest; delete fragment files on success.

    Always rewrites ``last_run`` for ``layer`` (cleared on each submit/run).
    No-op fragment dir is allowed: only ``finalized_at`` + ``last_run`` are
    refreshed.
    """
    manifest = load_manifest(table_dir)
    fragments = _read_fragments(table_dir)
    failed_shards: list[dict[str, Any]] = []
    shards_observed: list[str] = []
    effective_shard_by: list[str] = list(shard_by or [])
    leaf_count = 0
    for frag in fragments:
        sid = str(frag.get("shard_id", ""))
        shards_observed.append(sid)
        if frag.get("shard_by") and not effective_shard_by:
            effective_shard_by = list(frag["shard_by"])
        leaves = frag.get("leaves") or {}
        for leaf_path, leaf_data in leaves.items():
            record_leaf(
                manifest, layer, leaf_path,
                source_files=leaf_data.get("source_files", []),
                source_hashes=leaf_data.get("source_hashes", []),
                schema_fingerprint=leaf_data.get("schema_fingerprint", ""),
                row_count=leaf_data.get("row_count", 0),
                status=leaf_data.get("status", "ok"),
                drift_events=leaf_data.get("drift_events"),
                encoding_used=leaf_data.get("encoding_used"),
                ingest_ts=leaf_data.get("ingest_ts"),
            )
            leaf_count += 1
        if frag.get("failed"):
            failed_shards.append(frag["failed"])
    if fragments:
        set_last_run(
            manifest, layer,
            shard_by=effective_shard_by,
            shards_observed=sorted(shards_observed),
            failed_shards=failed_shards,
        )
    manifest["finalized_at"] = utc_now_iso()
    save_manifest(manifest, table_dir)
    fdir = fragment_dir(table_dir)
    if fdir.is_dir():
        for entry in fdir.iterdir():
            if entry.is_file() and entry.suffix == ".json":
                try:
                    entry.unlink()
                except OSError as exc:
                    logger.warning("could not remove fragment %s: %s", entry, exc)
    logger.info("finalized %s: merged %d fragment(s), %d leaf entries",
                table_dir, len(fragments), leaf_count)
    return manifest


def fragment_count(table_dir: Path) -> int:
    fdir = fragment_dir(table_dir)
    if not fdir.is_dir():
        return 0
    return sum(1 for f in fdir.iterdir() if f.is_file() and f.suffix == ".json")
