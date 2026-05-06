"""Shard enumeration: glob → naming_pattern → leaf partition + shard grouping.

A *leaf* partition path is the full Hive path under ``partition_by``; a
*shard* is the ``shard_by`` prefix (a strict prefix of ``partition_by``).
``shard_by == partition_by`` collapses to one shard per leaf (default);
``shard_by == []`` collapses to a single shard for the whole table.
"""
from __future__ import annotations

import glob as _glob
import re
from dataclasses import dataclass, field
from pathlib import Path

from .config import TableConfig, ConfigError


@dataclass(frozen=True)
class Shard:
    """One unit of compute work."""

    shard_id: str
    shard_values: dict[str, str]                     # {key: value} for shard_by
    leaves: dict[str, list[Path]] = field(default_factory=dict)  # leaf → files

    @property
    def input_paths(self) -> list[Path]:
        out: list[Path] = []
        for files in self.leaves.values():
            out.extend(files)
        return out


def _safe_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _shard_id(shard_by: list[str], values: dict[str, str]) -> str:
    if not shard_by:
        return "all"
    return "_".join(f"{k}={_safe_token(values[k])}" for k in shard_by)


def _leaf_path(partition_by: list[str], values: dict[str, str]) -> str:
    return "/".join(f"{k}={values[k]}" for k in partition_by)


def validate_shard_by(config: TableConfig) -> None:
    """``shard_by`` must be a strict prefix of ``partition_by`` (or ``[]``)."""
    if not config.shard_by:
        return
    n = len(config.shard_by)
    if config.shard_by != config.partition_by[:n]:
        raise ConfigError(
            f"shard_by {config.shard_by!r} must be a strict prefix of "
            f"partition_by {config.partition_by!r} (or [] for one shard)."
        )


def enumerate_shards(
    config: TableConfig,
) -> tuple[list[Shard], list[Path]]:
    """Return ``(shards, unmatched_files)`` for ``config.source_glob``."""
    validate_shard_by(config)
    pattern = re.compile(config.naming_pattern)
    matches: list[tuple[dict[str, str], Path]] = []
    unmatched: list[Path] = []
    for f in sorted(_glob.glob(config.source_glob)):
        path = Path(f)
        m = pattern.fullmatch(path.name)
        if m is None:
            unmatched.append(path)
            continue
        groupdict = m.groupdict()
        try:
            values = {k: groupdict[k] for k in config.partition_by}
        except KeyError as exc:
            raise ConfigError(
                f"naming_pattern missing named group for {exc.args[0]!r}"
            ) from exc
        matches.append((values, path))

    shards: dict[str, Shard] = {}
    for values, path in matches:
        shard_values = {k: values[k] for k in config.shard_by}
        sid = _shard_id(config.shard_by, shard_values)
        leaf = _leaf_path(config.partition_by, values)
        shard = shards.get(sid)
        if shard is None:
            shard = Shard(shard_id=sid, shard_values=dict(shard_values), leaves={})
            shards[sid] = shard
        shard.leaves.setdefault(leaf, []).append(path)
    for shard in shards.values():
        for leaf in shard.leaves:
            shard.leaves[leaf].sort()
    return sorted(shards.values(), key=lambda s: s.shard_id), unmatched
