"""Filename-regex partition batcher + pre-scan validation gate.

:class:`PartitionBatcher` is the silent helper used by
:mod:`fairway.pipeline._enumerate_shards`. :func:`expand_and_validate`
is the :mod:`fairway.cli.submit` pre-scan gate (Step 9.3) — it surfaces
unmatched files so callers can hard-error or log + skip via
``--allow-skip``.
"""
from __future__ import annotations

import glob as _glob
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from . import manifest

if TYPE_CHECKING:  # pragma: no cover — import-only
    from .config import Config


@dataclass(frozen=True)
class ShardSpec:
    """One pre-scan-validated shard: identity + inputs + partition values."""

    shard_id: str
    input_paths: list[Path]
    partition_values: dict[str, str]


class PartitionBatcher:
    """Group files by ``naming_pattern`` named-group values."""

    @staticmethod
    def extract_partition_values(file_path, naming_pattern, partition_by, compiled_re=None):
        """Tuple of partition values in ``partition_by`` order, else ``None``."""
        m = (compiled_re or re.compile(naming_pattern)).search(os.path.basename(file_path))
        if not m or not all(k in m.groupdict() for k in partition_by):
            return None
        return tuple(m.groupdict()[k] for k in partition_by)

    @staticmethod
    def group_files(file_paths, naming_pattern, partition_by):
        """Group files by partition tuple; unmatched keyed under ``None``."""
        compiled = re.compile(naming_pattern) if file_paths else None
        batches: dict = {}
        for p in file_paths:
            v = PartitionBatcher.extract_partition_values(p, naming_pattern, partition_by, compiled)
            batches.setdefault(v, []).append(p)
        return batches


def expand_and_validate(config: "Config") -> tuple[list[ShardSpec], list[Path]]:
    """Pre-scan ``config.source_glob`` and cluster files into ShardSpecs.

    Matching files (``re.fullmatch`` of ``naming_pattern`` on basename)
    cluster by named-group ``partition_values`` into one :class:`ShardSpec`
    per cluster; non-matching files are returned in the second tuple element.
    """
    pattern = re.compile(config.naming_pattern)
    matched: dict[tuple, list[Path]] = {}
    unmatched: list[Path] = []
    for f in sorted(_glob.glob(config.source_glob)):
        path = Path(f)
        m = pattern.fullmatch(path.name)
        if m is None:
            unmatched.append(path)
            continue
        g = m.groupdict()
        pv_key = tuple(sorted((k, g[k]) for k in config.partition_by if k in g))
        matched.setdefault(pv_key, []).append(path)
    shards = sorted(
        (ShardSpec(manifest.compute_task_id(dict(k)), sorted(paths), dict(k))
         for k, paths in matched.items()),
        key=lambda s: s.shard_id,
    )
    return shards, unmatched
