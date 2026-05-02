"""Default ingest helpers exposed to user transform plugins.

Stub module — bodies are filled in Step 7.2. The shapes here exist so
that :mod:`fairway.duckdb_runner` and the user-facing ``transform``
plugin contract can import these names from Step 7.1 onward without
the implementation being in place yet.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-only
    from .ctx import IngestCtx


def default_ingest(con: Any, ctx: "IngestCtx") -> Any:
    """Read inputs per ``ctx.config`` and return a DuckDBPyRelation. (Step 7.2)"""
    raise NotImplementedError("default_ingest is filled in Step 7.2")


def default_ingest_read_only(con: Any, ctx: "IngestCtx") -> Any:
    """Apply only the read step (delimited or fixed-width). (Step 7.2)"""
    raise NotImplementedError("default_ingest_read_only is filled in Step 7.2")


def apply_type_a(rel: Any) -> Any:
    """Lowercase/snake_case rename with fail-loud collision detection. (Step 7.2)"""
    raise NotImplementedError("apply_type_a is filled in Step 7.2")


def unzip_inputs(
    zip_paths: list[Path],
    scratch_dir: Path,
    inner_pattern: str,
    password_file: Path | None,
) -> list[Path]:
    """Extract zip archives into ``scratch_dir`` and return inner paths. (Step 7.2)"""
    raise NotImplementedError("unzip_inputs is filled in Step 7.2")
