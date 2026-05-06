"""Reusable ingest helpers: source-view binding, Type A rename, unzip."""
from __future__ import annotations

import fnmatch
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .ctx import IngestCtx
    from .config import TableConfig


_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]+")
_ENCODING_HINTS = ("unicode", "encoding", "utf-8", "utf8")


class ColumnRenameCollision(RuntimeError):
    """Two or more original columns map to the same Type A cleaned name."""

    def __init__(self, originals: list[str], cleaned: str) -> None:
        super().__init__(f"Type A collision: {originals!r} -> {cleaned!r}")
        self.originals, self.cleaned = originals, cleaned


class ZipSlipError(ValueError):
    """A zip member's resolved path escapes the extraction root or is a symlink."""

    def __init__(self, member: str, reason: str) -> None:
        super().__init__(f"Refusing unsafe zip member {member!r}: {reason}")
        self.member, self.reason = member, reason


def _format_path_sql(path: Path | str) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def bind_source_view(
    con: Any,
    paths: list[Path],
    config: "TableConfig",
    view_name: str,
) -> str:
    """Create or replace ``view_name`` over the source files.

    Returns the encoding actually used (matters for ``encoding_used``
    bookkeeping). Currently delimited only — fixed-width is layered on
    top by the schema engine.
    """
    if config.source_format == "fixed_width":
        raise NotImplementedError("fixed-width ingest is wired in schema.apply_schema")
    sql_paths = "[" + ", ".join(_format_path_sql(p) for p in paths) + "]"
    delim_sql = _format_path_sql(config.delimiter)
    enc_sql = _format_path_sql(config.encoding)
    create_sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM read_csv_auto({sql_paths}, "
        f"delim={delim_sql}, header={'true' if config.has_header else 'false'}, "
        f"all_varchar=true, encoding={enc_sql})"
    )
    try:
        con.execute(create_sql)
        return config.encoding
    except Exception as exc:
        msg = str(exc).lower()
        if not config.allow_encoding_fallback or not any(h in msg for h in _ENCODING_HINTS):
            raise
        fb_sql = _format_path_sql(config.encoding_fallback)
        con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_csv_auto({sql_paths}, "
            f"delim={delim_sql}, header={'true' if config.has_header else 'false'}, "
            f"all_varchar=true, encoding={fb_sql})"
        )
        return config.encoding_fallback


def apply_type_a(rel: Any) -> Any:
    """Lower-case + non-alnum→underscore column rename, with collision check."""
    originals: list[str] = list(rel.columns)
    cleaned = [_NON_ALNUM.sub("_", c).strip("_").lower() for c in originals]
    buckets: dict[str, list[str]] = {}
    for orig, new in zip(originals, cleaned):
        buckets.setdefault(new, []).append(orig)
    for new, srcs in buckets.items():
        if len(srcs) > 1:
            raise ColumnRenameCollision(srcs, new)
    return rel.project(", ".join(f'"{o}" AS "{n}"' for o, n in zip(originals, cleaned)))


def _is_zip_symlink(info: zipfile.ZipInfo) -> bool:
    return info.create_system == 3 and ((info.external_attr >> 28) & 0xF) == 0xA


def unzip_inputs(
    zip_paths: list[Path], scratch_dir: Path,
    inner_pattern: str = "*.csv|*.tsv|*.txt",
    password_file: Path | None = None,
) -> list[Path]:
    """Extract zip archives with Zip Slip + symlink protection."""
    patterns = [p.strip() for p in inner_pattern.split("|") if p.strip()]
    pwd = password_file.read_bytes().strip() if password_file else None
    extracted: list[Path] = []
    for zp in zip_paths:
        target = scratch_dir / zp.stem
        target.mkdir(parents=True, exist_ok=True)
        target_root = os.path.realpath(target)
        with zipfile.ZipFile(zp) as zf:
            for info in zf.infolist():
                if _is_zip_symlink(info):
                    raise ZipSlipError(info.filename, "symlink entries forbidden")
                dest = os.path.realpath(os.path.join(target_root, info.filename))
                if dest != target_root and not dest.startswith(target_root + os.sep):
                    raise ZipSlipError(info.filename, "path escapes extraction root")
                zf.extract(info, path=target, pwd=pwd)
        for p in target.rglob("*"):
            if p.is_file() and any(fnmatch.fnmatch(p.name, pat) for pat in patterns):
                extracted.append(p)
    return extracted
