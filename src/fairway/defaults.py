"""Default ingest helpers (delimited read, Type A rename, unzip)."""
from __future__ import annotations

import fnmatch
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .ctx import IngestCtx

_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]+")
_ENCODING_HINTS = ("unicode", "encoding", "utf-8", "utf8")


class ColumnRenameCollision(RuntimeError):
    """Two or more original columns map to the same cleaned name."""
    def __init__(self, originals: list[str], cleaned: str) -> None:
        super().__init__(f"Type A collision: {originals!r} -> {cleaned!r}")
        self.originals, self.cleaned = originals, cleaned


class ZipSlipError(ValueError):
    """A zip member's resolved path escapes the extraction root or is a symlink.

    Raised by :func:`unzip_inputs` when an archive contains a path-traversal
    entry (e.g. ``../escape.txt``) or a symlink entry — both can be used to
    write outside the intended extraction directory. v0.2 had this guard
    (commits f481f5b, 206f93f); restored after forge code-review (Step 13.6).
    """

    def __init__(self, member: str, reason: str) -> None:
        super().__init__(f"Refusing unsafe zip member {member!r}: {reason}")
        self.member, self.reason = member, reason


def _is_zip_symlink(info: zipfile.ZipInfo) -> bool:
    """Detect a Unix-style symlink entry in a zip archive.

    Unix-zip entries encode the file type in the upper 4 bits of the
    high 16 bits of ``external_attr``; ``0xA`` is ``S_IFLNK``.
    ``create_system == 3`` identifies the entry as Unix-originated.
    """
    return (
        info.create_system == 3
        and ((info.external_attr >> 28) & 0xF) == 0xA
    )


def default_ingest_read_only(con: Any, ctx: "IngestCtx") -> Any:
    """Read inputs (delimited; fixed-width deferred to Step 7.3)."""
    cfg = ctx.config
    if cfg.source_format == "fixed_width":
        raise NotImplementedError("fixed_width read is wired in Step 7.3")
    kw = dict(delimiter=cfg.delimiter, header=cfg.has_header, all_varchar=True, filename=True)
    paths = [str(p) for p in ctx.input_paths]
    try:
        return con.read_csv(paths, encoding=cfg.encoding, ignore_errors=False, **kw)
    except Exception as exc:
        is_enc = any(h in str(exc).lower() for h in _ENCODING_HINTS)
        if not is_enc:
            return con.read_csv(paths, encoding=cfg.encoding, ignore_errors=True, **kw)
        if not cfg.allow_encoding_fallback:
            raise
        out_dir = ctx.scratch_dir / "_recoded"
        out_dir.mkdir(parents=True, exist_ok=True)
        recoded: list[str] = []
        for src in ctx.input_paths:
            dest = out_dir / src.name
            text = src.read_bytes().decode(cfg.encoding_fallback, errors="replace")
            dest.write_text(text, encoding="utf-8")
            recoded.append(str(dest))
        return con.read_csv(recoded, encoding="utf-8", ignore_errors=True, **kw)


def default_ingest(con: Any, ctx: "IngestCtx") -> Any:
    return default_ingest_read_only(con, ctx)


def apply_type_a(rel: Any) -> Any:
    originals: list[str] = list(rel.columns)
    cleaned = [_NON_ALNUM.sub("_", c).strip("_").lower() for c in originals]
    buckets: dict[str, list[str]] = {}
    for orig, new in zip(originals, cleaned):
        buckets.setdefault(new, []).append(orig)
    for new, srcs in buckets.items():
        if len(srcs) > 1:
            raise ColumnRenameCollision(srcs, new)
    return rel.project(", ".join(f'"{o}" AS "{n}"' for o, n in zip(originals, cleaned)))


def unzip_inputs(zip_paths: list[Path], scratch_dir: Path,
                 inner_pattern: str = "*.csv|*.tsv|*.txt",
                 password_file: Path | None = None) -> list[Path]:
    """Extract zip archives with Zip Slip + symlink protection.

    Each member is validated before extraction:

    * Symlink entries (Unix file-type ``S_IFLNK``) are rejected outright.
    * Each member's resolved path must stay within the per-archive target
      directory; ``../`` entries are rejected.

    A failing archive raises :class:`ZipSlipError` naming the offending
    member; no partial extraction is left in place beyond the members that
    had already passed validation when the bad member was reached. Members
    are extracted one-by-one with :meth:`zipfile.ZipFile.extract` (NOT
    ``extractall``) to keep the validation gate authoritative.
    """
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
                # Resolve member's eventual on-disk location; must stay
                # within ``target_root``. ``os.path.realpath`` collapses
                # ``..`` components and any pre-existing symlinks.
                dest = os.path.realpath(os.path.join(target_root, info.filename))
                if dest != target_root and not dest.startswith(target_root + os.sep):
                    raise ZipSlipError(
                        info.filename, "path escapes extraction root")
                zf.extract(info, path=target, pwd=pwd)
        for p in target.rglob("*"):
            if p.is_file() and any(fnmatch.fnmatch(p.name, pat) for pat in patterns):
                extracted.append(p)
    return extracted
