"""Default ingest helpers (delimited read, Type A rename, unzip)."""
from __future__ import annotations

import fnmatch
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
    patterns = [p.strip() for p in inner_pattern.split("|") if p.strip()]
    pwd = password_file.read_bytes().strip() if password_file else None
    extracted: list[Path] = []
    for zp in zip_paths:
        target = scratch_dir / zp.stem
        target.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zp) as zf:
            zf.extractall(path=target, pwd=pwd)
        for p in target.rglob("*"):
            if p.is_file() and any(fnmatch.fnmatch(p.name, pat) for pat in patterns):
                extracted.append(p)
    return extracted
