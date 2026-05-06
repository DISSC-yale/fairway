"""Load an optional ``tables/<t>/transform.py`` hook.

Returns a callable ``transform(con, ctx) -> duckdb.DuckDBPyRelation``.
If the file is absent, ``load_transform`` returns ``None`` and the
runner uses an identity pass-through over ``ctx.input_view``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable


TransformFn = Callable[[Any, Any], Any]


def load_transform(table_dir: Path) -> TransformFn | None:
    """Return the ``transform`` callable from ``tables/<t>/transform.py``, or None."""
    path = table_dir / "transform.py"
    if not path.is_file():
        return None
    module_name = f"fairway.transforms.{table_dir.name}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not build import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, "transform", None)
    if fn is None:
        return None
    if not callable(fn):
        raise AttributeError(f"{path}: `transform` must be callable")
    return fn  # type: ignore[no-any-return]
