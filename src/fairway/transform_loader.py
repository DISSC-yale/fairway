"""Minimal Python plugin loader for user-supplied transforms.

Imports a user's ``<dataset>.py`` from a file path and returns its
top-level ``transform`` callable. There is no registry, no base class,
and no discovery — just file-path-to-function lookup. The runner stage
hands the returned callable an open ``DuckDBPyConnection`` and an
:class:`~fairway.ctx.IngestCtx`.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover — import-only
    from .ctx import IngestCtx


def load_transform(path: Path) -> Callable[[Any, "IngestCtx"], Any]:
    """Import ``path`` as a module and return its top-level ``transform``.

    Raises:
        FileNotFoundError: if ``path`` does not exist on disk.
        AttributeError: if the imported module has no ``transform`` attribute.
    """
    if not path.exists():
        raise FileNotFoundError(f"transform file not found: {path}")
    spec = importlib.util.spec_from_file_location(f"fairway_transform_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not build import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "transform"):
        raise AttributeError(f"{path} defines no top-level `transform` function")
    return module.transform  # type: ignore[no-any-return]
