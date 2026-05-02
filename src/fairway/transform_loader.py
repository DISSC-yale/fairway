"""Minimal Python plugin loader: file path → top-level ``transform`` callable.

The runner hands the returned callable a DuckDB connection and an
``IngestCtx``. No registry, no base class, no discovery.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable


def load_transform(path: Path) -> Callable[[Any, Any], Any]:
    """Import ``path`` and return its top-level ``transform`` callable.

    Raises ``FileNotFoundError`` if missing, ``ImportError`` if the spec
    cannot be built, ``AttributeError`` if no ``transform`` is defined.
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
