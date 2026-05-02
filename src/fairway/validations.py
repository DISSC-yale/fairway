"""Runner-side validation checks. Stub for Step 7.1; bodies in Step 7.2.

Defines :class:`ShardValidationError` and the
:func:`apply_validations` entry point that the runner calls after the
user's transform returns a relation.
"""
from __future__ import annotations

from typing import Any

from .config import Validations


class ShardValidationError(RuntimeError):
    """Raised on the first failing validation check for a shard."""


def apply_validations(rel: Any, validations: Validations) -> None:
    """Run each validation in ``validations`` against ``rel``. (Step 7.2)"""
    raise NotImplementedError("apply_validations is filled in Step 7.2")
