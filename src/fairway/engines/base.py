"""Engine protocol — the contract every compute engine must satisfy.

The protocol is structural (PEP 544): any class that implements these
methods with compatible signatures counts as an Engine, regardless of
whether it explicitly inherits from the protocol.

Both DuckDBEngine and PySparkEngine are checked against this protocol;
adding a new engine (or extending an existing one) should start here —
if the new code doesn't satisfy the protocol, static type checkers and
the test suite will surface it.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Protocol, runtime_checkable


@runtime_checkable
class EngineProtocol(Protocol):
    """Structural type for a fairway compute engine.

    Methods map one-to-one onto pipeline callsites. If you add a callsite
    that needs a new engine method, add it here first and implement it on
    every engine.
    """

    def ingest(
        self,
        input_path: str,
        output_path: str,
        format: str = "csv",
        partition_by: Optional[Iterable[str]] = None,
        metadata: Optional[dict] = None,
        naming_pattern: Optional[str] = None,
        hive_partitioning: bool = False,
        target_rows: Optional[int] = None,
        schema: Optional[dict] = None,
        write_mode: str = "overwrite",
        **kwargs: Any,
    ) -> Any:
        """Read data from input_path and write parquet to output_path."""
        ...

    def enforce_types(
        self,
        input_path: str,
        output_path: str,
        columns: dict,
        on_fail: str = "null",
        **kwargs: Any,
    ) -> Any:
        """Cast columns to target types, writing the result to output_path."""
        ...

    def read_result(self, path: str) -> Any:
        """Return a lazy handle (DuckDB relation / Spark DataFrame) over path."""
        ...

    def infer_schema(
        self,
        path: str,
        format: str = "csv",
        **kwargs: Any,
    ) -> dict:
        """Return a {column_name: type_string} schema for data at path."""
        ...

    def stop(self) -> None:
        """Terminate the engine lifecycle and release held resources.

        This is end-of-life, not a reset: after stop(), subsequent ingest /
        enforce_types / read_result / infer_schema calls must raise a clear
        error (not a NoneType AttributeError). Idempotent — a second call
        is a no-op, so teardown code is safe to call from both explicit
        shutdown and atexit-style finalizers.
        """
        ...
