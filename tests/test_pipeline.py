"""Tests for :func:`fairway.pipeline.run_pipeline` (rewrite/v0.3 Step 8.2).

Verifies the pipeline-level wiring of the duckdb_runner manifest-fragment
contract. The runner is the sole writer of manifest fragments
(:func:`fairway.duckdb_runner.run_shard` calls
:func:`fairway.manifest.write_fragment` in both the ok and error branches);
the pipeline only orchestrates and aggregates results. These tests confirm
that:

* a happy-path single shard produces a ``status="ok"`` fragment with
  non-empty ``schema``/``schema_fingerprint``/``row_count``;
* a forced validation failure produces a ``status="error"`` fragment with
  a populated ``error`` string and the pipeline does NOT re-raise;
* ``shard_index=0`` runs exactly that one shard;
* ``shard_index=None`` runs every shard and ``ok_count + error_count``
  equals ``total_shards``.

Each test materialises its own YAML config + transform.py inside
``tmp_path`` and instantiates :class:`fairway.config.Config` via
:func:`fairway.config.load_config` — no shared fixture state.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import yaml

from fairway.config import load_config
from fairway.pipeline import PipelineResult, run_pipeline


# ---------------------------------------------------------------------------
# Helpers — keep each test self-contained.
# ---------------------------------------------------------------------------

_DEFAULT_TRANSFORM = textwrap.dedent(
    """\
    from fairway.defaults import default_ingest

    def transform(con, ctx):
        return default_ingest(con, ctx)
    """
)


def _write_transform(path: Path, body: str = _DEFAULT_TRANSFORM) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def _write_csv(path: Path, header: list[str], rows: list[tuple]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    lines.extend(",".join(str(c) for c in row) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_config(
    yaml_path: Path,
    *,
    dataset_name: str,
    python: Path,
    storage_root: Path,
    source_glob: str,
    naming_pattern: str = r"(?P<state>[A-Z]+)_(?P<year>\d{4})\.csv",
    partition_by: list[str] | None = None,
    extra: dict | None = None,
) -> Path:
    payload: dict = {
        "dataset_name": dataset_name,
        "python": str(python),
        "storage_root": str(storage_root),
        "source_glob": source_glob,
        "naming_pattern": naming_pattern,
        "partition_by": partition_by or ["state", "year"],
        "delimiter": ",",
        "slurm_cpus_per_task": 2,
        "slurm_mem": "1G",
    }
    if extra:
        payload.update(extra)
    yaml_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return yaml_path


def _fragment_dir(config) -> Path:
    """Mirror :func:`fairway.duckdb_runner._layer_root` for assertions."""
    return Path(config.storage_root) / config.layer / config.dataset_name / "_fragments"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_run_pipeline_happy_path_writes_ok_fragment(tmp_path: Path) -> None:
    """One matching CSV → one shard → status=ok fragment with full payload."""
    _write_csv(
        tmp_path / "inputs" / "CT_2023.csv",
        ["id", "value"],
        [(1, "a"), (2, "b"), (3, "c")],
    )
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_happy",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={"scratch_dir": str(tmp_path / "scratch")},
    )
    config = load_config(yaml_path)

    result = run_pipeline(config)

    assert isinstance(result, PipelineResult)
    assert result.total_shards == 1
    assert result.ok_count == 1
    assert result.error_count == 0
    assert len(result.shard_results) == 1
    sr = result.shard_results[0]
    assert sr.status == "ok"
    assert sr.error is None
    assert sr.row_count == 3
    assert sr.schema_fingerprint and len(sr.schema_fingerprint) == 16

    fragments = list(_fragment_dir(config).glob("*.json"))
    assert len(fragments) == 1, fragments
    payload = json.loads(fragments[0].read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["error"] is None
    assert payload["row_count"] == 3
    assert payload["partition_values"] == {"state": "CT", "year": "2023"}
    assert payload["schema_fingerprint"] == sr.schema_fingerprint
    assert payload["schema"], "expected non-empty schema in fragment"


def test_run_pipeline_validation_error_records_fragment_without_reraising(
    tmp_path: Path,
) -> None:
    """Forced ShardValidationError surfaces as error_count, NOT an exception."""
    _write_csv(
        tmp_path / "inputs" / "CT_2023.csv",
        ["id", "value"],
        [(1, "a"), (2, "b")],
    )
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_validate_err",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={
            "scratch_dir": str(tmp_path / "scratch"),
            "validations": {"min_rows": 999},
        },
    )
    config = load_config(yaml_path)

    # Must NOT raise: pipeline absorbs shard failures into PipelineResult.
    result = run_pipeline(config)

    assert result.total_shards == 1
    assert result.ok_count == 0
    assert result.error_count == 1
    sr = result.shard_results[0]
    assert sr.status == "error"
    assert sr.error and "min_rows" in sr.error

    fragments = list(_fragment_dir(config).glob("*.json"))
    assert len(fragments) == 1, fragments
    payload = json.loads(fragments[0].read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["error"], "error fragment must record a non-empty error string"
    assert "min_rows" in payload["error"]
    assert payload["row_count"] == 0


def test_run_pipeline_user_transform_exception_recorded_as_error_fragment(
    tmp_path: Path,
) -> None:
    """Custom exception from user transform.py → error fragment, no re-raise."""
    _write_csv(
        tmp_path / "inputs" / "CT_2023.csv",
        ["id"],
        [(1,)],
    )
    bad_transform = _write_transform(
        tmp_path / "ds.py",
        body=(
            "def transform(con, ctx):\n"
            "    raise RuntimeError('user-transform-boom')\n"
        ),
    )
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_user_err",
        python=bad_transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={"scratch_dir": str(tmp_path / "scratch")},
    )
    config = load_config(yaml_path)

    result = run_pipeline(config)

    assert result.total_shards == 1
    assert result.error_count == 1
    assert result.ok_count == 0
    payload = json.loads(
        next(_fragment_dir(config).glob("*.json")).read_text(encoding="utf-8")
    )
    assert payload["status"] == "error"
    assert "user-transform-boom" in payload["error"]


def test_run_pipeline_single_shard_index_zero_runs_that_shard(
    tmp_path: Path,
) -> None:
    """``shard_index=0`` against a one-shard source: total_shards=1, one ok result."""
    _write_csv(
        tmp_path / "inputs" / "CT_2023.csv",
        ["id", "v"],
        [(1, "a"), (2, "b")],
    )
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_single",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={"scratch_dir": str(tmp_path / "scratch")},
    )
    config = load_config(yaml_path)

    result = run_pipeline(config, shard_index=0)

    assert result.total_shards == 1
    assert len(result.shard_results) == 1
    assert result.ok_count == 1
    assert result.error_count == 0
    assert len(list(_fragment_dir(config).glob("*.json"))) == 1


def test_run_pipeline_shard_index_selects_exactly_one_of_many(
    tmp_path: Path,
) -> None:
    """With N enumerated shards, ``shard_index=k`` runs exactly one of them.

    ``total_shards`` reflects discovery (all enumerated specs), but
    ``shard_results`` and the on-disk fragment count must reflect only the
    single shard requested — that is the Slurm-array contract.
    """
    for state in ("CT", "MA"):
        _write_csv(
            tmp_path / "inputs" / f"{state}_2023.csv",
            ["id", "v"],
            [(1, "a")],
        )
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_pick_one",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={"scratch_dir": str(tmp_path / "scratch")},
    )
    config = load_config(yaml_path)

    result = run_pipeline(config, shard_index=0)

    assert result.total_shards == 2  # both partitions enumerated
    assert len(result.shard_results) == 1  # only one executed
    assert result.ok_count + result.error_count == 1
    # Only one fragment written, despite two shards being known.
    assert len(list(_fragment_dir(config).glob("*.json"))) == 1


def test_run_pipeline_all_shards_iterates_every_partition(tmp_path: Path) -> None:
    """``shard_index=None`` runs all shards; counts must reconcile."""
    for state in ("CT", "MA", "NY"):
        _write_csv(
            tmp_path / "inputs" / f"{state}_2023.csv",
            ["id", "v"],
            [(1, "x"), (2, "y")],
        )
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_all",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "inputs" / "*.csv"),
        extra={"scratch_dir": str(tmp_path / "scratch")},
    )
    config = load_config(yaml_path)

    result = run_pipeline(config, shard_index=None)

    assert result.total_shards == 3
    assert result.ok_count + result.error_count == result.total_shards
    assert result.ok_count == 3
    assert result.error_count == 0
    # One fragment per shard, all with status=ok.
    fragments = sorted(_fragment_dir(config).glob("*.json"))
    assert len(fragments) == 3
    for f in fragments:
        payload = json.loads(f.read_text(encoding="utf-8"))
        assert payload["status"] == "ok"
        assert payload["error"] is None
