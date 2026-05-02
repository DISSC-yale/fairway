"""Integration tests for :func:`fairway.duckdb_runner.run_shard`.

Covers the four contract points from PLAN.md Step 7.3:

* (a) output partition tree under ``ctx.output_path``;
* (b) successful fragment with schema/fingerprint/row_count;
* (c) error path produces error fragment but does not re-raise;
* (d) ``sub_partition_by`` adds a second-level Hive directory.
"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from fairway.config import Config
from fairway.ctx import IngestCtx
from fairway.duckdb_runner import ShardResult, run_shard


def _make_config(tmp_path: Path, transform_path: Path, **over: object) -> Config:
    cfg = Config(
        dataset_name="ds",
        python=transform_path,
        storage_root=tmp_path / "store",
        source_glob=str(tmp_path / "*.csv"),
        naming_pattern=r"(?P<state>\w+)_(?P<date>[\d-]+)\.csv",
        partition_by=["state", "date"],
        delimiter=",",
        slurm_cpus_per_task=2,
        slurm_mem="1G",
    )
    return replace(cfg, **over) if over else cfg


def _make_ctx(tmp_path: Path, cfg: Config, csv_path: Path) -> IngestCtx:
    scratch = tmp_path / "scratch"
    scratch.mkdir(exist_ok=True)
    out = tmp_path / "out" / "shard"
    return IngestCtx(
        config=cfg,
        input_paths=[csv_path],
        output_path=out,
        partition_values={"state": "CT", "date": "2018-03-15"},
        shard_id="shard-test",
        scratch_dir=scratch,
    )


def _write_transform(path: Path, body: str = "") -> Path:
    path.write_text(
        "from fairway.defaults import default_ingest\n"
        "def transform(con, ctx):\n"
        "    rel = default_ingest(con, ctx)\n"
        f"    {body or 'return rel'}\n",
        encoding="utf-8",
    )
    return path


def test_run_shard_success_writes_parquet_and_fragment(tmp_path: Path) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text("id,name,year\n1,foo,2018\n2,bar,2018\n3,baz,2018\n", encoding="utf-8")
    transform = _write_transform(tmp_path / "ds.py")
    cfg = _make_config(tmp_path, transform)
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)

    assert isinstance(result, ShardResult)
    assert result.status == "ok"
    assert result.error is None
    assert result.row_count == 3
    assert result.schema_fingerprint and len(result.schema_fingerprint) == 16
    assert result.encoding_used == "utf-8"
    # Hive layout under output_path: __state=CT/__date=2018-03-15/
    parquets = sorted(ctx.output_path.rglob("*.parquet"))
    assert parquets, "expected at least one parquet file"
    assert any("__state=CT" in str(p) for p in parquets)
    assert any("__date=2018-03-15" in str(p) for p in parquets)
    assert result.output_paths == parquets

    # Fragment lives at <storage_root>/raw/<dataset>/_fragments/<shard_id>.json
    layer_root = cfg.storage_root / "raw" / cfg.dataset_name
    fragments = list((layer_root / "_fragments").glob("*.json"))
    assert len(fragments) == 1
    payload = json.loads(fragments[0].read_text())
    assert payload["status"] == "ok"
    assert payload["error"] is None
    assert payload["row_count"] == 3
    assert payload["partition_values"] == {"state": "CT", "date": "2018-03-15"}
    assert payload["schema_fingerprint"] == result.schema_fingerprint
    assert any(c["name"] == "__state" for c in payload["schema"])
    assert any(c["name"] == "__date" for c in payload["schema"])
    assert payload["source_files"] == [str(csv)]
    assert payload["source_hashes"] and len(payload["source_hashes"][0]) == 16


def test_run_shard_user_transform_error_writes_error_fragment(tmp_path: Path) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text("a,b\n1,2\n", encoding="utf-8")
    transform = tmp_path / "ds.py"
    transform.write_text(
        "def transform(con, ctx):\n"
        "    raise RuntimeError('boom-from-user')\n",
        encoding="utf-8",
    )
    cfg = _make_config(tmp_path, transform)
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)

    assert result.status == "error"
    assert result.error is not None and "boom-from-user" in result.error
    assert result.row_count == 0
    assert result.schema == []
    assert result.output_paths == []

    layer_root = cfg.storage_root / "raw" / cfg.dataset_name
    fragments = list((layer_root / "_fragments").glob("*.json"))
    assert len(fragments) == 1
    payload = json.loads(fragments[0].read_text())
    assert payload["status"] == "error"
    assert "boom-from-user" in payload["error"]
    assert payload["row_count"] == 0
    assert payload["partition_values"] == {"state": "CT", "date": "2018-03-15"}


def test_run_shard_validation_error_is_captured(tmp_path: Path) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text("a,b\n1,2\n", encoding="utf-8")
    transform = _write_transform(tmp_path / "ds.py")
    from fairway.config import Validations
    cfg = _make_config(
        tmp_path,
        transform,
        validations=Validations(min_rows=999),
    )
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)

    assert result.status == "error"
    assert "min_rows" in (result.error or "")
    layer_root = cfg.storage_root / "raw" / cfg.dataset_name
    fragments = list((layer_root / "_fragments").glob("*.json"))
    assert len(fragments) == 1
    payload = json.loads(fragments[0].read_text())
    assert payload["status"] == "error"


def test_run_shard_sub_partition_by_creates_extra_level(tmp_path: Path) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text("id,year\n1,2018\n2,2019\n", encoding="utf-8")
    transform = _write_transform(tmp_path / "ds.py")
    cfg = _make_config(tmp_path, transform, sub_partition_by=["year"])
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)

    assert result.status == "ok", result.error
    parquets = sorted(ctx.output_path.rglob("*.parquet"))
    paths = [str(p) for p in parquets]
    assert any("__state=CT" in p and "__date=2018-03-15" in p and "year=2018" in p for p in paths)
    assert any("year=2019" in p for p in paths)


def test_run_shard_storage_layer_override_is_respected(tmp_path: Path) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text("a,b\n1,2\n2,3\n", encoding="utf-8")
    transform = _write_transform(tmp_path / "ds.py")
    raw_override = tmp_path / "raw_override"
    cfg = _make_config(tmp_path, transform, storage_raw=raw_override)
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)
    assert result.status == "ok", result.error

    fragments = list((raw_override / cfg.dataset_name / "_fragments").glob("*.json"))
    assert len(fragments) == 1
    # The default storage_root/raw/ path must NOT be used.
    assert not (cfg.storage_root / "raw" / cfg.dataset_name / "_fragments").exists()


@pytest.mark.parametrize("apply_type_a", [True, False])
def test_run_shard_apply_type_a_flag(tmp_path: Path, apply_type_a: bool) -> None:
    csv = tmp_path / "CT_2018-03-15.csv"
    csv.write_text('"Mixed Case",val\n1,2\n', encoding="utf-8")
    transform = _write_transform(tmp_path / "ds.py")
    cfg = _make_config(tmp_path, transform, apply_type_a=apply_type_a)
    ctx = _make_ctx(tmp_path, cfg, csv)

    con = duckdb.connect(":memory:")
    result = run_shard(con, ctx)
    assert result.status == "ok", result.error

    names = {c.name for c in result.schema}
    if apply_type_a:
        assert "mixed_case" in names
    else:
        assert "Mixed Case" in names
