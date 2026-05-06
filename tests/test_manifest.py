"""Tests for partition-keyed manifest + fragment merge."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fairway import manifest


def _table_dir(tmp_path: Path) -> Path:
    td = tmp_path / "tables" / "x"
    td.mkdir(parents=True)
    return td


def test_partition_keyed_structure(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.record_leaf(
        m, "processed", "state=CT/year=2023",
        source_files=["a.csv"], source_hashes=["abc"],
        schema_fingerprint="fp_v1",
        row_count=42, status="ok",
    )
    manifest.save_manifest(m, td)
    on_disk = json.loads(manifest.manifest_path(td).read_text())
    assert on_disk["version"] == "0.3"
    assert on_disk["table"] == "x"
    parts = on_disk["layers"]["processed"]["partitions"]
    assert "state=CT/year=2023" in parts
    leaf = parts["state=CT/year=2023"]
    assert leaf["row_count"] == 42
    assert leaf["status"] == "ok"
    assert leaf["schema_fingerprint"] == "fp_v1"


def test_atomic_write_no_partial_on_replace_failure(tmp_path, monkeypatch):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.save_manifest(m, td)
    original = manifest.manifest_path(td).read_text()
    real_replace = manifest.os.replace
    calls = {"n": 0}
    def boom(src, dst):
        calls["n"] += 1
        raise OSError("simulated")
    monkeypatch.setattr(manifest.os, "replace", boom)
    m2 = manifest.empty_manifest("x")
    m2["finalized_at"] = "2026-01-01T00:00:00+00:00"
    with pytest.raises(OSError):
        manifest.save_manifest(m2, td)
    monkeypatch.setattr(manifest.os, "replace", real_replace)
    assert manifest.manifest_path(td).read_text() == original


def test_finalize_merges_fragments(tmp_path):
    td = _table_dir(tmp_path)
    for sid, leaf in (("s1", "state=CT/year=2023"), ("s2", "state=NY/year=2023")):
        manifest.write_fragment(
            td, sid, layer="processed",
            leaves={leaf: {
                "source_files": [f"{sid}.csv"],
                "source_hashes": ["h"],
                "schema_fingerprint": "fp_v1",
                "row_count": 1, "status": "ok",
                "ingest_ts": "2026-05-05T00:00:00+00:00",
            }},
            shard_by=["state", "year"],
        )
    m = manifest.finalize(td)
    parts = m["layers"]["processed"]["partitions"]
    assert "state=CT/year=2023" in parts and "state=NY/year=2023" in parts
    assert manifest.fragment_count(td) == 0


def test_finalize_empty_fragments_noop(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.finalize(td)
    assert m["layers"]["processed"]["partitions"] == {}
    assert m["finalized_at"] is not None


def test_is_leaf_valid_all_conditions(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.record_leaf(
        m, "processed", "leaf",
        source_files=["a.csv"], source_hashes=["h"],
        schema_fingerprint="fp", row_count=1, status="ok",
    )
    assert manifest.is_leaf_valid(m, "processed", "leaf", ["a.csv"], ["h"], "fp")
    assert not manifest.is_leaf_valid(m, "processed", "leaf", ["a.csv"], ["other"], "fp")
    assert not manifest.is_leaf_valid(m, "processed", "leaf", ["a.csv"], ["h"], "fp_v2")
    assert not manifest.is_leaf_valid(m, "processed", "missing_leaf", ["a.csv"], ["h"], "fp")


def test_is_leaf_valid_status_error(tmp_path):
    m = manifest.empty_manifest("x")
    manifest.record_leaf(
        m, "processed", "leaf",
        source_files=["a"], source_hashes=["h"],
        schema_fingerprint="fp", row_count=0, status="error",
    )
    assert not manifest.is_leaf_valid(m, "processed", "leaf", ["a"], ["h"], "fp")


def test_no_legacy_fields(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.record_leaf(m, "processed", "leaf",
                         source_files=["a"], source_hashes=["h"],
                         schema_fingerprint="fp", row_count=1, status="ok")
    manifest.save_manifest(m, td)
    blob = manifest.manifest_path(td).read_text()
    for legacy in ("schemas", "column_presence", "column_dtypes_seen", "schema_history"):
        assert legacy not in blob


def test_encoding_used_omitted_when_not_exercised(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.record_leaf(m, "processed", "leaf",
                         source_files=["a"], source_hashes=["h"],
                         schema_fingerprint="fp", row_count=1, status="ok")
    leaf = m["layers"]["processed"]["partitions"]["leaf"]
    assert "encoding_used" not in leaf


def test_failed_shards_cleared_on_new_submit(tmp_path):
    td = _table_dir(tmp_path)
    m = manifest.empty_manifest("x")
    manifest.set_last_run(m, "processed",
                          shard_by=["state"],
                          shards_observed=["s1"],
                          failed_shards=[{"shard_id": "s1", "error_message": "boom"}])
    manifest.clear_last_run(m)
    assert m["layers"]["processed"]["last_run"] is None


def test_finalize_records_failed_shard(tmp_path):
    td = _table_dir(tmp_path)
    manifest.write_fragment(
        td, "s1", layer="processed",
        leaves={},
        failed={"shard_id": "s1", "error_message": "boom",
                "failed_at": "2026-01-01T00:00:00+00:00",
                "leaves_attempted": ["leaf"]},
        shard_by=["state"],
    )
    m = manifest.finalize(td)
    failed = m["layers"]["processed"]["last_run"]["failed_shards"]
    assert len(failed) == 1 and failed[0]["shard_id"] == "s1"


def test_source_hash_stable(tmp_path):
    p = tmp_path / "f.csv"
    p.write_text("hello", encoding="utf-8")
    h1 = manifest.source_hash(p)
    h2 = manifest.source_hash(p)
    assert h1 == h2 and len(h1) == 16
