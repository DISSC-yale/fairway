"""Tests for the v0.3 fragment-based manifest.

Covers the public API in ``fairway.manifest``:

* deterministic ``compute_task_id`` / ``schema_fingerprint`` formulas
* ``source_hash`` algorithm (size-prefixed head+tail)
* ``write_fragment`` round-trip + atomic-write semantics
* ``finalize`` merge → ``manifest.json`` + ``schema_summary.json`` shape
* duplicate-shard-id detection (:class:`DuplicateShardIdError`)
* atomic-write recovery from a simulated mid-write crash

Spirit preserved from the v0.2 manifest tests: concurrent-write safety
(here via per-shard fragment files) and content-change detection (here via
``source_hash``).
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
from pathlib import Path

import pytest

from fairway.manifest import (
    DuplicateShardIdError,
    FRAGMENT_DIR_NAME,
    HEAD_TAIL_BYTES,
    MANIFEST_FILE_NAME,
    MANIFEST_VERSION,
    SCHEMA_SUMMARY_FILE_NAME,
    SMALL_FILE_THRESHOLD,
    build_fragment,
    compute_task_id,
    finalize,
    fragment_path,
    schema_fingerprint,
    source_hash,
    write_fragment,
)


# ---------------------------------------------------------------------------
# task_id / shard_id formula
# ---------------------------------------------------------------------------


class TestComputeTaskId:
    def test_deterministic(self):
        pv = {"state": "CT", "date": "2018-03-15"}
        assert compute_task_id(pv) == compute_task_id(pv)

    def test_pinned_formula_matches_spec(self):
        """Reproduce the exact pinned formula from PLAN.md verbatim."""
        pv = {"state": "CT", "date": "2018-03-15"}
        expected = hashlib.sha256(
            "/".join(f"__{k}={v}" for k, v in sorted(pv.items())).encode("utf-8")
        ).hexdigest()[:16]
        assert compute_task_id(pv) == expected

    def test_order_independent_via_sort(self):
        a = compute_task_id({"state": "CT", "date": "2018-03-15"})
        b = compute_task_id({"date": "2018-03-15", "state": "CT"})
        assert a == b

    def test_different_values_different_id(self):
        a = compute_task_id({"state": "CT"})
        b = compute_task_id({"state": "NY"})
        assert a != b

    def test_returns_16_hex_chars(self):
        sid = compute_task_id({"state": "CT"})
        assert len(sid) == 16
        assert all(c in "0123456789abcdef" for c in sid)


# ---------------------------------------------------------------------------
# schema_fingerprint
# ---------------------------------------------------------------------------


class TestSchemaFingerprint:
    def test_deterministic(self):
        s = [{"name": "a", "dtype": "VARCHAR"}, {"name": "b", "dtype": "INTEGER"}]
        assert schema_fingerprint(s) == schema_fingerprint(s)

    def test_column_order_independent(self):
        a = [{"name": "a", "dtype": "VARCHAR"}, {"name": "b", "dtype": "INTEGER"}]
        b = [{"name": "b", "dtype": "INTEGER"}, {"name": "a", "dtype": "VARCHAR"}]
        assert schema_fingerprint(a) == schema_fingerprint(b)

    def test_different_dtype_different_fingerprint(self):
        a = [{"name": "a", "dtype": "VARCHAR"}]
        b = [{"name": "a", "dtype": "INTEGER"}]
        assert schema_fingerprint(a) != schema_fingerprint(b)

    def test_different_name_different_fingerprint(self):
        a = [{"name": "a", "dtype": "VARCHAR"}]
        b = [{"name": "b", "dtype": "VARCHAR"}]
        assert schema_fingerprint(a) != schema_fingerprint(b)

    def test_returns_16_hex_chars(self):
        fp = schema_fingerprint([{"name": "a", "dtype": "VARCHAR"}])
        assert len(fp) == 16


# ---------------------------------------------------------------------------
# source_hash (size + head + tail)
# ---------------------------------------------------------------------------


class TestSourceHash:
    def test_small_file_full_hash(self, tmp_path):
        f = tmp_path / "tiny.bin"
        f.write_bytes(b"hello world")
        h = source_hash(f)
        expected = hashlib.sha256(struct.pack("<Q", 11) + b"hello world").hexdigest()[:16]
        assert h == expected

    def test_large_file_head_tail_hash(self, tmp_path):
        f = tmp_path / "big.bin"
        size = SMALL_FILE_THRESHOLD * 4
        payload = bytes(range(256)) * (size // 256)
        f.write_bytes(payload)
        head = payload[:HEAD_TAIL_BYTES]
        tail = payload[-HEAD_TAIL_BYTES:]
        expected = hashlib.sha256(struct.pack("<Q", size) + head + tail).hexdigest()[:16]
        assert source_hash(f) == expected

    def test_size_change_changes_hash(self, tmp_path):
        f = tmp_path / "x.bin"
        f.write_bytes(b"a" * (SMALL_FILE_THRESHOLD + 100))
        h1 = source_hash(f)
        f.write_bytes(b"a" * (SMALL_FILE_THRESHOLD + 200))
        assert source_hash(f) != h1

    def test_head_change_changes_hash(self, tmp_path):
        f = tmp_path / "x.bin"
        size = SMALL_FILE_THRESHOLD + HEAD_TAIL_BYTES
        f.write_bytes(b"A" * size)
        h1 = source_hash(f)
        # change head only (preserve size and tail)
        modified = bytearray(b"A" * size)
        modified[0:10] = b"B" * 10
        f.write_bytes(bytes(modified))
        assert source_hash(f) != h1

    def test_returns_16_hex_chars(self, tmp_path):
        f = tmp_path / "x.bin"
        f.write_bytes(b"x")
        assert len(source_hash(f)) == 16


# ---------------------------------------------------------------------------
# Fragment round-trip + atomic write
# ---------------------------------------------------------------------------


def _sample_fragment(state="CT", date="2018-03-15", schema=None, status="ok"):
    if schema is None:
        schema = [{"name": "id", "dtype": "INTEGER"}, {"name": "value", "dtype": "VARCHAR"}]
    return build_fragment(
        partition_values={"state": state, "date": date},
        source_files=[f"/in/{state}_{date}.csv"],
        source_hashes=["deadbeef" * 2],
        schema=schema,
        row_count=100,
        status=status,
        error=None if status == "ok" else "boom",
        encoding_used="utf-8",
    )


class TestWriteFragmentRoundTrip:
    def test_writes_to_correct_path(self, tmp_path):
        frag = _sample_fragment()
        write_fragment(frag, tmp_path)
        target = fragment_path(tmp_path, frag["shard_id"])
        assert target.exists()
        assert target.parent.name == FRAGMENT_DIR_NAME

    def test_round_trip_preserves_fields(self, tmp_path):
        frag = _sample_fragment()
        write_fragment(frag, tmp_path)
        loaded = json.loads(fragment_path(tmp_path, frag["shard_id"]).read_text())
        assert loaded == frag

    def test_no_tmp_file_left_behind(self, tmp_path):
        frag = _sample_fragment()
        write_fragment(frag, tmp_path)
        leftover = list((tmp_path / FRAGMENT_DIR_NAME).glob("*.tmp"))
        assert leftover == []

    def test_idempotent_overwrite(self, tmp_path):
        frag = _sample_fragment()
        write_fragment(frag, tmp_path)
        write_fragment(frag, tmp_path)  # same shard_id; second write is a no-op overwrite
        assert json.loads(fragment_path(tmp_path, frag["shard_id"]).read_text()) == frag


class TestAtomicWriteRecovery:
    def test_orphan_tmp_file_does_not_corrupt_finalize(self, tmp_path):
        """Simulate a mid-write crash: a ``.json.tmp`` orphan exists in the
        fragments dir. ``finalize`` must skip it cleanly (it has the wrong
        suffix) and never produce a torn read."""
        frag_dir = tmp_path / FRAGMENT_DIR_NAME
        frag_dir.mkdir(parents=True)
        # Write one good fragment.
        good = _sample_fragment(state="CT")
        write_fragment(good, tmp_path)
        # Now plant an orphan .tmp file with garbage to mimic a crash mid-write.
        orphan = frag_dir / "deadbeefdeadbeef.json.tmp"
        orphan.write_text("{ truncated json")
        merged = finalize(tmp_path)
        assert merged["fragment_count"] == 1
        assert merged["fragments"][0]["shard_id"] == good["shard_id"]


# ---------------------------------------------------------------------------
# finalize merge
# ---------------------------------------------------------------------------


class TestFinalizeMerge:
    def test_merge_multiple_fragments(self, tmp_path):
        a = _sample_fragment(state="CT")
        b = _sample_fragment(state="NY")
        c = _sample_fragment(state="MA")
        for f in (a, b, c):
            write_fragment(f, tmp_path)
        merged = finalize(tmp_path)
        assert merged["version"] == MANIFEST_VERSION
        assert merged["fragment_count"] == 3
        assert {f["shard_id"] for f in merged["fragments"]} == {a["shard_id"], b["shard_id"], c["shard_id"]}

    def test_writes_manifest_and_summary_files(self, tmp_path):
        write_fragment(_sample_fragment(state="CT"), tmp_path)
        finalize(tmp_path)
        assert (tmp_path / MANIFEST_FILE_NAME).is_file()
        assert (tmp_path / SCHEMA_SUMMARY_FILE_NAME).is_file()

    def test_empty_fragments_dir_yields_empty_merge(self, tmp_path):
        merged = finalize(tmp_path)
        assert merged["fragment_count"] == 0
        assert merged["fragments"] == []

    def test_finalize_atomic_no_tmp_leftover(self, tmp_path):
        write_fragment(_sample_fragment(state="CT"), tmp_path)
        finalize(tmp_path)
        leftover = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
        assert leftover == []


class TestDuplicateShardIdDetection:
    def test_finalize_raises_on_duplicate_shard_id(self, tmp_path):
        """Two fragments with identical ``shard_id`` (same partition_values)
        but different file contents → finalize must raise."""
        frag = _sample_fragment(state="CT")
        write_fragment(frag, tmp_path)
        # Plant a second fragment file with the SAME shard_id under a different
        # filename — finalize must catch the in-content collision.
        evil = dict(frag, source_files=["/in/different.csv"])
        evil_path = (tmp_path / FRAGMENT_DIR_NAME) / "duplicate_collision.json"
        evil_path.write_text(json.dumps(evil))
        with pytest.raises(DuplicateShardIdError):
            finalize(tmp_path)


class TestSchemaSummary:
    def test_distinct_schemas_aggregated(self, tmp_path):
        s1 = [{"name": "id", "dtype": "INTEGER"}]
        s2 = [{"name": "id", "dtype": "INTEGER"}, {"name": "extra", "dtype": "VARCHAR"}]
        write_fragment(_sample_fragment(state="CT", schema=s1), tmp_path)
        write_fragment(_sample_fragment(state="NY", schema=s1), tmp_path)
        write_fragment(_sample_fragment(state="MA", schema=s2), tmp_path)
        finalize(tmp_path)
        summary = json.loads((tmp_path / SCHEMA_SUMMARY_FILE_NAME).read_text())
        # Two distinct fingerprints, with shard counts {2, 1}.
        counts = sorted(d["shard_count"] for d in summary["distinct_schemas"])
        assert counts == [1, 2]
        assert summary["ok_shard_count"] == 3

    def test_column_presence_rates(self, tmp_path):
        s1 = [{"name": "a", "dtype": "VARCHAR"}]
        s2 = [{"name": "a", "dtype": "VARCHAR"}, {"name": "b", "dtype": "VARCHAR"}]
        write_fragment(_sample_fragment(state="CT", schema=s1), tmp_path)
        write_fragment(_sample_fragment(state="NY", schema=s2), tmp_path)
        finalize(tmp_path)
        summary = json.loads((tmp_path / SCHEMA_SUMMARY_FILE_NAME).read_text())
        # `a` appears in both shards → 1.0; `b` in 1 of 2 → 0.5.
        assert summary["column_presence"]["a"] == 1.0
        assert summary["column_presence"]["b"] == 0.5

    def test_column_dtypes_seen_collects_drift(self, tmp_path):
        s1 = [{"name": "v", "dtype": "VARCHAR"}]
        s2 = [{"name": "v", "dtype": "INTEGER"}]
        write_fragment(_sample_fragment(state="CT", schema=s1), tmp_path)
        write_fragment(_sample_fragment(state="NY", schema=s2), tmp_path)
        finalize(tmp_path)
        summary = json.loads((tmp_path / SCHEMA_SUMMARY_FILE_NAME).read_text())
        assert summary["column_dtypes_seen"]["v"] == ["INTEGER", "VARCHAR"]

    def test_error_fragments_excluded_from_summary(self, tmp_path):
        write_fragment(_sample_fragment(state="CT"), tmp_path)
        write_fragment(_sample_fragment(state="NY", status="error"), tmp_path)
        finalize(tmp_path)
        summary = json.loads((tmp_path / SCHEMA_SUMMARY_FILE_NAME).read_text())
        assert summary["ok_shard_count"] == 1


# ---------------------------------------------------------------------------
# build_fragment shape contract (PLAN.md v3.3)
# ---------------------------------------------------------------------------


class TestBuildFragmentShape:
    def test_required_fields_present(self):
        frag = _sample_fragment()
        for key in (
            "shard_id", "partition_values", "source_files", "source_hashes",
            "schema", "schema_fingerprint", "row_count", "ingest_ts",
            "status", "error", "encoding_used",
        ):
            assert key in frag

    def test_shard_id_matches_compute_task_id(self):
        pv = {"state": "CT", "date": "2018-03-15"}
        frag = _sample_fragment(state="CT", date="2018-03-15")
        assert frag["shard_id"] == compute_task_id(pv)

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError):
            build_fragment(
                partition_values={}, source_files=[], source_hashes=[],
                schema=[], row_count=0, status="other", error=None,
                encoding_used="utf-8",
            )


# ---------------------------------------------------------------------------
# Pipeline-integration tests retained from v0.2 — guarded as expected-fail
# until Step 8 wires the pipeline to the new fragment manifest API.
# ---------------------------------------------------------------------------


class TestManifestThroughPipeline:
    """Pipeline must use the manifest to skip unchanged files on second run.

    Both tests are marked xfail until Step 8: pipeline.py still imports the
    v0.2 ``ManifestStore`` shim, which raises NotImplementedError on use.
    """

    @pytest.mark.xfail(reason="pipeline.py uses v0.2 ManifestStore shim; rewired in Step 8", strict=False)
    def test_second_run_skips_unchanged_file(self, fixtures_dir, tmp_path, monkeypatch):
        import time
        from tests.helpers import build_config, get_output_mtime
        from fairway.pipeline import IngestionPipeline

        monkeypatch.chdir(tmp_path)
        config = build_config(tmp_path, table={
            "name": "incremental",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
        })
        pipeline = IngestionPipeline(config)
        pipeline.run()
        mtime_first = get_output_mtime(tmp_path, "incremental")
        time.sleep(0.05)
        pipeline.run()
        mtime_second = get_output_mtime(tmp_path, "incremental")
        assert mtime_first == mtime_second

    @pytest.mark.xfail(reason="pipeline.py uses v0.2 ManifestStore shim; rewired in Step 8", strict=False)
    def test_modified_file_triggers_reprocess(self, tmp_path, monkeypatch):
        from tests.helpers import build_config, read_curated
        from fairway.pipeline import IngestionPipeline

        monkeypatch.chdir(tmp_path)
        source = tmp_path / "source.csv"
        source.write_text("id,name,value\n1,alice,100\n2,bob,200\n")
        config = build_config(tmp_path, table={
            "name": "modified",
            "path": str(source),
            "format": "csv",
            "write_mode": "overwrite",
        })
        pipeline = IngestionPipeline(config)
        pipeline.run()
        first_count = len(read_curated(tmp_path, "modified"))
        assert first_count == 2
        source.write_text("id,name,value\n1,alice,100\n2,bob,200\n3,carol,300\n")
        pipeline.run()
        second_count = len(read_curated(tmp_path, "modified"))
        assert second_count == 3
