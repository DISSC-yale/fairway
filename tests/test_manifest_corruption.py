"""Tests for manifest robustness under the v0.3 fragment design.

Spirit preserved from the v0.2 corruption suite (corruption resilience, batch
rollback, cache invalidation), now expressed in fragment-API terms:

* corrupted manifest JSON → falls back gracefully → corrupted fragment files
  are logged and skipped during ``finalize`` (pin: log + skip, not hard fail).
* batch rollback semantics → no longer relevant (each fragment is a single
  atomic write), but partial-write recovery is covered: an orphan ``.tmp``
  must not be picked up.
* cache invalidation when content changes → ``source_hash`` detects size and
  head/tail changes (the fragment's ``source_hashes`` field is the new cache
  key).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fairway.manifest import (
    DuplicateShardIdError,
    FRAGMENT_DIR_NAME,
    HEAD_TAIL_BYTES,
    MANIFEST_FILE_NAME,
    SCHEMA_SUMMARY_FILE_NAME,
    SMALL_FILE_THRESHOLD,
    build_fragment,
    finalize,
    fragment_path,
    source_hash,
    write_fragment,
)


def _frag(state: str, status: str = "ok") -> dict:
    return build_fragment(
        partition_values={"state": state, "date": "2025-01-01"},
        source_files=[f"/in/{state}.csv"],
        source_hashes=["abc123"],
        schema=[{"name": "id", "dtype": "INTEGER"}],
        row_count=10,
        status=status,
        error=None if status == "ok" else "synthetic",
        encoding_used="utf-8",
    )


@pytest.mark.local
def test_corrupt_fragment_is_logged_and_skipped(tmp_path, caplog):
    """Pinned behavior: a corrupt fragment JSON is logged and skipped during
    finalize. Good fragments still merge cleanly.

    This is the v0.3 analogue of the v0.2 ``test_corrupted_manifest_falls_back_to_empty``:
    corruption must not crash the pipeline.
    """
    write_fragment(_frag("CT"), tmp_path)
    write_fragment(_frag("NY"), tmp_path)

    corrupt = (tmp_path / FRAGMENT_DIR_NAME) / "deadbeefdeadbeef.json"
    corrupt.write_text("{ not valid json !!!")

    with caplog.at_level("WARNING", logger="fairway.manifest"):
        merged = finalize(tmp_path)

    assert merged["fragment_count"] == 2  # only the 2 valid fragments merged
    assert any("corrupt fragment" in rec.message for rec in caplog.records)
    # Merged manifest still written successfully.
    assert (tmp_path / MANIFEST_FILE_NAME).is_file()
    assert (tmp_path / SCHEMA_SUMMARY_FILE_NAME).is_file()


@pytest.mark.local
def test_orphan_tmp_file_does_not_become_a_torn_fragment(tmp_path):
    """Simulated mid-write crash leaves only ``<sid>.json.tmp`` behind. The
    ``.tmp`` suffix means ``finalize`` ignores it — no partial-commit.

    v0.3 analogue of the v0.2 ``test_batch_context_rolls_back_on_exception``:
    no torn data is ever surfaced to a reader.
    """
    write_fragment(_frag("CT"), tmp_path)
    frag_dir = tmp_path / FRAGMENT_DIR_NAME
    orphan = frag_dir / "feedfacefeedface.json.tmp"
    orphan.write_text('{"shard_id": "feedfacefeedface", "status":')  # truncated mid-write

    merged = finalize(tmp_path)
    assert merged["fragment_count"] == 1
    assert merged["fragments"][0]["partition_values"]["state"] == "CT"
    # The orphan stays on disk (operator can clean it up); finalize never read it.
    assert orphan.exists()


@pytest.mark.local
def test_duplicate_shard_id_hard_fails(tmp_path):
    """Two fragments with the same shard_id → finalize raises (no silent merge).

    Pinned behavior: corruption-of-intent (two writers claiming the same
    partition) is louder than data corruption — must raise, not skip.
    """
    a = _frag("CT")
    write_fragment(a, tmp_path)
    # Plant a second file under a different filename but identical shard_id.
    duplicate_path = (tmp_path / FRAGMENT_DIR_NAME) / "alias_for_ct.json"
    duplicate_path.write_text(json.dumps(dict(a, source_files=["/elsewhere.csv"])))
    with pytest.raises(DuplicateShardIdError):
        finalize(tmp_path)


@pytest.mark.local
def test_source_hash_detects_size_change(tmp_path):
    """v0.3 analogue of the v0.2 ``test_cache_invalidated_when_action_changes``:
    a content change (here: appending a row → size change) invalidates the
    fragment's recorded ``source_hash``."""
    src = tmp_path / "data.csv"
    src.write_bytes(b"id,name\n1,alice\n")
    h1 = source_hash(src)

    src.write_bytes(b"id,name\n1,alice\n2,bob\n")
    h2 = source_hash(src)

    assert h1 != h2, "size change must invalidate the source_hash cache key"


@pytest.mark.local
def test_source_hash_detects_head_change_in_large_file(tmp_path):
    """Head-of-file changes (most common case) are caught even when size is
    preserved — the head 4 KB are part of the fingerprint."""
    src = tmp_path / "big.bin"
    size = SMALL_FILE_THRESHOLD + HEAD_TAIL_BYTES * 2
    src.write_bytes(b"A" * size)
    h1 = source_hash(src)

    modified = bytearray(b"A" * size)
    modified[:8] = b"BBBBBBBB"  # head edit, same size
    src.write_bytes(bytes(modified))
    h2 = source_hash(src)

    assert h1 != h2


@pytest.mark.local
def test_finalize_recovers_when_only_corrupt_fragments_present(tmp_path):
    """Edge case: every fragment file is corrupt → finalize emits an empty
    merged manifest rather than crashing. Researcher can then re-run shards."""
    frag_dir = tmp_path / FRAGMENT_DIR_NAME
    frag_dir.mkdir(parents=True)
    (frag_dir / "aaaaaaaaaaaaaaaa.json").write_text("not json")
    (frag_dir / "bbbbbbbbbbbbbbbb.json").write_text("{also not json")

    merged = finalize(tmp_path)
    assert merged["fragment_count"] == 0
    assert (tmp_path / MANIFEST_FILE_NAME).is_file()
