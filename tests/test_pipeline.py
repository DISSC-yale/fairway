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

from fairway import manifest
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


# ---------------------------------------------------------------------------
# Step 12 smoke test — end-to-end on a synthetic state×date fixture.
# ---------------------------------------------------------------------------

def _write_tsv(path: Path, header: list[str], rows: int, *, extra_col: bool) -> Path:
    """Materialize ~1 MB of synthetic TSV in ``path``.

    The ``id`` and ``value`` columns are present in every file; ``extra_col``
    appends a third column that produces the schema-divergent shard.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")
        for i in range(rows):
            cells = [str(i), f"value_{i:08d}"]
            if extra_col:
                cells.append(f"extra_{i:08d}")
            f.write("\t".join(cells) + "\n")
    return path


def test_run_pipeline_smoke_state_date_synthetic_fixture(tmp_path: Path) -> None:
    """Step 12 smoke test — covers the full ingest pipeline end-to-end on a
    < 100 MB synthetic fixture. Real-data scale validation is the operator's
    responsibility, not in-loop CI.

    Fixture: 4 TSVs (~1 MB each) named ``mortality_<STATE>_<YYYY-MM-DD>.tsv``
    spanning two states × two dates. Three files share schema ``[id, value]``;
    the fourth (NY, 2019-04-01) carries an additional ``extra`` column.

    Step 13.5 correctness fix: each shard's ``DESCRIBE`` reads ONLY its own
    Hive partition leaf (``__state=<S>/__date=<D>/**/*.parquet``), so the
    schema fingerprint depends solely on what THAT shard wrote — not on
    sibling shards visible to a dataset-root glob. The divergent shard's
    fingerprint must therefore be unique regardless of ``shard_id`` sort
    order. (See companion tests for the determinism / reverse-order
    invariants.)
    """
    rows_per_file = 50_000  # ~1 MB per TSV — well under the 100 MB cap.
    inputs = tmp_path / "inputs"
    schema_a = ["id", "value"]
    schema_b = ["id", "value", "extra"]
    _write_tsv(inputs / "mortality_CT_2018-03-15.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_NY_2018-03-15.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_CT_2019-04-01.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_NY_2019-04-01.tsv", schema_b, rows_per_file, extra_col=True)

    # Sanity: every fixture file is between 0.5 MB and 10 MB; aggregate < 100 MB.
    sizes = [p.stat().st_size for p in sorted(inputs.glob("*.tsv"))]
    assert all(500_000 < s < 10 * 1024 * 1024 for s in sizes), sizes
    assert sum(sizes) < 100 * 1024 * 1024

    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name="ds_smoke",
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(inputs / "*.tsv"),
        naming_pattern=r"mortality_(?P<state>[A-Z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.tsv",
        partition_by=["state", "date"],
        extra={"scratch_dir": str(tmp_path / "scratch"), "delimiter": "\t"},
    )
    config = load_config(yaml_path)

    # (3) Full all-shards iteration.
    result = run_pipeline(config, shard_index=None)

    # (4f) PipelineResult shape.
    assert isinstance(result, PipelineResult)
    assert result.total_shards == 4
    assert result.ok_count == 4
    assert result.error_count == 0
    assert result.ok_count + result.error_count == result.total_shards

    dataset_root = Path(config.storage_root) / config.layer / config.dataset_name

    # (4a) Output partition tree shape — Hive-style ``__key=val`` directories
    # with at least one parquet file inside each (state, date) combo.
    expected_partitions = {
        ("CT", "2018-03-15"),
        ("NY", "2018-03-15"),
        ("CT", "2019-04-01"),
        ("NY", "2019-04-01"),
    }
    for state, date in expected_partitions:
        part_dir = dataset_root / f"__state={state}" / f"__date={date}"
        assert part_dir.is_dir(), f"missing partition dir {part_dir}"
        assert list(part_dir.glob("*.parquet")), f"no parquet in {part_dir}"

    # (4b) One manifest fragment per shard, written by the runner under
    # ``<dataset_root>/_fragments/<shard_id>.json``.
    fragments = sorted(_fragment_dir(config).glob("*.json"))
    assert len(fragments) == len(expected_partitions)

    payloads = [json.loads(f.read_text(encoding="utf-8")) for f in fragments]

    # (4c) Every fragment is status='ok' with non-empty schema/fingerprint.
    for p in payloads:
        assert p["status"] == "ok", f"unexpected error fragment: {p.get('error')!r}"
        assert p["error"] is None
        assert p["schema"], "ok fragment must record a schema"
        assert p["schema_fingerprint"] and len(p["schema_fingerprint"]) == 16
        assert p["row_count"] == rows_per_file

    by_partition = {
        (p["partition_values"]["state"], p["partition_values"]["date"]): p
        for p in payloads
    }
    assert set(by_partition) == expected_partitions

    # (4d) ``manifest.finalize`` materialises both merged outputs.
    merged = manifest.finalize(dataset_root)
    manifest_json = dataset_root / "manifest.json"
    schema_summary_json = dataset_root / "schema_summary.json"
    assert manifest_json.is_file()
    assert schema_summary_json.is_file()
    assert merged["fragment_count"] == len(expected_partitions)
    assert merged["version"] == manifest.MANIFEST_VERSION
    assert {f["shard_id"] for f in merged["fragments"]} == {p["shard_id"] for p in payloads}

    # (4e) Schema fingerprint detection: the divergent (NY, 2019-04-01)
    # shard's fingerprint is unique; the three same-schema shards share one.
    divergent_fp = by_partition[("NY", "2019-04-01")]["schema_fingerprint"]
    same_schema_fps = {
        by_partition[k]["schema_fingerprint"]
        for k in expected_partitions
        if k != ("NY", "2019-04-01")
    }
    assert len(same_schema_fps) == 1, (
        f"shards with identical [id, value] schema must share a fingerprint; got {same_schema_fps}"
    )
    assert divergent_fp not in same_schema_fps, (
        "shard with the extra column must produce a distinct fingerprint"
    )

    # (4g) ``schema_summary.json`` reflects exactly the two unique schemas
    # observed across shards — and only ok shards contribute.
    summary = json.loads(schema_summary_json.read_text(encoding="utf-8"))
    assert summary["ok_shard_count"] == len(expected_partitions)
    summary_fingerprints = {d["fingerprint"] for d in summary["distinct_schemas"]}
    assert summary_fingerprints == same_schema_fps | {divergent_fp}
    assert len(summary["distinct_schemas"]) == 2
    by_fp = {d["fingerprint"]: d for d in summary["distinct_schemas"]}
    assert by_fp[divergent_fp]["shard_count"] == 1
    assert by_fp[next(iter(same_schema_fps))]["shard_count"] == 3


# ---------------------------------------------------------------------------
# Step 13.5 — schema_fingerprint correctness across orderings.
#
# The pre-fix runner globbed the dataset root for ``DESCRIBE``; under
# Slurm-array concurrency the recorded fingerprint depended on which
# sibling shards happened to be visible. The fix narrows ``DESCRIBE`` to
# the shard's own Hive partition leaf, so fingerprints must be:
#
# * deterministic — same fixture run twice produces byte-identical fps;
# * order-independent — running shards individually in any order
#   produces the same fp each shard produces in all-shards mode.
# ---------------------------------------------------------------------------

def _build_smoke_config(tmp_path: Path, *, dataset_name: str) -> object:
    """Materialise the Step 12 four-shard state×date fixture in ``tmp_path``."""
    inputs = tmp_path / "inputs"
    schema_a = ["id", "value"]
    schema_b = ["id", "value", "extra"]
    rows_per_file = 2_000  # smaller than smoke (≪ 1 MB) — these tests run more iterations.
    _write_tsv(inputs / "mortality_CT_2018-03-15.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_NY_2018-03-15.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_CT_2019-04-01.tsv", schema_a, rows_per_file, extra_col=False)
    _write_tsv(inputs / "mortality_NY_2019-04-01.tsv", schema_b, rows_per_file, extra_col=True)
    transform = _write_transform(tmp_path / "ds.py")
    yaml_path = _write_config(
        tmp_path / "ds.yaml",
        dataset_name=dataset_name,
        python=transform,
        storage_root=tmp_path / "store",
        source_glob=str(inputs / "*.tsv"),
        naming_pattern=r"mortality_(?P<state>[A-Z]+)_(?P<date>\d{4}-\d{2}-\d{2})\.tsv",
        partition_by=["state", "date"],
        extra={"scratch_dir": str(tmp_path / "scratch"), "delimiter": "\t"},
    )
    return load_config(yaml_path)


def test_schema_fingerprints_are_byte_identical_across_repeated_runs(
    tmp_path: Path,
) -> None:
    """Running the same pipeline twice yields identical fingerprints per shard.

    Determinism gate: the fix must NOT introduce any run-to-run variance
    (e.g. dependence on filesystem mtimes, file enumeration order, or
    DuckDB session state).
    """
    config_a = _build_smoke_config(tmp_path / "run_a", dataset_name="ds_det_a")
    config_b = _build_smoke_config(tmp_path / "run_b", dataset_name="ds_det_b")

    res_a = run_pipeline(config_a, shard_index=None)
    res_b = run_pipeline(config_b, shard_index=None)

    assert res_a.ok_count == 4 and res_b.ok_count == 4
    fp_a = {r.schema_fingerprint for r in res_a.shard_results}
    fp_b = {r.schema_fingerprint for r in res_b.shard_results}
    # Same fingerprints (as a set) — same divergent + same same-schema fp.
    assert fp_a == fp_b, (fp_a, fp_b)
    # Two distinct fingerprints overall: the divergent NY/2019 + the shared one.
    assert len(fp_a) == 2


def test_schema_fingerprints_match_when_shards_run_in_reverse_order(
    tmp_path: Path,
) -> None:
    """Per-shard fingerprints must be invariant to execution order.

    Runs once with ``shard_index=None`` (the reference), then re-runs
    every shard individually in REVERSE ``shard_id`` order and asserts
    the per-partition fingerprint matches the reference for every shard.
    This is the property that the broken pre-fix DESCRIBE-over-root
    behavior could NOT satisfy.
    """
    config_ref = _build_smoke_config(tmp_path / "ref", dataset_name="ds_ref")
    ref = run_pipeline(config_ref, shard_index=None)
    assert ref.ok_count == 4
    ref_by_part = {
        (
            json.loads(p.read_text(encoding="utf-8"))["partition_values"]["state"],
            json.loads(p.read_text(encoding="utf-8"))["partition_values"]["date"],
        ): json.loads(p.read_text(encoding="utf-8"))["schema_fingerprint"]
        for p in sorted(_fragment_dir(config_ref).glob("*.json"))
    }

    config_rev = _build_smoke_config(tmp_path / "rev", dataset_name="ds_rev")
    # Walk shard_index in REVERSE: 3, 2, 1, 0.
    rev_by_part: dict[tuple[str, str], str] = {}
    for idx in range(3, -1, -1):
        result = run_pipeline(config_rev, shard_index=idx)
        assert result.ok_count == 1, result.shard_results
        # The single freshly-written fragment carries this iteration's outcome.
        # Match it via partition_values (independent of fragment filename).
        for frag in _fragment_dir(config_rev).glob("*.json"):
            payload = json.loads(frag.read_text(encoding="utf-8"))
            key = (payload["partition_values"]["state"], payload["partition_values"]["date"])
            rev_by_part[key] = payload["schema_fingerprint"]

    assert set(rev_by_part) == set(ref_by_part)
    for key, ref_fp in ref_by_part.items():
        assert rev_by_part[key] == ref_fp, (key, ref_fp, rev_by_part[key])
