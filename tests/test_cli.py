"""Tests for :mod:`fairway.cli` (rewrite/v0.3 Step 9.1).

Pins the bounded subcommand surface defined in PLAN.md Step 9.1:
``init``, ``enrich``, ``validate`` and ``manifest finalize`` are the
real-behaviour commands wired in this step. ``run``, ``status``,
``transform``, ``summarize`` and ``submit`` are exercised as
help-resolvable but their bodies land in later substeps.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from fairway.cli import main
from fairway import manifest as manifest_mod


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

class TestInit:
    def test_creates_expected_tree(self, tmp_path: Path, runner: CliRunner) -> None:
        proj = tmp_path / "proj"
        result = runner.invoke(main, ["init", str(proj)])
        assert result.exit_code == 0, result.output
        for sub in (
            "datasets",
            "transforms/raw_to_processed",
            "transforms/processed_to_curated",
            "data",
            "build",
        ):
            assert (proj / sub).is_dir(), sub
        assert (proj / ".gitignore").read_text().count("data/") == 1
        assert "build/" in (proj / ".gitignore").read_text()
        assert (proj / "README.md").is_file()
        assert "proj" in (proj / "README.md").read_text()

    def test_dataset_scaffold_yaml_and_py(
        self, tmp_path: Path, runner: CliRunner
    ) -> None:
        proj = tmp_path / "proj"
        result = runner.invoke(
            main, ["init", str(proj), "--dataset", "mydata"]
        )
        assert result.exit_code == 0, result.output
        ds_yaml = proj / "datasets" / "mydata.yaml"
        ds_py = proj / "datasets" / "mydata.py"
        assert ds_yaml.is_file()
        assert ds_py.is_file()

        # YAML must be parseable and contain Required Config keys.
        parsed = yaml.safe_load(ds_yaml.read_text())
        assert parsed["dataset_name"] == "mydata"
        for key in ("python", "storage_root", "source_glob",
                    "naming_pattern", "partition_by"):
            assert key in parsed, key

        # .py must import default_ingest and define a `transform` callable.
        body = ds_py.read_text()
        assert "from fairway.defaults import default_ingest" in body
        assert "def transform" in body

        # Scaffolded LOC per dataset must stay <100 (PLAN.md Step 9.1).
        lines = ds_yaml.read_text().splitlines() + ds_py.read_text().splitlines()
        assert len(lines) < 100, f"{len(lines)} lines is over 100 budget"

    def test_dataset_py_executes_with_real_default_ingest(
        self, tmp_path: Path, runner: CliRunner
    ) -> None:
        """The scaffolded ``transform`` is a thin wrapper around
        ``default_ingest`` — importing the file must work without errors."""
        proj = tmp_path / "proj"
        runner.invoke(main, ["init", str(proj), "--dataset", "mydata"])
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "scaffolded_mydata", proj / "datasets" / "mydata.py"
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert callable(mod.transform)

    def test_refuses_nonempty_dir_without_force(
        self, tmp_path: Path, runner: CliRunner
    ) -> None:
        proj = tmp_path / "proj"
        proj.mkdir()
        (proj / "preexisting.txt").write_text("user data")
        result = runner.invoke(main, ["init", str(proj)])
        assert result.exit_code != 0
        assert "exists" in result.output.lower()
        # Sentinel file untouched.
        assert (proj / "preexisting.txt").read_text() == "user data"

    def test_force_overrides_nonempty_refusal(
        self, tmp_path: Path, runner: CliRunner
    ) -> None:
        proj = tmp_path / "proj"
        proj.mkdir()
        (proj / "preexisting.txt").write_text("user data")
        result = runner.invoke(main, ["init", str(proj), "--force"])
        assert result.exit_code == 0, result.output
        assert (proj / "datasets").is_dir()

    def test_dataset_add_to_existing_project_without_force(
        self, tmp_path: Path, runner: CliRunner
    ) -> None:
        """Once a project is initialized, --dataset adds without --force."""
        proj = tmp_path / "proj"
        first = runner.invoke(main, ["init", str(proj)])
        assert first.exit_code == 0
        second = runner.invoke(main, ["init", str(proj), "--dataset", "ds2"])
        assert second.exit_code == 0, second.output
        assert (proj / "datasets" / "ds2.yaml").is_file()


# ---------------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------------

def test_enrich_exits_2_with_stderr_message(runner: CliRunner) -> None:
    result = runner.invoke(main, ["enrich"])
    assert result.exit_code == 2
    # Click puts ``err=True`` echoes on stderr; the message must mention
    # the deferral so operators don't think the command silently succeeded.
    assert "deferred" in (result.stderr or result.output).lower()


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

@pytest.fixture
def tiny_parquet(tmp_path: Path) -> Path:
    duckdb = pytest.importorskip("duckdb")
    out = tmp_path / "ds"
    out.mkdir()
    con = duckdb.connect(":memory:")
    try:
        con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, value)") \
           .write_parquet(str(out / "data.parquet"), compression="zstd")
    finally:
        con.close()
    return out


def test_validate_passes_when_rules_match(
    tiny_parquet: Path, tmp_path: Path, runner: CliRunner
) -> None:
    rules = tmp_path / "rules.yaml"
    rules.write_text(yaml.safe_dump({"validations": {"min_rows": 1}}))
    result = runner.invoke(
        main, ["validate", str(tiny_parquet), "--rules", str(rules)]
    )
    assert result.exit_code == 0, result.output
    assert "OK" in result.output or "all checks passed" in result.output


def test_validate_fails_with_exit_2_on_violation(
    tiny_parquet: Path, tmp_path: Path, runner: CliRunner
) -> None:
    rules = tmp_path / "rules.yaml"
    rules.write_text(yaml.safe_dump({"validations": {"min_rows": 999}}))
    result = runner.invoke(
        main, ["validate", str(tiny_parquet), "--rules", str(rules)]
    )
    assert result.exit_code == 2
    assert "min_rows" in ((result.stderr or "") + result.output).lower()


# ---------------------------------------------------------------------------
# manifest finalize
# ---------------------------------------------------------------------------

def test_manifest_finalize_merges_fragments(
    tmp_path: Path, runner: CliRunner
) -> None:
    layer_root = tmp_path / "layer"
    (layer_root / "_fragments").mkdir(parents=True)
    frag_ok = manifest_mod.build_fragment(
        partition_values={"state": "CT", "year": "2023"},
        source_files=["/tmp/CT_2023.csv"],
        source_hashes=["abc123"],
        schema=[{"name": "id", "dtype": "BIGINT"}],
        row_count=3,
        status="ok",
        error=None,
        encoding_used="utf-8",
    )
    frag_err = manifest_mod.build_fragment(
        partition_values={"state": "NY", "year": "2023"},
        source_files=["/tmp/NY_2023.csv"],
        source_hashes=[""],
        schema=[],
        row_count=0,
        status="error",
        error="boom",
        encoding_used="utf-8",
    )
    manifest_mod.write_fragment(frag_ok, root_dir=layer_root)
    manifest_mod.write_fragment(frag_err, root_dir=layer_root)

    result = runner.invoke(main, ["manifest", "finalize", str(layer_root)])
    assert result.exit_code == 0, result.output
    assert "fragments merged: 2" in result.output
    assert "ok=1" in result.output and "error=1" in result.output

    merged = json.loads((layer_root / "manifest.json").read_text())
    assert merged["fragment_count"] == 2
    assert (layer_root / "schema_summary.json").is_file()


# ---------------------------------------------------------------------------
# Help-only smoke tests for deferred subcommands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cmd", ["run", "status", "transform", "summarize", "submit"])
def test_deferred_subcommands_have_help(cmd: str, runner: CliRunner) -> None:
    result = runner.invoke(main, [cmd, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage" in result.output
