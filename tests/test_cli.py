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


# ---------------------------------------------------------------------------
# submit (Step 9.2 — sbatch core, dry-run, sbatch-not-found)
# ---------------------------------------------------------------------------

@pytest.fixture
def submit_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Path]:
    """Materialize a project + 2 input CSVs + config.yaml; chdir to ``tmp_path``."""
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "CT_2023.csv").write_text("id,v\n1,a\n2,b\n", encoding="utf-8")
    (inputs / "MA_2023.csv").write_text("id,v\n3,c\n4,d\n", encoding="utf-8")
    transform_py = tmp_path / "ds.py"
    transform_py.write_text(
        "from fairway.defaults import default_ingest\n"
        "def transform(con, ctx):\n"
        "    return default_ingest(con, ctx)\n",
        encoding="utf-8",
    )
    yaml_path = tmp_path / "ds.yaml"
    yaml_path.write_text(yaml.safe_dump({
        "dataset_name": "submit_demo",
        "python": str(transform_py),
        "storage_root": str(tmp_path / "store"),
        "source_glob": str(inputs / "*.csv"),
        "naming_pattern": r"(?P<state>[A-Z]+)_(?P<year>\d{4})\.csv",
        "partition_by": ["state", "year"],
        "delimiter": ",",
        "slurm_account": "myacct",
        "slurm_partition": "day",
        "slurm_concurrency": 4,
    }), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return {"yaml": yaml_path, "tmp": tmp_path}


class TestSubmit:
    def test_dry_run_renders_scripts_without_invoking_sbatch(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # If anything tries to invoke sbatch the test must fail loudly.
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not be invoked on --dry-run")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)

        result = runner.invoke(
            main, ["submit", str(submit_env["yaml"]), "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert "Would submit:" in result.output

        shards_path = (submit_env["tmp"] / "build" / "sbatch"
                       / "submit_demo.shards.json")
        script_path = submit_env["tmp"] / "build" / "sbatch" / "submit_demo.sh"
        assert shards_path.is_file()
        assert script_path.is_file()

        payload = json.loads(shards_path.read_text())
        assert payload["config"] == str(submit_env["yaml"])
        assert len(payload["shards"]) == 2
        ids = [s["shard_id"] for s in payload["shards"]]
        assert ids == sorted(ids), "shards must be sorted by shard_id"
        states = {s["partition_values"]["state"] for s in payload["shards"]}
        assert states == {"CT", "MA"}

        body = script_path.read_text()
        assert "#SBATCH --job-name=fairway-submit_demo" in body
        assert "#SBATCH --account=myacct" in body
        assert "#SBATCH --partition=day" in body
        assert "#SBATCH --array=0-1%4" in body
        assert "#SBATCH --output=logs/fairway-submit_demo-%A_%a.out" in body
        assert "python -m fairway run --shards-file" in body
        assert "$SLURM_ARRAY_TASK_ID" in body

        sub_dir = (submit_env["tmp"] / "store" / "raw" / "submit_demo"
                   / "manifest" / "_submissions")
        assert not sub_dir.exists() or not list(sub_dir.glob("*.json")), (
            "dry-run must not write a submission record"
        )

    def test_refuses_when_sbatch_missing_on_path(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("fairway.cli.shutil.which", lambda _: None)
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 2
        msg = (result.stderr if result.stderr_bytes else "") + result.output
        assert "sbatch not found on PATH" in msg

    def test_writes_submission_record_with_parsed_array_job_id(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from types import SimpleNamespace

        proc = SimpleNamespace(
            stdout="Submitted batch job 9988776\n", returncode=0
        )
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> SimpleNamespace:
            calls.append(list(cmd))
            return proc

        monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/bin/sbatch")
        monkeypatch.setattr("fairway.cli.subprocess.run", fake_run)

        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert calls and calls[0][0] == "sbatch"
        assert "Submitted Slurm array 9988776" in result.output

        sub_dir = (submit_env["tmp"] / "store" / "raw" / "submit_demo"
                   / "manifest" / "_submissions")
        records = list(sub_dir.glob("*.json"))
        assert len(records) == 1, records
        rec = json.loads(records[0].read_text())
        assert rec["array_job_id"] == "9988776"
        assert rec["n_shards"] == 2
        assert rec["chunk_index"] == 0
        assert rec["sbatch_script"].endswith("submit_demo.sh")
        assert rec["submitted_at"]  # non-empty timestamp
