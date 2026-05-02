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
        # Step 9.4 message: "Submitting <N> shards (skipped <M> ...). Submit array <id>."
        assert "Submit array 9988776" in result.output
        assert "Submitting 2 shards" in result.output
        assert "skipped 0 existing-ok shards" in result.output

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


# ---------------------------------------------------------------------------
# submit pre-scan validation (Step 9.3 — naming_pattern + --allow-skip)
# ---------------------------------------------------------------------------

@pytest.fixture
def dirty_submit_env(submit_env: dict[str, Path]) -> dict[str, Path]:
    """Augment ``submit_env`` with two files that don't match naming_pattern."""
    inputs = submit_env["tmp"] / "inputs"
    (inputs / "readme.csv").write_text("notes\n", encoding="utf-8")
    (inputs / "lower_case.csv").write_text("notes\n", encoding="utf-8")
    return submit_env


class TestSubmitPreScan:
    def test_help_shows_allow_skip_flag(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["submit", "--help"])
        assert result.exit_code == 0, result.output
        assert "--allow-skip" in result.output

    def test_rejects_unmatched_files_without_allow_skip(
        self,
        dirty_submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # If sbatch were invoked on a rejection the test must fail loudly.
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not run on pre-scan rejection")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)
        monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/bin/sbatch")

        result = runner.invoke(main, ["submit", str(dirty_submit_env["yaml"])])
        assert result.exit_code == 2
        msg = (result.stderr if result.stderr_bytes else "") + result.output
        assert "did not match naming_pattern" in msg
        assert "readme.csv" in msg
        assert "lower_case.csv" in msg

        # No sbatch script and no submission record were written.
        sbatch_dir = dirty_submit_env["tmp"] / "build" / "sbatch"
        assert not sbatch_dir.exists() or not list(sbatch_dir.glob("*"))

    def test_allow_skip_writes_skipped_log_and_proceeds(
        self,
        dirty_submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from types import SimpleNamespace

        proc = SimpleNamespace(stdout="Submitted batch job 4242\n", returncode=0)
        monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/bin/sbatch")
        monkeypatch.setattr(
            "fairway.cli.subprocess.run",
            lambda *a, **k: proc,
        )

        result = runner.invoke(
            main, ["submit", str(dirty_submit_env["yaml"]), "--allow-skip"]
        )
        assert result.exit_code == 0, result.output
        assert "Skipping 2 file(s)" in result.output
        assert "readme.csv" in result.output and "lower_case.csv" in result.output
        assert "Submit array 4242" in result.output

        skipped_dir = (dirty_submit_env["tmp"] / "store" / "raw" / "submit_demo"
                       / "manifest" / "_skipped")
        logs = list(skipped_dir.glob("*.json"))
        assert len(logs) == 1, logs
        payload = json.loads(logs[0].read_text())
        names = {Path(entry["file"]).name for entry in payload}
        assert names == {"readme.csv", "lower_case.csv"}
        for entry in payload:
            assert entry["reason"] == "naming_pattern mismatch"

        # Matching shards still flow through.
        shards = json.loads(
            (dirty_submit_env["tmp"] / "build" / "sbatch"
             / "submit_demo.shards.json").read_text()
        )
        assert {s["partition_values"]["state"] for s in shards["shards"]} == {"CT", "MA"}

    def test_dry_run_with_allow_skip_prints_but_writes_no_log(
        self,
        dirty_submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not run on --dry-run")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)

        result = runner.invoke(
            main,
            ["submit", str(dirty_submit_env["yaml"]),
             "--dry-run", "--allow-skip"],
        )
        assert result.exit_code == 0, result.output
        assert "Would skip 2 file(s)" in result.output
        assert "readme.csv" in result.output and "lower_case.csv" in result.output
        assert "Would submit:" in result.output

        skipped_dir = (dirty_submit_env["tmp"] / "store" / "raw" / "submit_demo"
                       / "manifest" / "_skipped")
        assert not skipped_dir.exists() or not list(skipped_dir.glob("*.json")), (
            "dry-run must not write a _skipped log"
        )


# ---------------------------------------------------------------------------
# submit idempotent resume + --force (Step 9.4)
# ---------------------------------------------------------------------------


def _layer_root_from_env(env: dict[str, Path]) -> Path:
    return env["tmp"] / "store" / "raw" / "submit_demo"


def _ok_fragment_for(env: dict[str, Path], state: str, year: str) -> dict:
    fpath = env["tmp"] / "inputs" / f"{state}_{year}.csv"
    return manifest_mod.build_fragment(
        partition_values={"state": state, "year": year},
        source_files=[str(fpath)],
        source_hashes=[manifest_mod.source_hash(fpath)],
        schema=[{"name": "id", "dtype": "BIGINT"}],
        row_count=2,
        status="ok",
        error=None,
        encoding_used="utf-8",
    )


def _err_fragment_for(env: dict[str, Path], state: str, year: str) -> dict:
    fpath = env["tmp"] / "inputs" / f"{state}_{year}.csv"
    return manifest_mod.build_fragment(
        partition_values={"state": state, "year": year},
        source_files=[str(fpath)],
        source_hashes=[manifest_mod.source_hash(fpath)],
        schema=[],
        row_count=0,
        status="error",
        error="boom",
        encoding_used="utf-8",
    )


@pytest.fixture
def stub_sbatch(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    from types import SimpleNamespace
    proc = SimpleNamespace(stdout="Submitted batch job 7777\n", returncode=0)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_kw: object) -> SimpleNamespace:
        calls.append(list(cmd))
        return proc

    monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/bin/sbatch")
    monkeypatch.setattr("fairway.cli.subprocess.run", fake_run)
    return calls


class TestSubmitIdempotent:
    def test_clean_dataset_submits_all_matched_shards(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert "Submitting 2 shards" in result.output
        assert "skipped 0 existing-ok shards" in result.output
        shards = json.loads(
            (submit_env["tmp"] / "build" / "sbatch"
             / "submit_demo.shards.json").read_text()
        )
        assert len(shards["shards"]) == 2

    def test_partial_dataset_skips_ok_shards(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        layer_root = _layer_root_from_env(submit_env)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "CT", "2023"), root_dir=layer_root)
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert "Submitting 1 shards" in result.output
        assert "skipped 1 existing-ok shards" in result.output
        shards = json.loads(
            (submit_env["tmp"] / "build" / "sbatch"
             / "submit_demo.shards.json").read_text()
        )
        states = {s["partition_values"]["state"] for s in shards["shards"]}
        assert states == {"MA"}, "CT was already ok and must be skipped"

    def test_source_hash_mismatch_resubmits_shard(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        layer_root = _layer_root_from_env(submit_env)
        ct_frag = _ok_fragment_for(submit_env, "CT", "2023")
        ct_frag["source_hashes"] = ["deadbeefdeadbeef"]  # forced mismatch
        manifest_mod.write_fragment(ct_frag, root_dir=layer_root)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "MA", "2023"), root_dir=layer_root)
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        # Only CT (hash drift) re-submits; MA is skipped.
        assert "Submitting 1 shards" in result.output
        assert "skipped 1 existing-ok shards" in result.output
        shards = json.loads(
            (submit_env["tmp"] / "build" / "sbatch"
             / "submit_demo.shards.json").read_text()
        )
        states = {s["partition_values"]["state"] for s in shards["shards"]}
        assert states == {"CT"}, "hash-drifted CT must be re-submitted"

    def test_error_status_fragment_resubmits_shard(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        layer_root = _layer_root_from_env(submit_env)
        manifest_mod.write_fragment(
            _err_fragment_for(submit_env, "CT", "2023"), root_dir=layer_root)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "MA", "2023"), root_dir=layer_root)
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        # status="error" is not "done" — must re-submit; MA still skipped.
        assert "Submitting 1 shards" in result.output
        assert "skipped 1 existing-ok shards" in result.output
        shards = json.loads(
            (submit_env["tmp"] / "build" / "sbatch"
             / "submit_demo.shards.json").read_text()
        )
        states = {s["partition_values"]["state"] for s in shards["shards"]}
        assert states == {"CT"}

    def test_force_flag_resubmits_every_matched_shard(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        layer_root = _layer_root_from_env(submit_env)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "CT", "2023"), root_dir=layer_root)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "MA", "2023"), root_dir=layer_root)
        result = runner.invoke(
            main, ["submit", str(submit_env["yaml"]), "--force"])
        assert result.exit_code == 0, result.output
        assert "Submitting 2 shards" in result.output
        assert "skipped 0 existing-ok shards" in result.output

    def test_all_ok_prints_nothing_to_submit_and_exits_zero(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not be invoked when all shards "
                                 "are already ok")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)
        layer_root = _layer_root_from_env(submit_env)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "CT", "2023"), root_dir=layer_root)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "MA", "2023"), root_dir=layer_root)
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert "nothing to submit" in result.output.lower()
        assert "--force" in result.output
        sbatch_dir = submit_env["tmp"] / "build" / "sbatch"
        assert not sbatch_dir.exists() or not list(sbatch_dir.glob("*.sh"))
        sub_dir = layer_root / "manifest" / "_submissions"
        assert not sub_dir.exists() or not list(sub_dir.glob("*.json"))

    def test_dry_run_prints_would_submit_and_would_skip_counts(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not be invoked on --dry-run")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)
        layer_root = _layer_root_from_env(submit_env)
        manifest_mod.write_fragment(
            _ok_fragment_for(submit_env, "CT", "2023"), root_dir=layer_root)
        result = runner.invoke(
            main, ["submit", str(submit_env["yaml"]), "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Would submit: 1 shards" in result.output
        assert "Would skip (existing ok): 1 shards" in result.output
        sub_dir = layer_root / "manifest" / "_submissions"
        assert not sub_dir.exists() or not list(sub_dir.glob("*.json"))
        skipped_dir = layer_root / "manifest" / "_skipped"
        assert not skipped_dir.exists() or not list(skipped_dir.glob("*.json"))


# ---------------------------------------------------------------------------
# submit chunking for large arrays (Step 9.5)
# ---------------------------------------------------------------------------


@pytest.fixture
def big_submit_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Path]:
    """5-input variant of ``submit_env`` so chunk_size=2 produces 3 chunks."""
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for state in ("CT", "MA", "NY", "NJ", "PA"):
        (inputs / f"{state}_2023.csv").write_text(
            "id,v\n1,a\n2,b\n", encoding="utf-8")
    transform_py = tmp_path / "ds.py"
    transform_py.write_text(
        "from fairway.defaults import default_ingest\n"
        "def transform(con, ctx):\n"
        "    return default_ingest(con, ctx)\n",
        encoding="utf-8",
    )
    yaml_path = tmp_path / "ds.yaml"
    yaml_path.write_text(yaml.safe_dump({
        "dataset_name": "chunky",
        "python": str(transform_py),
        "storage_root": str(tmp_path / "store"),
        "source_glob": str(inputs / "*.csv"),
        "naming_pattern": r"(?P<state>[A-Z]+)_(?P<year>\d{4})\.csv",
        "partition_by": ["state", "year"],
        "delimiter": ",",
        "slurm_chunk_size": 2,  # config-default chunk size for this test
        "slurm_concurrency": 4,
    }), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return {"yaml": yaml_path, "tmp": tmp_path}


@pytest.fixture
def stub_sbatch_seq(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """Stub sbatch with a sequence of distinct array_job_ids per call."""
    from types import SimpleNamespace
    job_ids = iter(["1001", "1002", "1003", "1004", "1005"])
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **_kw: object) -> SimpleNamespace:
        calls.append(list(cmd))
        return SimpleNamespace(
            stdout=f"Submitted batch job {next(job_ids)}\n", returncode=0)

    monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/bin/sbatch")
    monkeypatch.setattr("fairway.cli.subprocess.run", fake_run)
    return {"calls": calls}


class TestSubmitChunking:
    def test_help_shows_chunk_size_flag(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["submit", "--help"])
        assert result.exit_code == 0, result.output
        assert "--chunk-size" in result.output

    def test_small_input_emits_single_chunk(
        self,
        submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch: list[list[str]],
    ) -> None:
        """Regression: 2 shards <= default 4000 chunk_size → existing single
        un-suffixed shards.json + one Submit-array message + one record."""
        result = runner.invoke(main, ["submit", str(submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert "Submit array 7777" in result.output
        sbatch_dir = submit_env["tmp"] / "build" / "sbatch"
        assert (sbatch_dir / "submit_demo.shards.json").is_file()
        assert (sbatch_dir / "submit_demo.sh").is_file()
        # Chunked filenames must NOT be created for the single-chunk path.
        assert not list(sbatch_dir.glob("*-chunk-*"))
        sub_dir = (submit_env["tmp"] / "store" / "raw" / "submit_demo"
                   / "manifest" / "_submissions")
        assert len(list(sub_dir.glob("*.json"))) == 1
        # Only one sbatch invocation happened.
        assert len(stub_sbatch) == 1

    def test_chunk_size_flag_overrides_config(
        self,
        big_submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch_seq: dict[str, list],
    ) -> None:
        """--chunk-size 5 forces a single chunk even though config says 2."""
        result = runner.invoke(
            main, ["submit", str(big_submit_env["yaml"]), "--chunk-size", "5"])
        assert result.exit_code == 0, result.output
        assert "Submit array 1001" in result.output
        sbatch_dir = big_submit_env["tmp"] / "build" / "sbatch"
        assert (sbatch_dir / "chunky.shards.json").is_file()
        assert not list(sbatch_dir.glob("*-chunk-*"))
        assert len(stub_sbatch_seq["calls"]) == 1

    def test_large_input_emits_n_chunks_with_separate_artifacts(
        self,
        big_submit_env: dict[str, Path],
        runner: CliRunner,
        stub_sbatch_seq: dict[str, list],
    ) -> None:
        """5 shards with config slurm_chunk_size=2 → ceil(5/2) = 3 chunks."""
        result = runner.invoke(main, ["submit", str(big_submit_env["yaml"])])
        assert result.exit_code == 0, result.output
        assert "Submitting 3 chunks" in result.output
        assert "total 5 shards" in result.output
        assert "Job IDs: 1001, 1002, 1003" in result.output
        sbatch_dir = big_submit_env["tmp"] / "build" / "sbatch"
        for i in range(3):
            assert (sbatch_dir / f"chunky-chunk-{i}.shards.json").is_file()
            assert (sbatch_dir / f"chunky-chunk-{i}.sh").is_file()
        # Un-suffixed names must NOT be created.
        assert not (sbatch_dir / "chunky.shards.json").exists()
        assert not (sbatch_dir / "chunky.sh").exists()
        # Three sbatch invocations, one per chunk.
        assert len(stub_sbatch_seq["calls"]) == 3
        # Three submission records, one per chunk, sharing submitted_at.
        sub_dir = (big_submit_env["tmp"] / "store" / "raw" / "chunky"
                   / "manifest" / "_submissions")
        records = sorted(sub_dir.glob("*.json"))
        assert len(records) == 3
        loaded = [json.loads(r.read_text()) for r in records]
        loaded.sort(key=lambda r: r["chunk_index"])
        assert [r["chunk_index"] for r in loaded] == [0, 1, 2]
        assert [r["array_job_id"] for r in loaded] == ["1001", "1002", "1003"]
        assert [r["n_shards"] for r in loaded] == [2, 2, 1]
        # All chunks share the same submitted_at instant (parallel submission).
        assert len({r["submitted_at"] for r in loaded}) == 1
        # Each chunk's sbatch_script points at its own per-chunk file.
        for i, rec in enumerate(loaded):
            assert rec["sbatch_script"].endswith(f"chunky-chunk-{i}.sh")

    def test_chunked_dry_run_renders_all_scripts_no_sbatch(
        self,
        big_submit_env: dict[str, Path],
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _explode(*_a: object, **_kw: object) -> None:
            raise AssertionError("sbatch must not be invoked on --dry-run")
        monkeypatch.setattr("fairway.cli.subprocess.run", _explode)
        result = runner.invoke(
            main, ["submit", str(big_submit_env["yaml"]), "--dry-run"])
        assert result.exit_code == 0, result.output
        assert result.output.count("Would submit: sbatch") == 3
        sbatch_dir = big_submit_env["tmp"] / "build" / "sbatch"
        for i in range(3):
            assert (sbatch_dir / f"chunky-chunk-{i}.shards.json").is_file()


# ---------------------------------------------------------------------------
# status aggregates across chunks of the most recent submission (Step 9.5)
# ---------------------------------------------------------------------------


class TestStatusAggregation:
    def test_status_aggregates_squeue_across_chunks(
        self,
        tmp_path: Path,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two chunked submission records (same submitted_at) → status calls
        squeue once per array_job_id and aggregates the parsed state counts."""
        from types import SimpleNamespace
        from fairway import _sbatch
        layer_root = tmp_path / "data" / "raw" / "agg_demo"
        ts = "2026-05-02T12-00-00"
        # Two chunked records sharing submitted_at; -chunk-<i>.json filenames.
        _sbatch.write_submission_record(
            layer_root, sbatch_script=Path("/x/agg_demo-chunk-0.sh"),
            array_job_id="2001", n_shards=2, ts=ts,
            chunk_index=0, total_chunks=2)
        _sbatch.write_submission_record(
            layer_root, sbatch_script=Path("/x/agg_demo-chunk-1.sh"),
            array_job_id="2002", n_shards=3, ts=ts,
            chunk_index=1, total_chunks=2)
        squeue_outputs = {
            "2001": "RUNNING\nPENDING\n",          # 2 tasks
            "2002": "RUNNING\nRUNNING\nPENDING\n", # 3 tasks
        }
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> SimpleNamespace:
            calls.append(list(cmd))
            jid = cmd[cmd.index("-j") + 1]
            return SimpleNamespace(stdout=squeue_outputs[jid], returncode=0)

        monkeypatch.setattr("fairway.cli.shutil.which", lambda _: "/usr/bin/squeue")
        monkeypatch.setattr("fairway.cli.subprocess.run", fake_run)

        result = runner.invoke(main, [
            "status", "--dataset", "agg_demo",
            "--storage-root", str(tmp_path / "data"), "--layer", "raw"])
        assert result.exit_code == 0, result.output
        # Header line shows chunk count + aggregated total + both job ids.
        assert "2 chunks" in result.output
        assert "total 5 shards" in result.output
        assert "2001" in result.output and "2002" in result.output
        # squeue invoked once per chunk's array_job_id.
        invoked_jids = [c[c.index("-j") + 1] for c in calls
                        if c and c[0] == "squeue"]
        assert invoked_jids == ["2001", "2002"]
        # Aggregated state counts across both chunks: PENDING=2, RUNNING=3.
        assert "Slurm states (aggregated):" in result.output
        assert "PENDING=2" in result.output
        assert "RUNNING=3" in result.output
