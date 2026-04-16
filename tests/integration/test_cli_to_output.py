"""Integration tests: fairway CLI commands → filesystem output.

Uses Click CliRunner + real DuckDB engine. No mocking of fairway internals.
"""
import pytest
from click.testing import CliRunner

pytest.importorskip("duckdb")


@pytest.mark.local
class TestCLIRunCommand:

    def test_cli_run_creates_curated_parquet(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        config_path = str(simple_project / "config" / "fairway.yaml")
        result = CliRunner().invoke(main, ["run", "--config", config_path, "--skip-summary"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert (simple_project / "data" / "curated" / "people.parquet").exists()

    def test_cli_run_autodiscover_config(self, simple_project, monkeypatch):
        """fairway run without --config auto-discovers config/fairway.yaml."""
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        result = CliRunner().invoke(main, ["run", "--skip-summary"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert (simple_project / "data" / "curated" / "people.parquet").exists()

    def test_cli_run_output_contains_success_message(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        config_path = str(simple_project / "config" / "fairway.yaml")
        result = CliRunner().invoke(main, ["run", "--config", config_path, "--skip-summary"])
        assert "completed successfully" in result.output.lower(), \
            f"Expected success message. Got: {result.output}"

    def test_cli_run_dry_run_no_output_created(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        config_path = str(simple_project / "config" / "fairway.yaml")
        result = CliRunner().invoke(main, ["run", "--config", config_path, "--dry-run"])
        assert result.exit_code == 0
        assert not (simple_project / "data" / "curated" / "people.parquet").exists(), \
            "Dry run should not create output"


@pytest.mark.local
class TestCLIInitCommand:

    def test_cli_init_creates_config_yaml(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from fairway.cli import main
        result = CliRunner().invoke(main, ["init", "myproject", "--engine", "duckdb"])
        assert result.exit_code == 0, f"init failed: {result.output}"
        assert (tmp_path / "myproject" / "config" / "fairway.yaml").exists()

    def test_cli_init_config_is_parseable_yaml(self, tmp_path, monkeypatch):
        """fairway init produces valid YAML with no unresolved $placeholders."""
        monkeypatch.chdir(tmp_path)
        from fairway.cli import main
        import yaml
        CliRunner().invoke(main, ["init", "myproject", "--engine", "duckdb"])
        config_text = (tmp_path / "myproject" / "config" / "fairway.yaml").read_text()
        data = yaml.safe_load(config_text)
        assert isinstance(data, dict)
        assert "dataset_name" in data
        assert "$" not in data.get("dataset_name", ""), \
            f"Unrendered placeholder: {data.get('dataset_name')}"

    def test_cli_init_refuses_to_overwrite_existing_dir(self, tmp_path, monkeypatch):
        """Re-running init on an existing project must not clobber user edits."""
        monkeypatch.chdir(tmp_path)
        from fairway.cli import main
        runner = CliRunner()

        first = runner.invoke(main, ["init", "myproject", "--engine", "duckdb"])
        assert first.exit_code == 0

        config_file = tmp_path / "myproject" / "config" / "fairway.yaml"
        config_file.write_text(config_file.read_text() + "\n# USER EDIT\n")

        second = runner.invoke(main, ["init", "myproject", "--engine", "duckdb"])
        assert second.exit_code != 0, "init should refuse to overwrite existing dir"
        assert "already exists" in second.output.lower()
        assert "USER EDIT" in config_file.read_text(), "User edit was clobbered"

    def test_cli_init_force_overwrites_existing_dir(self, tmp_path, monkeypatch):
        """--force allows re-initialization of an existing directory."""
        monkeypatch.chdir(tmp_path)
        from fairway.cli import main
        runner = CliRunner()

        first = runner.invoke(main, ["init", "myproject", "--engine", "duckdb"])
        assert first.exit_code == 0

        second = runner.invoke(
            main, ["init", "myproject", "--engine", "duckdb", "--force"]
        )
        assert second.exit_code == 0, f"--force init failed: {second.output}"


@pytest.mark.local
class TestCLIManifestCommands:

    def test_manifest_list_after_run(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        result = CliRunner().invoke(main, ["manifest", "list"])
        assert result.exit_code == 0
        assert "people" in result.output

    def test_manifest_query_shows_success(self, simple_project, monkeypatch):
        monkeypatch.chdir(simple_project)
        from fairway.cli import main
        from fairway.pipeline import IngestionPipeline
        config_path = str(simple_project / "config" / "fairway.yaml")
        IngestionPipeline(config_path).run(skip_summary=True)
        result = CliRunner().invoke(main, ["manifest", "query", "--table", "people"])
        assert result.exit_code == 0
        assert "success" in result.output.lower()
