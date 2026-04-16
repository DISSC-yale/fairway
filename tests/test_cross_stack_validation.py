"""Cross-stack tests for validation and summarization.

Replaces test_scalability_refactor.py, which used MagicMock for everything
and verified no real behaviour. These tests run the full pipeline on real
data and assert on physical outputs.

RULE-111: runnable via `pytest tests/`
RULE-113: real engine, no mocks
RULE-114: tmp_path for cleanup
"""
import pytest
import pandas as pd
from pathlib import Path

from tests.helpers import build_config, read_curated


def _write_csv(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


class TestValidationCrossStack:

    @pytest.mark.local
    def test_level1_min_rows_passes_for_sufficient_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "data.csv"
        _write_csv(csv_path, "id,name\n1,alice\n2,bob\n3,charlie\n")
        config_path = build_config(tmp_path, table={
            "name": "people", "path": str(csv_path), "format": "csv",
        }, validations={"level1": {"min_rows": 2}})

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)

        df = read_curated(tmp_path, "people")
        assert len(df) == 3

    @pytest.mark.local
    def test_pipeline_produces_numeric_types_from_csv(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "typed.csv"
        _write_csv(csv_path, "user_id,score,label\n1,9.5,excellent\n2,7.2,good\n")
        config_path = build_config(tmp_path, table={
            "name": "typed", "path": str(csv_path), "format": "csv",
        })

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)

        df = read_curated(tmp_path, "typed")
        assert len(df) == 2
        assert pd.api.types.is_numeric_dtype(df["score"]), \
            f"score should be numeric, got {df['score'].dtype}"


class TestSummarizerCrossStack:

    @pytest.mark.local
    def test_run_produces_summary_csv_and_report_md(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "survey.csv"
        _write_csv(csv_path, "respondent_id,age\n1,25\n2,34\n3,45\n")
        config_path = build_config(tmp_path, table={
            "name": "survey", "path": str(csv_path), "format": "csv",
        })

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run()  # summary ON by default

        curated = Path(tmp_path) / "data" / "curated"
        assert (curated / "survey_summary.csv").exists(), "Summary CSV not produced"
        assert (curated / "survey_report.md").exists(), "Markdown report not produced"
        summary_df = pd.read_csv(curated / "survey_summary.csv")
        assert "variable" in summary_df.columns

    @pytest.mark.local
    def test_skip_summary_produces_no_report_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_path = tmp_path / "raw" / "events.csv"
        _write_csv(csv_path, "event_id,type\n1,click\n2,view\n")
        config_path = build_config(tmp_path, table={
            "name": "events", "path": str(csv_path), "format": "csv",
        })

        from fairway.pipeline import IngestionPipeline
        IngestionPipeline(config_path).run(skip_summary=True)

        curated = Path(tmp_path) / "data" / "curated"
        assert not (curated / "events_summary.csv").exists()
        assert not (curated / "events_report.md").exists()
        assert list(curated.rglob("*.parquet")), "Curated parquet missing"
