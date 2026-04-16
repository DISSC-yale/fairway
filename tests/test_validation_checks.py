"""Tests for the validation error reporting model and pipeline integration.

Covers (merged from phase2 and phase3):
  - ValidationResult dataclass (findings, severity, threshold support)
  - Validator.run_all() unified entry point
  - Per-table validation overrides (shallow merge with global)
  - output_layer config validation
  - Pipeline uses Validator.run_all() instead of legacy level1/level2
"""
import pytest
import yaml
import pandas as pd
from fairway.validations.checks import Validator
from fairway.validations.result import ValidationResult
from fairway.config_loader import Config, ConfigValidationError


# ---------------------------------------------------------------------------
# Absorbed from test_validation_phase2.py: Error reporting model
# ---------------------------------------------------------------------------

# ============ ValidationResult dataclass ============

class TestValidationResult:
    """Tests for the ValidationResult dataclass."""

    def test_default_is_passing(self):
        """A fresh ValidationResult with no findings should pass."""
        result = ValidationResult()
        assert result.passed is True
        assert result.errors == []
        assert result.warnings == []

    def test_add_error_finding(self):
        """Adding an error finding marks result as failed."""
        result = ValidationResult()
        result.add_finding({
            "column": "id",
            "check": "check_nulls",
            "message": "Null values found",
            "severity": "error",
            "failed_count": 5,
            "total_count": 100,
        })
        assert result.passed is False
        assert len(result.errors) == 1
        assert result.errors[0]["column"] == "id"

    def test_add_warning_finding(self):
        """Adding a warn finding does NOT mark result as failed."""
        result = ValidationResult()
        result.add_finding({
            "column": "age",
            "check": "check_range",
            "message": "2 rows out of range",
            "severity": "warn",
            "failed_count": 2,
            "total_count": 100,
        })
        assert result.passed is True
        assert len(result.warnings) == 1
        assert len(result.errors) == 0

    def test_default_severity_is_error(self):
        """A finding with no severity defaults to error."""
        result = ValidationResult()
        result.add_finding({
            "column": "id",
            "check": "check_nulls",
            "message": "Null values found",
            "failed_count": 1,
            "total_count": 10,
        })
        assert result.passed is False
        assert len(result.errors) == 1

    def test_threshold_below_passes(self):
        """If violation rate is below threshold, finding becomes a warning."""
        result = ValidationResult()
        result.add_finding({
            "column": "year",
            "check": "check_range",
            "message": "1 row out of range",
            "severity": "error",
            "failed_count": 1,
            "total_count": 100,
            "threshold": 0.05,  # 5% threshold
        })
        # 1/100 = 1% < 5% threshold → downgraded to warning
        assert result.passed is True
        assert len(result.warnings) == 1
        assert len(result.errors) == 0

    def test_threshold_above_fails(self):
        """If violation rate exceeds threshold, finding remains error."""
        result = ValidationResult()
        result.add_finding({
            "column": "year",
            "check": "check_range",
            "message": "10 rows out of range",
            "severity": "error",
            "failed_count": 10,
            "total_count": 100,
            "threshold": 0.05,  # 5% threshold
        })
        # 10/100 = 10% > 5% → stays error
        assert result.passed is False
        assert len(result.errors) == 1

    def test_mixed_errors_and_warnings(self):
        """Result with both errors and warnings: passed=False, both lists populated."""
        result = ValidationResult()
        result.add_finding({
            "column": "id",
            "check": "check_nulls",
            "message": "Nulls found",
            "severity": "error",
            "failed_count": 5,
            "total_count": 100,
        })
        result.add_finding({
            "column": "age",
            "check": "check_range",
            "message": "Out of range",
            "severity": "warn",
            "failed_count": 1,
            "total_count": 100,
        })
        assert result.passed is False
        assert len(result.errors) == 1
        assert len(result.warnings) == 1

    def test_result_includes_counts(self):
        """Findings include failed_count and total_count."""
        result = ValidationResult()
        result.add_finding({
            "column": "id",
            "check": "check_nulls",
            "message": "5 null values",
            "severity": "error",
            "failed_count": 5,
            "total_count": 100,
        })
        finding = result.errors[0]
        assert finding["failed_count"] == 5
        assert finding["total_count"] == 100


# ============ Validator.run_all() ============

class TestRunAll:
    """Tests for Validator.run_all() unified entry point."""

    def test_run_all_clean_data(self):
        """Clean data with valid config passes all checks."""
        df = pd.DataFrame({
            "person_id": [1, 2, 3],
            "year": [2020, 2021, 2022],
        })
        config = {"min_rows": 2, "check_nulls": ["person_id"]}
        result = Validator.run_all(df, config)
        assert isinstance(result, ValidationResult)
        assert result.passed is True

    def test_run_all_min_rows_fail(self):
        """run_all catches min_rows violation."""
        df = pd.DataFrame({"id": [1]})
        config = {"min_rows": 5}
        result = Validator.run_all(df, config)
        assert result.passed is False
        assert any("min_rows" in e["check"] for e in result.errors)

    def test_run_all_null_check_fail(self):
        """run_all catches null violations."""
        df = pd.DataFrame({"id": [1, None, 3]})
        config = {"check_nulls": ["id"]}
        result = Validator.run_all(df, config)
        assert result.passed is False
        assert any("check_nulls" in e["check"] for e in result.errors)

    def test_run_all_empty_config(self):
        """run_all with no checks passes."""
        df = pd.DataFrame({"id": [1, 2]})
        result = Validator.run_all(df, {})
        assert result.passed is True

    def test_run_all_legacy_config(self):
        """run_all works with legacy level1/level2 config."""
        df = pd.DataFrame({"id": [1]})
        config = {
            "level1": {"min_rows": 5},
            "level2": {"check_nulls": ["id"]},
        }
        result = Validator.run_all(df, config)
        assert result.passed is False
        assert any("min_rows" in e["check"] for e in result.errors)

    def test_run_all_returns_validation_result(self):
        """run_all always returns a ValidationResult, not a plain dict."""
        df = pd.DataFrame({"id": [1]})
        result = Validator.run_all(df, {})
        assert isinstance(result, ValidationResult)

    def test_run_all_multiple_failures(self):
        """run_all collects errors from multiple checks."""
        df = pd.DataFrame({"id": [None]})  # 1 row, has null
        config = {"min_rows": 5, "check_nulls": ["id"]}
        result = Validator.run_all(df, config)
        assert result.passed is False
        assert len(result.errors) == 2  # min_rows + null check


# ---------------------------------------------------------------------------
# Absorbed from test_validation_phase3.py: Per-table config, output_layer,
# pipeline run_all integration
# ---------------------------------------------------------------------------

# ============ Helper ============

def _write_config(tmp_path, tables, global_validations=None, extra=None):
    """Write a YAML config and create dummy files so path validation passes."""
    config_path = str(tmp_path / "config.yaml")
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    (data_dir / "test.csv").write_text("a,b\n1,2")

    config = {
        "dataset_name": "test",
        "engine": "duckdb",
        "tables": tables,
    }
    if global_validations is not None:
        config["validations"] = global_validations
    if extra:
        config.update(extra)
    with open(config_path, "w") as f:
        yaml.dump(config, f)
    return config_path


# ============ Per-table validations ============

class TestPerTableValidations:
    """Per-table validations override global with shallow merge."""

    def test_per_table_overrides_global(self, tmp_path):
        """Per-table min_rows overrides global min_rows."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "validations": {"min_rows": 500},
            }],
            global_validations={"min_rows": 10},
        )
        config = Config(config_path)
        table_validations = config.tables[0].get("validations", {})
        assert table_validations["min_rows"] == 500

    def test_per_table_list_replacement(self, tmp_path):
        """Per-table check_nulls replaces (not merges with) global list."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "validations": {"check_nulls": ["tx_id"]},
            }],
            global_validations={"check_nulls": ["id"]},
        )
        config = Config(config_path)
        table_validations = config.tables[0].get("validations", {})
        assert table_validations["check_nulls"] == ["tx_id"]

    def test_per_table_inherits_global(self, tmp_path):
        """Table without validations inherits global."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
            }],
            global_validations={"min_rows": 10, "check_nulls": ["id"]},
        )
        config = Config(config_path)
        table_validations = config.tables[0].get("validations", {})
        assert table_validations["min_rows"] == 10
        assert table_validations["check_nulls"] == ["id"]

    def test_no_global_no_table_validations(self, tmp_path):
        """No validations at any level produces empty dict."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
            }],
        )
        config = Config(config_path)
        table_validations = config.tables[0].get("validations", {})
        assert table_validations == {}

    def test_shallow_merge_replaces_entire_key(self, tmp_path):
        """Shallow merge: per-table check_range replaces ALL global check_range."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "validations": {"check_range": {"age": {"min": 0, "max": 120}}},
            }],
            global_validations={
                "check_range": {"income": {"min": 0}, "age": {"min": 18}},
                "min_rows": 1,
            },
        )
        config = Config(config_path)
        v = config.tables[0]["validations"]
        # Shallow merge: entire check_range replaced, global min_rows inherited
        assert "income" not in v.get("check_range", {})
        assert v["check_range"]["age"] == {"min": 0, "max": 120}
        assert v["min_rows"] == 1

    def test_per_table_partial_override(self, tmp_path):
        """Per-table adds a key not in global; global keys not overridden are kept."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "validations": {"max_rows": 1000},
            }],
            global_validations={"min_rows": 10},
        )
        config = Config(config_path)
        table_validations = config.tables[0].get("validations", {})
        assert table_validations["min_rows"] == 10
        assert table_validations["max_rows"] == 1000


# ============ output_layer config ============

class TestOutputLayerConfig:
    """output_layer per-table config validation."""

    def test_default_output_layer_is_curated(self, tmp_path):
        """Table without output_layer defaults to curated."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
            }],
        )
        config = Config(config_path)
        assert config.tables[0].get("output_layer") == "curated"

    def test_output_layer_processed(self, tmp_path):
        """output_layer: processed is accepted."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "output_layer": "processed",
            }],
        )
        config = Config(config_path)
        assert config.tables[0]["output_layer"] == "processed"

    def test_invalid_output_layer_rejected(self, tmp_path):
        """Invalid output_layer raises ConfigValidationError."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "output_layer": "gold",
            }],
        )
        with pytest.raises(ConfigValidationError, match="output_layer"):
            Config(config_path)

    def test_output_layer_processed_with_transformation_rejected(self, tmp_path):
        """output_layer: processed + transformation is a config error."""
        config_path = _write_config(
            tmp_path,
            tables=[{
                "name": "demographics",
                "path": str(tmp_path / "data" / "test.csv"),
                "format": "csv",
                "output_layer": "processed",
                "transformation": "transforms/demo.py",
            }],
        )
        with pytest.raises(ConfigValidationError, match="output_layer.*processed.*transformation"):
            Config(config_path)


# ============ Pipeline run_all integration ============

class TestPipelineRunAllIntegration:
    """Pipeline should use Validator.run_all() instead of legacy checks."""

    def test_run_all_with_per_table_config(self):
        """run_all works with merged per-table validation config."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        table_validations = {"min_rows": 2, "check_nulls": ["id"]}
        result = Validator.run_all(df, table_validations)
        assert result.passed is True

    def test_run_all_with_per_table_override(self):
        """Per-table config can set stricter thresholds."""
        df = pd.DataFrame({"id": [1]})  # Only 1 row
        table_validations = {"min_rows": 100}  # Per-table override
        result = Validator.run_all(df, table_validations)
        assert result.passed is False
