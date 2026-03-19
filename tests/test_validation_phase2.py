"""Phase 2 tests: Error reporting model.

TDD: These tests are written BEFORE implementation.
Tests cover:
  - ValidationResult dataclass
  - Finding format with severity/threshold support
  - Validator.run_all() method
"""
import pytest
import pandas as pd
from fairway.validations.checks import Validator
from fairway.validations.result import ValidationResult


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
