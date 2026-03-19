"""Tests for code review fixes: regex validation, config validation in run_all,
unimplemented check guards, threshold boundary.
"""
import pytest
import pandas as pd
from fairway.validations.checks import Validator
from fairway.validations.result import ValidationResult


class TestRegexValidation:
    """check_pattern regex is validated at config time."""

    def test_invalid_regex_rejected(self):
        errors = Validator._validate_validations_block({
            "check_pattern": {"col": "[invalid("}
        })
        assert len(errors) == 1
        assert "invalid regex" in errors[0]

    def test_valid_regex_accepted(self):
        errors = Validator._validate_validations_block({
            "check_pattern": {"fips": r"^\d{5}$"}
        })
        assert errors == []

    def test_non_string_pattern_rejected(self):
        errors = Validator._validate_validations_block({
            "check_pattern": {"col": 123}
        })
        assert len(errors) == 1
        assert "must be a string" in errors[0]


class TestConfigValidationInRunAll:
    """run_all() validates config schema and fails fast."""

    def test_unknown_key_fails_fast(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = Validator.run_all(df, {"bogus_check": True})
        assert result.passed is False
        assert any("config_validation" in e["check"] for e in result.errors)

    def test_bad_type_fails_fast(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = Validator.run_all(df, {"min_rows": "banana"})
        assert result.passed is False
        assert any("config_validation" in e["check"] for e in result.errors)

    def test_invalid_regex_fails_fast(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = Validator.run_all(df, {"check_pattern": {"id": "[bad("}})
        assert result.passed is False


class TestUnimplementedGuards:
    """check_custom and check_unique raise NotImplementedError."""

    def test_check_custom_raises(self):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(NotImplementedError, match="check_custom"):
            Validator.run_all(df, {"check_custom": "validators/test.py"})

    def test_check_unique_raises(self):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(NotImplementedError, match="check_unique"):
            Validator.run_all(df, {"check_unique": ["id"]})


class TestThresholdBoundary:
    """Threshold edge cases."""

    def test_threshold_exact_boundary_stays_error(self):
        """5/100 = 5% with threshold 0.05 — NOT strictly less, so stays error."""
        result = ValidationResult()
        result.add_finding({
            "column": "x",
            "check": "test",
            "message": "test",
            "severity": "error",
            "failed_count": 5,
            "total_count": 100,
            "threshold": 0.05,
        })
        assert result.passed is False
        assert len(result.errors) == 1

    def test_threshold_zero_means_no_tolerance(self):
        """threshold=0.0 means any violation is an error."""
        result = ValidationResult()
        result.add_finding({
            "column": "x",
            "check": "test",
            "message": "test",
            "severity": "error",
            "failed_count": 1,
            "total_count": 1000,
            "threshold": 0.0,
        })
        assert result.passed is False


class TestFullmatchConsistency:
    """check_pattern uses fullmatch (not match) for cross-engine consistency."""

    def test_pattern_without_anchors_requires_full_match(self):
        """Pattern '\\d{5}' should NOT match 'AB09001' (no substring matching)."""
        df = pd.DataFrame({"code": ["09001", "AB09001"]})
        config = {"check_pattern": {"code": r"\d{5}"}}
        result = Validator.run_all(df, config)
        assert result.passed is False
        assert result.errors[0]["failed_count"] == 1  # AB09001 fails
