"""Tests for structural/column-level validation checks and review fixes.

Covers (merged from phase4 and review_fixes):
  - expected_columns check (with strict mode)
  - max_rows check
  - check_range (with null/empty edge cases)
  - check_values
  - check_pattern (fullmatch semantics)
  - Combined check suites
  - Regex validation at config time
  - Config validation in run_all (fail-fast on unknown keys/bad types)
  - Unimplemented check guards (check_custom, check_unique)
  - Threshold boundary edge cases
"""
import pytest
import pandas as pd
from fairway.validations.checks import Validator
from fairway.validations.result import ValidationResult


# ---------------------------------------------------------------------------
# Absorbed from test_validation_phase4.py: Structural + column-level checks
# ---------------------------------------------------------------------------

# ============ Shared fixtures ============

@pytest.fixture
def clean_df():
    """DataFrame that passes all validations."""
    return pd.DataFrame({
        "person_id": [1, 2, 3, 4, 5],
        "year": [2020, 2021, 2022, 2023, 2024],
        "state": ["CT", "MA", "NY", "CT", "MA"],
        "age": [25, 34, 45, 28, 62],
        "fips_code": ["09001", "25003", "36061", "09009", "25017"],
        "income": [50000.0, 72000.0, 95000.0, 61000.0, 88000.0],
    })


@pytest.fixture
def df_with_nulls():
    return pd.DataFrame({
        "person_id": [1, None, 3, None, 5],
        "year": [2020, 2021, None, 2023, 2024],
        "state": ["CT", "MA", "NY", "CT", "MA"],
    })


@pytest.fixture
def df_out_of_range():
    return pd.DataFrame({
        "person_id": [1, 2, 3],
        "year": [1850, 2020, 2099],
        "age": [-5, 25, 150],
    })


@pytest.fixture
def df_bad_patterns():
    return pd.DataFrame({
        "person_id": [1, 2, 3],
        "fips_code": ["09001", "9001", "ABCDE"],
    })


@pytest.fixture
def df_invalid_values():
    return pd.DataFrame({
        "person_id": [1, 2, 3],
        "state": ["CT", "ZZ", "XX"],
    })


@pytest.fixture
def df_missing_columns():
    return pd.DataFrame({
        "person_id": [1, 2, 3],
    })


@pytest.fixture
def df_extra_columns():
    return pd.DataFrame({
        "person_id": [1, 2, 3],
        "year": [2020, 2021, 2022],
        "state": ["CT", "MA", "NY"],
        "ssn": ["123-45-6789", "987-65-4321", "111-22-3333"],
    })


@pytest.fixture
def df_empty():
    return pd.DataFrame({
        "person_id": pd.Series([], dtype="int64"),
        "year": pd.Series([], dtype="int64"),
    })


@pytest.fixture
def df_all_nulls():
    return pd.DataFrame({
        "person_id": [None, None],
        "year": [None, None],
    })


# ============ expected_columns ============

class TestExpectedColumns:

    def test_expected_columns_pass(self, clean_df):
        config = {"expected_columns": ["person_id", "year"]}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True

    def test_expected_columns_fail(self, clean_df):
        config = {"expected_columns": ["person_id", "year", "missing_col"]}
        result = Validator.run_all(clean_df, config)
        assert result.passed is False
        assert any("missing_col" in e["message"] for e in result.errors)

    def test_expected_columns_strict_mode(self, df_extra_columns):
        config = {"expected_columns": {"columns": ["person_id", "year", "state"], "strict": True}}
        result = Validator.run_all(df_extra_columns, config)
        assert result.passed is False
        assert any("ssn" in e["message"] for e in result.errors)

    def test_expected_columns_strict_pass(self, clean_df):
        """Strict mode passes when columns match exactly."""
        all_cols = list(clean_df.columns)
        config = {"expected_columns": {"columns": all_cols, "strict": True}}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True


# ============ max_rows ============

class TestMaxRows:

    def test_max_rows_pass(self, clean_df):
        config = {"max_rows": 10}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True

    def test_max_rows_fail(self, clean_df):
        config = {"max_rows": 3}
        result = Validator.run_all(clean_df, config)
        assert result.passed is False
        assert any("max_rows" in e["check"] for e in result.errors)


# ============ check_range ============

class TestCheckRange:

    def test_check_range_pass(self, clean_df):
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True

    def test_check_range_fail(self, df_out_of_range):
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(df_out_of_range, config)
        assert result.passed is False
        assert any(e["failed_count"] == 2 for e in result.errors)

    def test_check_range_nulls_excluded(self, df_with_nulls):
        """Nulls should not count as range violations."""
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(df_with_nulls, config)
        assert result.passed is True

    def test_check_range_all_nulls(self, df_all_nulls):
        """All-null column should pass range check (no rows to violate)."""
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(df_all_nulls, config)
        assert result.passed is True

    def test_check_range_empty_df(self, df_empty):
        """Empty df passes range check."""
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(df_empty, config)
        assert result.passed is True

    def test_check_range_missing_column(self, df_missing_columns):
        """Range check on missing column produces warning, not crash."""
        config = {"check_range": {"year": {"min": 1900, "max": 2030}}}
        result = Validator.run_all(df_missing_columns, config)
        assert result.passed is True  # doesn't crash, doesn't fail
        assert len(result.warnings) == 1


# ============ check_values ============

class TestCheckValues:

    def test_check_values_pass(self, clean_df):
        config = {"check_values": {"state": ["CT", "MA", "NY"]}}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True

    def test_check_values_fail(self, df_invalid_values):
        config = {"check_values": {"state": ["CT", "MA", "NY"]}}
        result = Validator.run_all(df_invalid_values, config)
        assert result.passed is False
        assert any(e["failed_count"] == 2 for e in result.errors)


# ============ check_pattern ============

class TestCheckPattern:

    def test_check_pattern_pass(self, clean_df):
        config = {"check_pattern": {"fips_code": r"^\d{5}$"}}
        result = Validator.run_all(clean_df, config)
        assert result.passed is True

    def test_check_pattern_fail(self, df_bad_patterns):
        config = {"check_pattern": {"fips_code": r"^\d{5}$"}}
        result = Validator.run_all(df_bad_patterns, config)
        assert result.passed is False
        assert any(e["failed_count"] == 2 for e in result.errors)

    def test_check_pattern_empty_df(self, df_empty):
        config = {"check_pattern": {"person_id": r"^\d+$"}}
        result = Validator.run_all(df_empty, config)
        assert result.passed is True

    def test_check_pattern_missing_column(self, df_missing_columns):
        """Pattern check on missing column produces warning, not crash."""
        config = {"check_pattern": {"fips_code": r"^\d{5}$"}}
        result = Validator.run_all(df_missing_columns, config)
        assert result.passed is True
        assert len(result.warnings) == 1


# ============ Combined checks ============

class TestCombinedChecks:

    def test_full_validation_suite_passes(self, clean_df):
        """Clean data passes a comprehensive validation config."""
        config = {
            "min_rows": 3,
            "max_rows": 100,
            "expected_columns": ["person_id", "year", "state"],
            "check_nulls": ["person_id", "year"],
            "check_range": {
                "year": {"min": 1900, "max": 2030},
                "age": {"min": 0, "max": 120},
            },
            "check_pattern": {"fips_code": r"^\d{5}$"},
            "check_values": {"state": ["CT", "MA", "NY"]},
        }
        result = Validator.run_all(clean_df, config)
        assert result.passed is True
        assert len(result.errors) == 0

    def test_multiple_check_failures(self, df_out_of_range):
        """Multiple checks can fail simultaneously."""
        config = {
            "min_rows": 10,  # will fail (only 3 rows)
            "check_range": {"year": {"min": 1900, "max": 2030}},  # will fail
        }
        result = Validator.run_all(df_out_of_range, config)
        assert result.passed is False
        assert len(result.errors) >= 2


# ---------------------------------------------------------------------------
# Absorbed from test_validation_review_fixes.py
# ---------------------------------------------------------------------------

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
