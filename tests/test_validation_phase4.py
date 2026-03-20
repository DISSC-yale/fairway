"""Phase 4 tests: Structural + column-level checks.

TDD: Tests written BEFORE implementation.
Covers: expected_columns, max_rows, check_range, check_values, check_pattern.
Also covers edge cases: empty df, all-null df, missing columns.
"""
import pytest
import pandas as pd
from fairway.validations.checks import Validator
from fairway.validations.result import ValidationResult


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
