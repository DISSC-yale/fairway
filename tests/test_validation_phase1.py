"""Phase 1 tests: Foundation fixes + config normalization.

TDD: These tests are written BEFORE implementation.
Tests cover:
  - @staticmethod fix on level2_check
  - _normalize_validation_config() accepts flat and legacy formats
  - _validate_validations_block() rejects invalid keys/types
"""
import pytest
import pandas as pd
from fairway.validations.checks import Validator


# ============ @staticmethod fix ============

class TestLevel2StaticMethod:
    """level2_check must be callable as Validator.level2_check() (static)."""

    def test_level2_check_callable_as_static(self):
        """Calling Validator.level2_check() without instance must work."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        config = {"level2": {"check_nulls": ["id"]}}
        result = Validator.level2_check(df, config)
        assert result["passed"] is True

    def test_level2_check_callable_on_instance(self):
        """Calling v.level2_check() on an instance must also work."""
        v = Validator()
        df = pd.DataFrame({"id": [1, None]})
        config = {"level2": {"check_nulls": ["id"]}}
        result = v.level2_check(df, config)
        assert result["passed"] is False


# ============ Config normalization ============

class TestNormalizeValidationConfig:
    """_normalize_validation_config accepts flat and legacy formats."""

    def test_flat_config_passthrough(self):
        """Flat keys are returned as-is."""
        raw = {"min_rows": 100, "check_nulls": ["id"]}
        result = Validator._normalize_validation_config(raw)
        assert result == {"min_rows": 100, "check_nulls": ["id"]}

    def test_legacy_level1_level2_flattened(self):
        """Legacy level1/level2 nesting is flattened to top-level keys."""
        raw = {
            "level1": {"min_rows": 100},
            "level2": {"check_nulls": ["id"]},
        }
        result = Validator._normalize_validation_config(raw)
        assert result["min_rows"] == 100
        assert result["check_nulls"] == ["id"]

    def test_empty_config(self):
        """Empty dict returns empty dict."""
        result = Validator._normalize_validation_config({})
        assert result == {}

    def test_none_config(self):
        """None returns empty dict."""
        result = Validator._normalize_validation_config(None)
        assert result == {}

    def test_mixed_flat_and_legacy_raises(self):
        """Mixing flat keys with level1/level2 nesting is ambiguous — reject it."""
        raw = {
            "min_rows": 100,
            "level1": {"min_rows": 200},
        }
        with pytest.raises(ValueError, match="[Cc]annot mix"):
            Validator._normalize_validation_config(raw)

    def test_legacy_with_empty_levels(self):
        """Legacy format with empty level dicts normalizes cleanly."""
        raw = {"level1": {}, "level2": {}}
        result = Validator._normalize_validation_config(raw)
        assert result == {}


# ============ Validation block validation ============

class TestValidateValidationsBlock:
    """_validate_validations_block rejects invalid keys and types."""

    def test_valid_min_rows(self):
        """min_rows with int value passes."""
        errors = Validator._validate_validations_block({"min_rows": 100})
        assert errors == []

    def test_min_rows_must_be_int(self):
        """min_rows with non-int value is rejected."""
        errors = Validator._validate_validations_block({"min_rows": "banana"})
        assert len(errors) == 1
        assert "min_rows" in errors[0]

    def test_min_rows_must_be_positive(self):
        """min_rows with zero or negative value is rejected."""
        errors = Validator._validate_validations_block({"min_rows": 0})
        assert len(errors) == 1
        errors = Validator._validate_validations_block({"min_rows": -5})
        assert len(errors) == 1

    def test_check_nulls_must_be_list(self):
        """check_nulls with non-list value is rejected."""
        errors = Validator._validate_validations_block({"check_nulls": "id"})
        assert len(errors) == 1
        assert "check_nulls" in errors[0]

    def test_check_nulls_list_of_strings(self):
        """check_nulls with list of strings passes."""
        errors = Validator._validate_validations_block({"check_nulls": ["id", "name"]})
        assert errors == []

    def test_unknown_key_rejected(self):
        """Unknown validation key is rejected."""
        errors = Validator._validate_validations_block({"check_nonexistent": True})
        assert len(errors) == 1
        assert "check_nonexistent" in errors[0]

    def test_empty_config_passes(self):
        """Empty config has no errors."""
        errors = Validator._validate_validations_block({})
        assert errors == []

    def test_multiple_errors_collected(self):
        """Multiple issues produce multiple errors."""
        errors = Validator._validate_validations_block({
            "min_rows": "banana",
            "check_nulls": 42,
            "fake_key": True,
        })
        assert len(errors) == 3


# ============ Integration: normalized config works with checks ============

class TestNormalizedConfigIntegration:
    """Checks work correctly after config normalization."""

    def test_level1_with_flat_config(self):
        """min_rows from flat config works with level1_check."""
        df = pd.DataFrame({"id": [1]})
        flat = {"min_rows": 5}
        # After normalization, level1_check should be able to use flat config
        result = Validator.level1_check(df, flat)
        assert result["passed"] is False

    def test_level2_with_flat_config(self):
        """check_nulls from flat config works with level2_check."""
        df = pd.DataFrame({"id": [1, None]})
        flat = {"check_nulls": ["id"]}
        result = Validator.level2_check(df, flat)
        assert result["passed"] is False
