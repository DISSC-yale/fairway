"""Tests for validation checks.

Covers:
  - Level1 and level2 basic Validator checks (unit)
  - Spark validation checks (skipped when Spark unavailable)
  - Pipeline-level validation integration
  - @staticmethod fix on level2_check
  - _normalize_validation_config() (flat and legacy formats)
  - _validate_validations_block() (invalid keys/types)
  - Config normalization integration with checks
"""
import pytest
pytest.skip("validations module removed in v0.3 Step 3 — deleted in Step 4", allow_module_level=True)
import pandas as pd
from fairway.validations.checks import Validator


class TestLevel1Validation:
    """Tests for level1 (basic sanity) checks."""

    def test_min_rows_pass(self):
        """Test level1 validation passes when row count meets minimum."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        config = {"level1": {"min_rows": 2}}

        result = Validator.level1_check(df, config)

        assert result["passed"] is True
        assert len(result["errors"]) == 0

    def test_min_rows_exact(self):
        """Test level1 validation passes when row count equals minimum."""
        df = pd.DataFrame({"id": [1, 2]})
        config = {"level1": {"min_rows": 2}}

        result = Validator.level1_check(df, config)

        assert result["passed"] is True

    def test_min_rows_fail(self):
        """Test level1 validation fails when row count too low."""
        df = pd.DataFrame({"id": [1]})
        config = {"level1": {"min_rows": 5}}

        result = Validator.level1_check(df, config)

        assert result["passed"] is False
        assert len(result["errors"]) == 1
        assert "Row count" in result["errors"][0]
        assert "1" in result["errors"][0]  # actual count
        assert "5" in result["errors"][0]  # minimum required

    def test_empty_config(self):
        """Test level1 validation passes with empty config."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        config = {}

        result = Validator.level1_check(df, config)

        assert result["passed"] is True

    def test_no_min_rows_specified(self):
        """Test level1 validation passes when min_rows not specified."""
        df = pd.DataFrame({"id": [1]})
        config = {"level1": {}}

        result = Validator.level1_check(df, config)

        assert result["passed"] is True


class TestLevel2Validation:
    """Tests for level2 (schema/distribution) checks."""

    def test_null_check_pass(self):
        """Test level2 passes when no nulls in checked columns."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        config = {"level2": {"check_nulls": ["id", "name"]}}

        result = Validator.level2_check(df, config)

        assert result["passed"] is True
        assert len(result["errors"]) == 0

    def test_null_check_fail(self):
        """Test level2 fails when nulls found in checked columns."""
        df = pd.DataFrame({"id": [1, None], "name": ["a", "b"]})
        config = {"level2": {"check_nulls": ["id"]}}

        result = Validator.level2_check(df, config)

        assert result["passed"] is False
        assert len(result["errors"]) == 1
        assert "Null values" in result["errors"][0]
        assert "id" in result["errors"][0]

    def test_null_check_unchecked_column(self):
        """Test level2 passes when nulls in non-checked columns."""
        df = pd.DataFrame({"id": [1, 2], "optional": [None, "value"]})
        config = {"level2": {"check_nulls": ["id"]}}

        result = Validator.level2_check(df, config)

        assert result["passed"] is True

    def test_null_check_missing_column(self):
        """Test level2 handles checking column that doesn't exist."""
        df = pd.DataFrame({"id": [1, 2]})
        config = {"level2": {"check_nulls": ["nonexistent"]}}

        result = Validator.level2_check(df, config)

        # Should pass because column doesn't exist (no nulls to find)
        assert result["passed"] is True

    def test_empty_config(self):
        """Test level2 passes with empty config."""
        df = pd.DataFrame({"id": [1, None]})
        config = {}

        result = Validator.level2_check(df, config)

        assert result["passed"] is True

    def test_multiple_null_columns(self):
        """Test level2 reports multiple columns with nulls."""
        df = pd.DataFrame({
            "id": [1, None],
            "name": [None, "bob"],
            "value": [100, 200]
        })
        config = {"level2": {"check_nulls": ["id", "name", "value"]}}

        result = Validator.level2_check(df, config)

        assert result["passed"] is False
        assert len(result["errors"]) == 2  # id and name have nulls


class TestSparkValidation:
    """Tests for Spark-native validation checks."""

    @pytest.mark.spark
    def test_spark_level1_min_rows_pass(self, pyspark_engine):
        """Test Spark level1 validation passes when row count meets minimum."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        config = {"level1": {"min_rows": 2}}

        result = Validator.level1_check_spark(df, config)

        assert result["passed"] is True

    @pytest.mark.spark
    def test_spark_level1_min_rows_fail(self, pyspark_engine):
        """Test Spark level1 validation fails when row count too low."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame([(1,)], ["id"])
        config = {"level1": {"min_rows": 5}}

        result = Validator.level1_check_spark(df, config)

        assert result["passed"] is False
        assert "Row count" in result["errors"][0]

    @pytest.mark.spark
    def test_spark_level2_null_check_pass(self, pyspark_engine):
        """Test Spark level2 passes when no nulls in checked columns."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        config = {"level2": {"check_nulls": ["id", "name"]}}

        result = Validator.level2_check_spark(df, config)

        assert result["passed"] is True

    @pytest.mark.spark
    def test_spark_level2_null_check_fail(self, pyspark_engine):
        """Test Spark level2 fails when nulls found in checked columns."""
        spark = pyspark_engine.spark
        df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "name"])
        config = {"level2": {"check_nulls": ["id"]}}

        result = Validator.level2_check_spark(df, config)

        assert result["passed"] is False
        assert "Null values" in result["errors"][0]


# ---------------------------------------------------------------------------
# Pipeline-level validation tests
# ---------------------------------------------------------------------------

class TestValidationThroughPipeline:
    """Validation failures must stop the pipeline. Valid data must pass through."""

    def test_min_rows_fail_stops_pipeline(self, fixtures_dir, tmp_path):
        """empty.csv (0 rows) with min_rows=1 must raise."""
        from tests.helpers import build_config
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "empty_check",
            "path": str(fixtures_dir / "formats" / "csv" / "empty.csv"),
            "format": "csv",
            "validations": {"min_rows": 1},
        })
        with pytest.raises(Exception, match="[Mm]in.?rows|[Rr]ow.?count"):
            IngestionPipeline(config).run()

    def test_min_rows_pass_allows_pipeline(self, fixtures_dir, tmp_path):
        """simple.csv (3 rows) with min_rows=1 must succeed."""
        from tests.helpers import build_config, read_curated
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "rows_ok",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "validations": {"min_rows": 1},
        })
        IngestionPipeline(config).run()
        df = read_curated(tmp_path, "rows_ok")
        assert len(df) == 3

    def test_check_nulls_fail_stops_pipeline(self, fixtures_dir, tmp_path):
        """missing_values.csv has null id. check_nulls: [id] must raise."""
        from tests.helpers import build_config
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "null_check",
            "path": str(fixtures_dir / "formats" / "csv" / "missing_values.csv"),
            "format": "csv",
            "validations": {"check_nulls": ["id"]},
        })
        with pytest.raises(Exception, match="[Nn]ull|id"):
            IngestionPipeline(config).run()

    def test_check_nulls_pass_on_clean_data(self, fixtures_dir, tmp_path):
        """simple.csv has no nulls. check_nulls must not raise."""
        from tests.helpers import build_config, read_curated
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "no_nulls",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "validations": {"check_nulls": ["id", "name", "value"]},
        })
        IngestionPipeline(config).run()
        df = read_curated(tmp_path, "no_nulls")
        assert len(df) == 3

    def test_max_rows_fail_stops_pipeline(self, fixtures_dir, tmp_path):
        """simple.csv (3 rows) with max_rows=2 must raise."""
        from tests.helpers import build_config
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "max_rows_fail",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "validations": {"max_rows": 2},
        })
        with pytest.raises(Exception, match="[Mm]ax.?rows|[Rr]ow.?count"):
            IngestionPipeline(config).run()

    def test_output_layer_processed_no_transform(self, fixtures_dir, tmp_path):
        """output_layer=processed writes to processed/, not curated/."""
        from pathlib import Path
        from tests.helpers import build_config, read_processed
        from fairway.pipeline import IngestionPipeline

        config = build_config(tmp_path, table={
            "name": "early_stop",
            "path": str(fixtures_dir / "formats" / "csv" / "simple.csv"),
            "format": "csv",
            "output_layer": "processed",
        })
        IngestionPipeline(config).run()

        df = read_processed(tmp_path, "early_stop")
        assert len(df) == 3

        processed_path = Path(tmp_path) / "data" / "processed" / "early_stop"
        curated_path = Path(tmp_path) / "data" / "curated" / "early_stop"
        # One of these path patterns must exist (file or directory)
        import os
        processed_exists = processed_path.exists() or Path(str(processed_path) + ".parquet").exists()
        curated_exists = curated_path.exists() or Path(str(curated_path) + ".parquet").exists()
        assert processed_exists
        assert not curated_exists


# ---------------------------------------------------------------------------
# Absorbed from test_validation_phase1.py
# ---------------------------------------------------------------------------

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
