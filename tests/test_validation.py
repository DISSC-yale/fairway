"""Tests for validation checks."""
import pytest
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
