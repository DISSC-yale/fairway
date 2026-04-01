"""Tests for PySpark salting — balanced partition distribution for skewed data."""
import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not available")
from pyspark.sql import functions as F


class TestSaltingDistribution:
    """Salting must spread skewed data across multiple salt buckets."""

    def test_salting_distributes_skewed_data(self, spark_session_2, tmp_path):
        """
        950 rows with category=A, 50 with category=B.
        With target_rows=100, num_salts = 1000 // 100 = 10.
        Each category=A bucket must have <1000 rows (not all skewed data in one partition).
        """
        from fairway.engines.pyspark_engine import PySparkEngine
        engine = PySparkEngine.__new__(PySparkEngine)
        engine.spark = spark_session_2

        data = [{"id": i, "category": "A"} for i in range(950)]
        data += [{"id": i + 1000, "category": "B"} for i in range(50)]
        df = engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=100,
        )

        result = engine.spark.read.parquet(str(output))

        # 1. Row count preserved
        assert result.count() == 1000

        # 2. Salt partition directory exists — proves salting was applied
        assert "salt" in result.columns

        # 3. Multiple distinct salt values — proves distribution happened
        distinct_salts = result.select("salt").distinct().count()
        assert distinct_salts > 1, f"Expected >1 salt bucket, got {distinct_salts}"

        # 4. No single bucket holds all skewed data
        rows_per_salt = result.groupBy("salt").count().collect()
        max_in_bucket = max(r["count"] for r in rows_per_salt)
        assert max_in_bucket < 1000, (
            f"All rows landed in one salt bucket — skew not resolved. "
            f"Max bucket size: {max_in_bucket}"
        )

        # 5. Dominant category (A) is spread across >1 salt value
        category_a_salts = (
            result.filter(F.col("category") == "A")
            .select("salt").distinct().count()
        )
        assert category_a_salts > 1, (
            f"category=A still confined to 1 salt bucket after salting"
        )

    def test_salting_preserves_all_rows(self, spark_session_2, tmp_path):
        """Salting must never drop data."""
        from fairway.engines.pyspark_engine import PySparkEngine
        engine = PySparkEngine.__new__(PySparkEngine)
        engine.spark = spark_session_2

        data = [{"id": i, "category": "A" if i % 10 != 0 else "B"} for i in range(500)]
        df = engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=50,
        )

        result = engine.spark.read.parquet(str(output))
        assert result.count() == 500

    def test_small_dataset_uses_single_salt(self, spark_session_2, tmp_path):
        """
        When total_rows < target_rows, num_salts=1 — no distribution needed.
        This is correct: small datasets shouldn't pay the salting overhead.
        """
        from fairway.engines.pyspark_engine import PySparkEngine
        engine = PySparkEngine.__new__(PySparkEngine)
        engine.spark = spark_session_2

        data = [{"id": i, "category": "A"} for i in range(100)]
        df = engine.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(str(tmp_path / "input"))

        output = tmp_path / "output"
        engine.ingest(
            str(tmp_path / "input"),
            str(output),
            format="parquet",
            partition_by=["category"],
            balanced=True,
            target_rows=500_000,  # num_salts = 100 // 500000 = 0 → max(1, 0) = 1
        )

        result = engine.spark.read.parquet(str(output))
        distinct_salts = result.select("salt").distinct().count()
        assert distinct_salts == 1, (
            f"Expected 1 salt for small dataset, got {distinct_salts}"
        )
