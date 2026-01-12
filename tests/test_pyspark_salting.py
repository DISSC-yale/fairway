import pytest
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from fairway.engines.pyspark_engine import PySparkEngine

# Attempt to initialize Spark to see if it works
try:
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_salting") \
        .getOrCreate()
    SPARK_AVAILABLE = True
except Exception:
    SPARK_AVAILABLE = False

@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available or misconfigured")
class TestPySparkSalting:
    @pytest.fixture(scope="class")
    def spark(self):
        return spark

    @pytest.fixture
    def engine(self):
        return PySparkEngine(spark_master="local[1]")

    def test_salting_logic(self, engine, tmp_path):
        # Create a dataframe with 1000 rows
        data = [{"id": i, "category": "A"} for i in range(1000)]
        df = engine.spark.createDataFrame(data)
        
        input_path = str(tmp_path / "input")
        output_path = str(tmp_path / "output")
        
        # Save input as parquet for ingestion
        df.write.parquet(input_path)
        
        # 1. Test with default target_rows (500,000) -> should have 1 salt (1000 // 500000 = 0 -> max(1, 0) = 1)
        # We can't easily check internal variables, but we can check the output partition structure if we partition by salt
        # But wait, salt is added to partition_cols.
        
        # Let's use a very small target_rows to force multiple salts
        target_rows = 100
        engine.ingest(
            input_path, 
            output_path, 
            format='parquet', 
            partition_by=['category'], 
            balanced=True, 
            target_rows=target_rows
        )
        
        # Read back and check if 'salt' column exists and has multiple values
        # Note: ingest writes to output_path. partitions will be /category=A/salt=X/
        
        result_df = engine.spark.read.parquet(output_path)
        
        # Check schema for salt
        assert "salt" in result_df.columns
        
        # Check distinct salt values
        unique_salts = result_df.select("salt").distinct().count()
        expected_salts = 1000 // 100 # = 10
        # It's random, but with 1000 rows and 10 bins, we should see close to 10 bins occupied
        assert unique_salts > 1, f"Expected multiple salts, got {unique_salts}"
        
    def test_salting_disabled_by_default_target_rows(self, engine, tmp_path):
        # 1000 rows, default target 500k -> 1 salt
        data = [{"id": i, "category": "A"} for i in range(1000)]
        df = engine.spark.createDataFrame(data)
        input_path = str(tmp_path / "input_default")
        output_path = str(tmp_path / "output_default")
        df.write.parquet(input_path)
        
        engine.ingest(
            input_path, 
            output_path, 
            format='parquet', 
            partition_by=['category'], 
            balanced=True 
            # target_rows default is 500k
        )
        
        result_df = engine.spark.read.parquet(output_path)
        unique_salts = result_df.select("salt").distinct().count()
        assert unique_salts == 1
