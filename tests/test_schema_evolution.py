
import pytest
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from fairway.engines.pyspark_engine import PySparkEngine

# Skip if PySpark is not available (though project requires it for these features)
try:
    import pyspark
except ImportError:
    pytest.skip("PySpark not installed", allow_module_level=True)

@pytest.fixture(scope="module")
def spark():
    builder = SparkSession.builder.master("local[1]").appName("fairway-test-schema")
    # Try to add delta if available for testing
    try:
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass
    
    sess = builder.getOrCreate()
    yield sess
    sess.stop()

@pytest.fixture
def engine(spark):
    # Hack engine to use our fixture spark session
    eng = PySparkEngine(spark_master="local[1]")
    eng.spark = spark 
    return eng

@pytest.fixture
def temp_dir(tmp_path):
    d = tmp_path / "fairway_test"
    d.mkdir()
    return str(d)

def test_strict_schema_validation_success(engine, temp_dir):
    """Test Option A: Strict schema works when data matches."""
    schema = {"id": "INTEGER", "name": "STRING"}
    
    # Create valid CSV in a directory
    input_dir = os.path.join(temp_dir, "valid_input")
    os.makedirs(input_dir, exist_ok=True)
    input_path = os.path.join(input_dir, "data.csv")
    
    with open(input_path, "w") as f:
        f.write("id,name\n1,Alice\n2,Bob")
        
    output_path = os.path.join(temp_dir, "output_valid.parquet")
    
    # Run Ingest on DIRECTORY
    success = engine.ingest(
        input_dir, output_path, format="csv", schema=schema, output_format="parquet"
    )
    assert success
    
    df = engine.read_result(output_path)
    assert df.count() == 2
    assert "id" in df.columns
    assert "name" in df.columns

def test_strict_schema_validation_extra_col_fail(engine, temp_dir):
    """Test Option A (Rule 115): Fails on extra columns."""
    schema = {"id": "INTEGER", "name": "STRING"}
    
    # Create CSV with EXTRA column 'age' in a directory
    input_dir = os.path.join(temp_dir, "extra_input")
    os.makedirs(input_dir, exist_ok=True)
    input_path = os.path.join(input_dir, "data.csv")
    
    with open(input_path, "w") as f:
        f.write("id,name,age\n1,Alice,30")
        
    output_path = os.path.join(temp_dir, "output_extra.parquet")
    
    # Expect Failure
    with pytest.raises(ValueError, match=r"\[RULE-115\] Data Integrity Error"):
        engine.ingest(
            input_dir, output_path, format="csv", schema=schema, output_format="parquet"
        )

def test_strict_schema_validation_fill_missing(engine, temp_dir):
    """Test Option A: Fills missing columns with null."""
    schema = {"id": "INTEGER", "name": "STRING", "city": "STRING"}
    
    # Create CSV MISSING 'city' in directory
    input_dir = os.path.join(temp_dir, "missing_input")
    os.makedirs(input_dir, exist_ok=True)
    input_path = os.path.join(input_dir, "data.csv")
    
    with open(input_path, "w") as f:
        f.write("id,name\n1,Alice")
        
    output_path = os.path.join(temp_dir, "output_missing.parquet")
    
    success = engine.ingest(
        input_dir, output_path, format="csv", schema=schema, output_format="parquet"
    )
    assert success
    
    df = engine.read_result(output_path)
    # Check city exists and is null
    row = df.first()
    assert "city" in df.columns
    assert row['city'] is None

def test_delta_lake_evolution(engine, temp_dir):
    """Test Option B: Delta Lake evolution."""
    try:
        import delta
    except ImportError:
        pytest.skip("Delta Delta not installed")

    # Batch 1: Simple
    input_dir1 = os.path.join(temp_dir, "delta_in1")
    os.makedirs(input_dir1, exist_ok=True)
    input1 = os.path.join(input_dir1, "batch1.csv")
    
    with open(input1, "w") as f:
        f.write("id,name\n1,Alice")
        
    delta_path = os.path.join(temp_dir, "delta_table")
    
    engine.ingest(
        input_dir1, delta_path, format="csv", output_format="delta", write_mode="overwrite"
    )
    
    # Verify Batch 1
    df = engine.spark.read.format("delta").load(delta_path)
    assert df.count() == 1
    assert "id" in df.columns and "name" in df.columns
    
    # Batch 2: Evolve (Add 'age')
    input_dir2 = os.path.join(temp_dir, "delta_in2")
    os.makedirs(input_dir2, exist_ok=True)
    input2 = os.path.join(input_dir2, "batch2.csv")
    
    with open(input2, "w") as f:
        f.write("id,name,age\n2,Bob,30")
        
    # Note: mergeSchema option is set inside engine for Delta
    engine.ingest(
        input_dir2, delta_path, format="csv", output_format="delta", write_mode="append"
    )
    
    # Verify Evolution
    df = engine.spark.read.format("delta").load(delta_path)
    assert df.count() == 2
    assert "age" in df.columns # Schema evolved!
    
    # Check Alice (row 1) has null age
    row1 = df.filter("id=1").first()
    assert row1['age'] is None
