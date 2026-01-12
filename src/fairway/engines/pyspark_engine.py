try:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None
    F = None

import random

class PySparkEngine:
    def __init__(self, spark_master=None):
        if SparkSession is None:
            raise ImportError("PySpark is not installed. Please install fairway[spark] or fairway[all].")
        builder = SparkSession.builder.appName("fairway-ingestion")
        if spark_master:
            builder = builder.master(spark_master)
        self.spark = builder.getOrCreate()

    def ingest(self, input_path, output_path, format='csv', partition_by=None, balanced=True, metadata=None):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        # PySpark supports generic load with format option
        reader = self.spark.read.format(format)
        
        if format == 'csv':
            reader = reader.option("header", "true").option("inferSchema", "true")
            
        df = reader.load(input_path)
        
        # Inject metadata if available (e.g. state from filename)
        if metadata:
            from pyspark.sql import functions as F
            for key, val in metadata.items():
                df = df.withColumn(key, F.lit(val))

        if balanced and partition_by:
            # Salting logic inspired by data_l2 to prevent skew
            # Assuming ~500k rows per file is a good default
            target_rows = 500000
            total_rows = df.count()
            num_salts = max(1, total_rows // target_rows)
            
            df = df.withColumn("salt", (F.rand() * num_salts).cast("int"))
            partition_cols = partition_by + ["salt"]
        else:
            partition_cols = partition_by

        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        writer.parquet(output_path)
        return True

    def query(self, query):
        """Mock query interface for pipeline compatibility (usually returns Pandas)."""
        # In a real Spark pipeline, we'd stay in Spark DataFrames
        # For fairway's current design, we collect to Pandas for localized enrichment/validation
        return self.spark.sql(query).toPandas()
