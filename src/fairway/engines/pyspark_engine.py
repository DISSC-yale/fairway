try:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
except ImportError as e:
    SparkSession = None
    F = None
    _spark_import_error = e

except ImportError as e:
    SparkSession = None
    F = None
    _spark_import_error = e

import random
import os
import re


class PySparkEngine:
    def __init__(self, spark_master=None):
        if SparkSession is None:
            additional_info = f" Original error: {_spark_import_error}" if '_spark_import_error' in globals() else ""
            raise ImportError(f"PySpark is not installed or failed to load. Please install fairway[spark] or fairway[all].{additional_info}")
        builder = SparkSession.builder.appName("fairway-ingestion")
        # Add JVM options for modern Java (17+) compatibility
        # Java 25 requires explicitly opening javax.security.auth and others
        jvm_options_list = [
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
        ]
        jvm_options = " ".join(jvm_options_list)
        
        # Ensure these options are passed to the driver via env if not already set (relying on builder config is sometimes insufficient)
        import os
        if 'PYSPARK_SUBMIT_ARGS' not in os.environ:
             os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-java-options "{jvm_options}" pyspark-shell'

        builder = builder.config("spark.driver.extraJavaOptions", jvm_options) \
                         .config("spark.executor.extraJavaOptions", jvm_options)

        if spark_master:
            builder = builder.master(spark_master)
        self.spark = builder.getOrCreate()

    def ingest(self, input_path, output_path, format='csv', partition_by=None, balanced=True, metadata=None, target_rows=500000, hive_partitioning=False, **kwargs):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        # PySpark supports generic load with format option
        reader = self.spark.read.format(format)
        
        if format == 'csv':
            reader = reader.option("header", "true").option("inferSchema", "true")
            if hive_partitioning:
                # Ensure recursive file lookup creates the correct partition discovery
                reader = reader.option("recursiveFileLookup", "true")
            
        df = reader.load(input_path)
        
        # Inject metadata if available (e.g. state from filename)
        # Inject metadata if available (e.g. state from filename)
        if metadata:
            for key, val in metadata.items():
                df = df.withColumn(key, F.lit(val))

        if balanced and partition_by:
            # Salting logic inspired by data_l2 to prevent skew
            # Assuming ~500k rows per file is a good default, or user provided value
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

    def inspect(self, query, limit=100000, as_pandas=True):
        """
        Inspect data using SQL (Control Plane only).
        WARNING: This pulls data to the driver. Always limited by default (100k rows) to prevent OOM.
        """
        # Intercept DuckDB-style "SELECT * FROM 'path'" queries
        match = re.search(r"SELECT\s+\*\s+FROM\s+'([^']*)'", query, re.IGNORECASE)
        if match:
             path = match.group(1)
             try:
                 # Clean up recursive glob for Spark to ensure partition discovery works best on the dir
                 if path.endswith("/**/*.parquet"):
                     path = path.replace("/**/*.parquet", "")
                 elif path.endswith("/**/*.csv"):
                     path = path.replace("/**/*.csv", "")
                     
                 # Spark doesn't support 'FROM "file"' syntax in SQL directly for all versions/configs
                 # But we can easily route this to spark.read
                 if ".parquet" in path or os.path.isdir(path):
                      df = self.spark.read.parquet(path)
                 elif ".csv" in path:
                      df = self.spark.read.option("header", "true").csv(path)
                 elif ".json" in path:
                      df = self.spark.read.json(path)
                 else:
                      # Fallback
                      df = self.spark.sql(query)
                      
                 if limit:
                     print(f"Applying limit of {limit} rows to driver inspection.")
                     df = df.limit(limit)
                     
                 if as_pandas:
                     return df.toPandas()
                 return df

             except Exception as e:
                 print(f"WARNING: Failed to optimize file query '{query}': {e}. Falling back to SQL.")

        # Standard SQL
        df = self.spark.sql(query)
        
        if limit:
            print(f"Applying limit of {limit} rows to driver inspection.")
            df = df.limit(limit)
            
        if as_pandas:
            return df.toPandas()
        return df

    def read_result(self, path):
        """
        Reads a Parquet result from the given path into a Spark DataFrame (Lazy).
        Spark handles directory recursion and schema discovery automatically.
        Enables 'mergeSchema' to handle evolving schemas across partitions.
        """
        return self.spark.read.option("mergeSchema", "true").parquet(path)
