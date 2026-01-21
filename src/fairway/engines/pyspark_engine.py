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

    def ingest(self, input_path, output_path, format='csv', partition_by=None, balanced=True, metadata=None, target_rows=500000, hive_partitioning=False, target_rows_per_file=None, schema=None, write_mode='overwrite', **kwargs):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        # PySpark supports generic load with format option
        reader = self.spark.read.format(format)
        
        # Apply Generic Read Options (passthrough)
        # e.g. header='false', delim='|', quote='"'
        if kwargs:
            # filters out None values just in case
            opts = {k: str(v) for k, v in kwargs.items() if v is not None}
            reader = reader.options(**opts)

        if format == 'csv':
            # Default behavior if not overridden options
            if 'header' not in kwargs:
                 reader = reader.option("header", "true")
            if 'inferSchema' not in kwargs:
                 reader = reader.option("inferSchema", "true")
                 
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
            total_rows_approx = df.rdd.count() # Force count for salt calculation
            if target_rows_per_file:
                 target_rows = target_rows_per_file
            
            num_salts = max(1, total_rows_approx // target_rows)
            
            df = df.withColumn("salt", (F.rand() * num_salts).cast("int"))
            partition_cols = partition_by + ["salt"]
        else:
            partition_cols = partition_by

        writer = df.write.mode(write_mode)
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

    def distribute_task(self, items, func):
        """
        Distributes a task (python function) across the Spark cluster using RDDs.
        
        Args:
            items (list): List of items (e.g., file paths) to process.
            func (callable): Python function that takes an item and returns a result.
            
        Returns:
            list: List of results collected from workers.
        """
        if not items:
            return []
            
        # Parallelize the items into an RDD
        # numSlices=len(items) ensures max parallelism, or spark default
        # For very large lists, standard parallelism is better.
        # For standard ingestion (1000s of files), defaults are usually fine, 
        # but we might want at least as many partitions as executors.
        rdd = self.spark.sparkContext.parallelize(items, numSlices=min(len(items), 10000))
        
        # Apply the function and collect results
        return rdd.map(func).collect()

    def calculate_hashes(self, file_paths, source_root=None, fast_check=True):
        """
        Calculates fingerprints for a list of files in parallel using Spark workers.
        Returns a list of result dicts containing path, rel_path, hash, and error.
        """
        if not file_paths:
            return []
            
        def worker_hash_func(path):
            import os
            import hashlib
            import glob
            
            try:
                # 1. Calculate Relative Key Part
                if source_root and path.startswith(source_root):
                    rel_path = os.path.relpath(path, source_root)
                else:
                    rel_path = os.path.basename(path)
                rel_path = rel_path.replace(os.sep, '/')
                
                # 2. Calculate Hash
                # Check for glob pattern first (before exists check which fails for globs)
                if '*' in path:
                     # Glob Logic
                     # For fast_check on glob, we can't easily do mtime on the pattern.
                     # We sum the mtimes of matching files?
                     # Or we fall back to hashing contents (slow but correct).
                     # Let's match ManifestManager:
                     files = sorted(glob.glob(path, recursive=True))
                     if not files:
                         return {'path': path, 'hash': hashlib.sha256(b"empty_glob").hexdigest(), 'error': None}
                         
                     if fast_check:
                         # Aggregate mtime:size signature
                         # Use strict sorting for determinism
                         sig_parts = []
                         for f in files:
                             if os.path.isfile(f):
                                 s = os.stat(f)
                                 sig_parts.append(f"{f}:{s.st_mtime}:{s.st_size}")
                         file_hash = hashlib.sha256("".join(sig_parts).encode('utf-8')).hexdigest()
                     else:
                         sha256_hash = hashlib.sha256()
                         for fpath in files:
                             if os.path.isfile(fpath):
                                 rel = os.path.relpath(fpath, os.path.dirname(path) or '.')
                                 sha256_hash.update(rel.encode('utf-8'))
                                 try:
                                     with open(fpath, "rb") as f:
                                         for byte_block in iter(lambda: f.read(4096), b""):
                                             sha256_hash.update(byte_block)
                                 except (IOError, OSError):
                                     pass
                         file_hash = sha256_hash.hexdigest()
                         
                elif not os.path.exists(path):
                    return {'path': path, 'error': "File not found"}

                elif os.path.isdir(path):
                     # Directory Logic
                     if fast_check:
                         sig_parts = []
                         for root, dirs, files in sorted(os.walk(path)):
                            for names in sorted(files):
                                filepath = os.path.join(root, names)
                                s = os.stat(filepath)
                                sig_parts.append(f"{filepath}:{s.st_mtime}:{s.st_size}")
                         file_hash = hashlib.sha256("".join(sig_parts).encode('utf-8')).hexdigest()
                     else:
                        sha256_hash = hashlib.sha256()
                        for root, dirs, files in sorted(os.walk(path)):
                            for names in sorted(files):
                                filepath = os.path.join(root, names)
                                rel = os.path.relpath(filepath, path)
                                sha256_hash.update(rel.encode('utf-8'))
                                try:
                                    with open(filepath, "rb") as f:
                                        for byte_block in iter(lambda: f.read(4096), b""):
                                            sha256_hash.update(byte_block)
                                except (IOError, OSError):
                                    pass
                        file_hash = sha256_hash.hexdigest()

                elif fast_check:
                    stats = os.stat(path)
                    file_hash = f"mtime:{stats.st_mtime}_size:{stats.st_size}"
                else:
                    with open(path, "rb") as f:
                        for byte_block in iter(lambda: f.read(4096), b""):
                            sha256_hash.update(byte_block)
                            
                    file_hash = sha256_hash.hexdigest()
                    
                return {
                    'path': path,
                    'rel_path': rel_path,
                    'hash': file_hash,
                    'error': None
                }
            except Exception as e:
                return {
                    'path': path, 
                    'error': str(e)
                }

        # Parallelize
        # Use simple partitioning based on file count
        num_slices = min(len(file_paths), 1000)
        rdd = self.spark.sparkContext.parallelize(file_paths, numSlices=num_slices)
        return rdd.map(worker_hash_func).collect()
