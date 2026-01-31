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

        # Delta Lake Configuration (Option B)
        try:
            from delta import configure_spark_with_delta_pip
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            # Delta not installed or valid; proceed with standard Spark
            pass

        if spark_master:
            builder = builder.master(spark_master)
            # When connecting to an external Spark cluster (e.g., Slurm-provisioned),
            # SASL authentication must match the cluster's configuration.
            # Our Slurm cluster script disables SASL authentication, so we must match.
            builder = builder.config("spark.authenticate", "false") \
                             .config("spark.authenticate.enableSaslEncryption", "false") \
                             .config("spark.network.crypto.enabled", "false")

        # Enable extensions for Delta (if the pip config helper didn't handle it or for explicit clarity)
        # Note: configure_spark_with_delta_pip usually handles .config("spark.sql.extensions", ...)
        # But we ensure we catch it safely.
            
        self.spark = builder.getOrCreate()

    def ingest(self, input_path, output_path, format='csv', partition_by=None, balanced=True, metadata=None, naming_pattern=None, target_rows=500000, hive_partitioning=False, target_rows_per_file=None, schema=None, write_mode='overwrite', **kwargs):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        # Normalize TSV/tab to CSV with tab delimiter
        if format in ('tsv', 'tab'):
            format = 'csv'
            if 'delimiter' not in kwargs and 'delim' not in kwargs:
                kwargs['delimiter'] = '\t'

        # PySpark supports generic load with format option
        print(f"INFO: PySpark Engine reading from {input_path} (format={format})")
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
                # When hive_partitioning is True, we rely on Spark's automatic partition discovery.
                # Setting recursiveFileLookup to True DISABLES partition discovery, so we must ensure it is False.
                reader = reader.option("recursiveFileLookup", "false")
            else:
                # If NOT using hive partitioning, we usually want to search recursively for files
                # so we don't miss data in subdirectories.
                reader = reader.option("recursiveFileLookup", "true")
            

        df = reader.load(input_path)

        # Extract metadata from file paths using naming_pattern
        # This is useful when metadata wasn't extracted during config expansion
        # (e.g., when preprocessing extracts files to new directories)
        if naming_pattern:
            import re
            # Parse the naming pattern to find all named groups (in order)
            pattern_groups = re.findall(r'\?P<([^>]+)>', naming_pattern)
            if pattern_groups:
                print(f"INFO: Extracting metadata from file paths using naming_pattern: {pattern_groups}")
                # Convert Python named groups to standard groups for Spark
                # (?P<name>pattern) -> (pattern)
                spark_pattern = re.sub(r'\?P<[^>]+>', '', naming_pattern)
                print(f"DEBUG: Spark regex pattern: {spark_pattern}")

                # Add the input file name as a column
                df = df.withColumn("_input_file", F.input_file_name())
                # Extract each named group from the file path
                for idx, group_name in enumerate(pattern_groups):
                    # Group indices start at 1 in regexp_extract
                    df = df.withColumn(
                        group_name,
                        F.regexp_extract("_input_file", spark_pattern, idx + 1)
                    )
                # Drop the temporary column
                df = df.drop("_input_file")

        # Inject static metadata if available (e.g. state from filename)
        if metadata:
            for key, val in metadata.items():
                df = df.withColumn(key, F.lit(val))

        # --- OPTION A (+115): Strict Schema Enforcement ---
        if schema:
            # Normalize schema keys to match DataFrame columns (case sensitivity?)
            # Assuming widely case-sensitive/insensitive defaults from Spark.
            # Here keeping it strict python string match for integrity.

            raw_columns = set(df.columns)
            expected_columns = set(schema.keys())

            # Columns that should be preserved even if not in schema:
            # - metadata columns (extracted from file paths or injected)
            # - partition_by columns (needed for partitioning)
            preserved_columns = set()
            if metadata:
                preserved_columns.update(metadata.keys())
            if partition_by:
                preserved_columns.update(partition_by)
            # Also preserve columns extracted via naming_pattern
            if naming_pattern:
                import re
                pattern_groups = re.findall(r'\?P<([^>]+)>', naming_pattern)
                preserved_columns.update(pattern_groups)

            # RULE-115: FAIL if extra columns exist (excluding preserved columns)
            extra_cols = raw_columns - expected_columns - preserved_columns

            if extra_cols:
                raise ValueError(f"[RULE-115] Data Integrity Error: Source file contains {len(extra_cols)} extra columns not in strict schema: {extra_cols}. Ingestion aborted to prevent data dropping.")

            # Align and Fill Missing Columns
            select_exprs = []
            for col_name, col_type in schema.items():
                if col_name in df.columns:
                    # Cast to Ensure Type Strictness?
                    # Ideally yes, let's cast to the configured type string
                    select_exprs.append(F.col(col_name).cast(col_type))
                else:
                    # Fill Missing as Null (Safe Evolution)
                    select_exprs.append(F.lit(None).cast(col_type).alias(col_name))

            # Also include preserved columns that exist in the DataFrame but not in schema
            for col_name in preserved_columns:
                if col_name in df.columns and col_name not in schema:
                    select_exprs.append(F.col(col_name))

            # Apply Selection (Ordering + Filling)
            df = df.select(*select_exprs)

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
        
        # --- OPTION B: Table Formats (Delta Lake) ---
        target_format = self.config_output_format if hasattr(self, 'config_output_format') else 'parquet'
        # Check if caller passed explicit setting or if we infer from extension? 
        # Actually PySparkEngine doesn't know global state easily perfectly here without breaking sig.
        # But `ingest` usually writes to parquet. 
        # Let's see if we can detect Delta intent via kwargs or standardizing?
        # Implementation Plan says: Support `format='delta'`.
        # NOTE: `format` arg in ingest is INPUT format. We need OUTPUT format control.
        # Let's assume output format defaults parquet but checks for Delta arg or config.
        
        # Checking if 'delta' is in kwargs for output format?
        output_format = kwargs.get('output_format', 'parquet')
        
        print(f"INFO: PySpark Engine writing to {output_path} (format={output_format}, mode={write_mode}, partitions={partition_cols})")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        if output_format == 'delta':
             # Enable Schema Evolution (MergeSchema) for Delta if write_mode is append or generally
             # option("mergeSchema", "true") allows adding new columns automatically
             writer = writer.format("delta").option("mergeSchema", "true")
             writer.save(output_path)
        else:
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
        num_slices = min(len(items), 10000)
        print(f"DEBUG: Creating RDD with {num_slices} slices for {len(items)} items.")
        rdd = self.spark.sparkContext.parallelize(items, numSlices=num_slices)
        
        # Apply the function and collect results
        return rdd.map(func).collect()

    def calculate_hashes(self, file_paths, table_root=None, fast_check=True):
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
                if table_root and path.startswith(table_root):
                    rel_path = os.path.relpath(path, table_root)
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
        num_slices = min(len(file_paths), 1000)
        rdd = self.spark.sparkContext.parallelize(file_paths, numSlices=num_slices)
        return rdd.map(worker_hash_func).collect()

    def _map_spark_type(self, spark_type):
        """Map Spark type to Fairway standard type."""
        spark_type = str(spark_type)

        if 'IntegerType' in spark_type:
            return 'INTEGER'
        elif 'LongType' in spark_type:
            return 'BIGINT'
        elif 'DoubleType' in spark_type:
            return 'DOUBLE'
        elif 'FloatType' in spark_type:
            return 'FLOAT'
        elif 'StringType' in spark_type:
            return 'STRING'
        elif 'TimestampType' in spark_type:
            return 'TIMESTAMP'
        elif 'DateType' in spark_type:
            return 'DATE'
        elif 'BooleanType' in spark_type:
            return 'BOOLEAN'
        else:
            return 'STRING'  # Fallback

    def _resolve_type_conflict(self, type_a, type_b):
        """Return broader type when conflict occurs.

        Type hierarchy: BOOLEAN < INTEGER < BIGINT < DOUBLE < STRING
        STRING always wins as it can represent any value.
        """
        TYPE_HIERARCHY = ['BOOLEAN', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'STRING']

        # Normalize types
        type_a = type_a.upper() if type_a else 'STRING'
        type_b = type_b.upper() if type_b else 'STRING'

        # Get indices (unknown types default to STRING)
        idx_a = TYPE_HIERARCHY.index(type_a) if type_a in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
        idx_b = TYPE_HIERARCHY.index(type_b) if type_b in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1

        return TYPE_HIERARCHY[max(idx_a, idx_b)]

    def infer_schema(self, path, format='csv', sampling_ratio=1.0, sample_files=50, **kwargs):
        """
        Infers schema from a dataset using Spark with two-phase approach.

        Two-Phase Approach:
            Phase 1: Column Discovery - Scan ALL files to get complete column set
            Phase 2: Type Inference - Merge types from sampled files

        Args:
            path: Input path (glob or directory)
            format: Input format (csv, tsv, json, parquet)
            sampling_ratio: Fraction of data to use for type inference (0.0 to 1.0)
            sample_files: Max number of files to sample for type inference (default 50)
        Returns:
            dict: Schema dictionary compatible with Fairway config

        Note:
            - Phase 1 ensures ALL columns are captured (no missing columns)
            - Phase 2 ensures type accuracy through sampling
            - Deterministic: files are sorted before processing
        """
        import glob as glob_module

        # Normalize TSV/tab to CSV with tab delimiter
        if format in ('tsv', 'tab'):
            format = 'csv'
            if 'delimiter' not in kwargs and 'delim' not in kwargs:
                kwargs['delimiter'] = '\t'

        print(f"INFO: Inferring schema from {path} (format={format})")

        # Build glob pattern for file discovery
        if '*' not in path and os.path.isdir(path):
            ext_map = {'csv': '**/*.csv', 'tsv': '**/*.tsv', 'json': '**/*.json', 'parquet': '**/*.parquet'}
            glob_pattern = os.path.join(path, ext_map.get(format, '*'))
        else:
            glob_pattern = path

        # Discover all files (sorted for determinism)
        all_files = sorted(glob_module.glob(glob_pattern, recursive=True))
        if not all_files:
            raise ValueError(f"No files found matching pattern: {glob_pattern}")

        # ============================================================
        # PHASE 1: Column Discovery (scan ALL files for column names)
        # ============================================================
        all_columns = set()
        column_sources = {}  # {column_name: [files_that_have_it]}
        file_schemas = {}  # {file_path: {col: type}}
        errors = []

        print(f"INFO: Phase 1 - Discovering columns from {len(all_files)} files...")

        # Build a reader with common options
        def get_reader():
            reader = self.spark.read.format(format)
            if format == 'csv':
                reader = reader.option("header", "true").option("inferSchema", "true")
            if kwargs:
                opts = {k: str(v) for k, v in kwargs.items() if v is not None}
                reader = reader.options(**opts)
            return reader

        # Read schema from each file
        for f in all_files:
            try:
                df = get_reader().load(f)
                file_schema = {}
                for field in df.schema.fields:
                    col_name = field.name
                    col_type = self._map_spark_type(field.dataType)
                    file_schema[col_name] = col_type
                    all_columns.add(col_name)
                    column_sources.setdefault(col_name, []).append(f)
                file_schemas[f] = file_schema
            except Exception as e:
                error_str = str(e)
                # Re-raise Java compatibility errors - these won't be fixed by retrying
                if "getSubject" in error_str or "UnsupportedOperationException" in error_str:
                    raise RuntimeError(
                        f"PySpark has Java compatibility issues. "
                        f"This typically occurs with Java 17+. Use Java 11 or configure proper JVM options. "
                        f"Original error: {error_str}"
                    ) from e
                errors.append((f, error_str))
                continue

        if errors:
            print(f"WARNING: Failed to read {len(errors)} files during column discovery")

        if not all_columns:
            raise ValueError(f"No columns found in any files matching: {glob_pattern}")

        print(f"INFO: Phase 1 complete - Found {len(all_columns)} unique columns across {len(file_schemas)} files")

        # ============================================================
        # PHASE 2: Type Inference with Coverage Guarantee
        # ============================================================
        # Ensure at least one file per column is included
        required_files = set()
        columns_covered = set()

        for col in sorted(all_columns):  # Sorted for determinism
            if col not in columns_covered:
                source_file = column_sources[col][0]
                required_files.add(source_file)
                columns_covered.update(file_schemas[source_file].keys())

        # Add additional samples up to sample_files limit
        remaining_budget = sample_files - len(required_files)
        if remaining_budget > 0:
            remaining_files = [f for f in all_files if f not in required_files]
            additional = remaining_files[:remaining_budget]
            sample = list(required_files) + additional
        else:
            sample = list(required_files)

        sample = sorted(sample)
        print(f"INFO: Phase 2 - Using {len(sample)} files for type inference")

        # Merge types from sampled files
        column_types = {}
        for f in sample:
            if f in file_schemas:
                for col_name, col_type in file_schemas[f].items():
                    if col_name not in column_types:
                        column_types[col_name] = col_type
                    else:
                        column_types[col_name] = self._resolve_type_conflict(column_types[col_name], col_type)

        # ============================================================
        # Combine: ALL columns from Phase 1, types from Phase 2
        # ============================================================
        schema_dict = {}
        for col in sorted(all_columns):  # Sorted for deterministic output
            if col in column_types:
                schema_dict[col] = column_types[col]
            else:
                print(f"WARNING: Column '{col}' has no type - defaulting to STRING")
                schema_dict[col] = 'STRING'

        print(f"INFO: Schema inference complete - {len(schema_dict)} columns")
        return schema_dict
