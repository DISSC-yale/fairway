try:
    import duckdb
except ImportError as e:
    duckdb = None
    _duckdb_import_error = e

import os

class DuckDBEngine:
    def __init__(self):
        if duckdb is None:
            additional_info = f" Original error: {_duckdb_import_error}" if '_duckdb_import_error' in globals() else ""
            raise ImportError(f"DuckDB is not installed or failed to load. Please install fairway[duckdb] or fairway[all].{additional_info}")
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute("INSTALL aws; LOAD aws;") # Or gcs if needed

    def ingest(self, input_path, output_path, format='csv', partition_by=None, metadata=None, hive_partitioning=False, target_rows=None, schema=None, write_mode='overwrite', **kwargs):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        # Normalize TSV/tab to CSV with tab delimiter
        if format in ('tsv', 'tab'):
            format = 'csv'
            if 'delim' not in kwargs:
                kwargs['delim'] = '\t'

        if format == 'csv':
            return self._ingest_csv(input_path, output_path, partition_by, metadata, hive_partitioning, schema, write_mode, **kwargs)
        elif format == 'json':
            return self._ingest_json(input_path, output_path, partition_by, metadata, write_mode, **kwargs)
        elif format == 'parquet':
            return self._ingest_parquet(input_path, output_path, partition_by, metadata, write_mode, **kwargs)
        else:
            raise ValueError(f"Unsupported format for DuckDB engine: {format}")

    def _ingest_csv(self, input_path, output_path, partition_by=None, metadata=None, hive_partitioning=False, schema=None, write_mode='overwrite', **kwargs):
        """
        Converts CSV to Parquet using DuckDB, with metadata injection.
        """
        # Construct read_csv_auto options
        options = []
        
        # Handle header=False logic: if no header, we typically need to provide names/types from schema
        if kwargs.get('header') is False and schema:
            # DuckDB read_csv_auto can take names/types
            # But the most robust way for read_csv_auto with no header is often to just let it infer or pass columns
            # However, mapping kwargs directly:
            pass

        # Pass through arbitrary kwargs to the SQL function string
        # e.g. read_csv_auto('path', header=False, delim='|')
        # We need to format python kwargs to SQL text options
        # e.g. header=False -> header=False
        
        # Explicit handling for common options to ensure correct types/formatting in SQL
        if hive_partitioning:
            kwargs['hive_partitioning'] = 1

        if kwargs.get('header') is False and schema:
             # If header is false, we try to use the schema keys as names if DuckDB supports it in read_csv_auto
             # Actually, read_csv_auto supports 'names' list. 
             kwargs['names'] = list(schema.keys())

        for k, v in kwargs.items():
            if isinstance(v, bool):
                val = str(v).lower() # true/false
            elif isinstance(v, str):
                val = f"'{v}'"
            elif isinstance(v, list):
                # list -> ['a', 'b']
                val = f"[{', '.join([f'{repr(x)}' for x in v])}]"
            else:
                val = v
            options.append(f"{k}={val}")
        
        options_str = ", ".join(options)
        if options_str:
            options_str = ", " + options_str

        # Check if input path looks like a glob. If not, make it recursive.
        if '*' not in input_path and os.path.isdir(input_path):
             read_path = os.path.join(input_path, "**/*.csv")
        else:
             read_path = input_path
             
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_csv_auto('{read_path}'{options_str})")
        
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _ingest_json(self, input_path, output_path, partition_by=None, metadata=None, write_mode='overwrite', **kwargs):
        """
        Converts JSON to Parquet.
        """
        # TODO: Pass kwargs to read_json_auto if needed
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_json_auto('{input_path}')")
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _ingest_parquet(self, input_path, output_path, partition_by=None, metadata=None, write_mode='overwrite', **kwargs):
        """
        Pass-through Parquet ingestion (useful for unifying pipeline logic).
        """
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_parquet('{input_path}')")
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _write_to_parquet(self, output_path, partition_by=None, metadata=None, write_mode='overwrite'):
        # Inject metadata columns if provided
        select_clause = "*"
        if metadata:
            meta_cols = ", ".join([f"'{val}' AS {key}" for key, val in metadata.items()])
            select_clause = f"*, {meta_cols}"

        # Write to Parquet with optional Hive partitioning
        partition_clause = ""
        if partition_by:
            # DuckDB 1.0+ supports PARTITION_BY in COPY
            partition_clause = f", PARTITION_BY ({', '.join(partition_by)})"
            
        # Determine overwrite behavior
        # DuckDB's COPY ... (OVERWRITE TRUE) replaces the directory/file
        # If write_mode='append', we should NOT use OVERWRITE TRUE. 
        # However, DuckDB 0.9.x/1.0 COPY to parquet directory behavior works as append if OVERWRITE is not specified?
        # Let's verify standard DuckDB behavior:
        # COPY ... TO 'dir' (FORMAT PARQUET, PARTITION_BY ...) -> Writes new files into dir.
        
        overwrite_option = "OVERWRITE_OR_IGNORE TRUE" if write_mode == 'overwrite' else "OVERWRITE_OR_IGNORE FALSE" 
        # Actually DuckDB syntax is typically: OVERWRITE TRUE/FALSE.
        # If write_mode is 'append', we want OVERWRITE FALSE (default usually, or just don't specify)
        
        overwrite_val = "TRUE" if write_mode == 'overwrite' else "FALSE"

        self.con.execute(f"""
            COPY (SELECT {select_clause} FROM raw_data) 
            TO '{output_path}' 
            (FORMAT PARQUET{partition_clause}, OVERWRITE {overwrite_val})
        """)
        return True

    def inspect(self, query, limit=None, as_pandas=True):
        """
        Inspect data using SQL (Control Plane only).
        """
        # DuckDB is already local, limit is less critical but kept for API consistency
        df = self.con.execute(query).df()
        if limit:
             df = df.head(limit)
        return df if as_pandas else df

    def read_result(self, path):
        """
        Reads a Parquet result from the given path into a DuckDB Relation (Lazy).
        """
        if os.path.isfile(path):
             return self.con.sql(f"SELECT * FROM '{path}'")
        return self.con.sql(f"SELECT * FROM '{path}/**/*.parquet'")

    def infer_schema(self, path, format='csv', sampling_ratio=0.1, sample_files=50, rows_per_file=1000, **kwargs):
        """
        Infers schema from a dataset using DuckDB with random sampling across files.

        Args:
            path: Input path (glob or directory)
            format: Input format (csv, tsv, json, parquet)
            sampling_ratio: Fraction of files to sample (0.0 to 1.0)
            sample_files: Max number of files to sample (default 50)
            rows_per_file: Rows to sample from each file (default 1000)
        Returns:
            dict: Schema dictionary compatible with Fairway config

        Note:
            - Randomly samples files to catch type variations across dataset
            - union_by_name=true: merges schemas across sampled files
            - For scalability considerations, see docs/design_docs/schema_inference_scalability.md
        """
        import glob as glob_module
        import random

        # Normalize TSV/tab to CSV
        if format in ('tsv', 'tab'):
            format = 'csv'

        print(f"INFO: Inferring schema from {path} (format={format})")

        # Build glob pattern for file discovery
        if '*' not in path and os.path.isdir(path):
            ext_map = {'csv': '**/*.csv', 'tsv': '**/*.tsv', 'json': '**/*.json', 'parquet': '**/*.parquet'}
            glob_pattern = os.path.join(path, ext_map.get(format, '*'))
        else:
            glob_pattern = path

        # Discover all files
        all_files = glob_module.glob(glob_pattern, recursive=True)
        if not all_files:
            raise ValueError(f"No files found matching pattern: {glob_pattern}")

        # Randomly sample files
        num_to_sample = min(sample_files, max(1, int(len(all_files) * sampling_ratio)))
        sampled_files = random.sample(all_files, min(num_to_sample, len(all_files)))
        print(f"INFO: Sampling {len(sampled_files)} of {len(all_files)} files for schema inference")

        # Build query to sample rows from each file and union them
        read_func = {'csv': 'read_csv_auto', 'json': 'read_json_auto', 'parquet': 'read_parquet'}[format]

        if len(sampled_files) == 1:
            query = f"SELECT * FROM {read_func}('{sampled_files[0]}') LIMIT {rows_per_file}"
        else:
            # Union samples from each file
            subqueries = [f"SELECT * FROM {read_func}('{f}') LIMIT {rows_per_file}" for f in sampled_files]
            query = " UNION ALL BY NAME ".join(subqueries)

        rel = self.con.sql(query)

        # Extract schema from DuckDB and convert to Fairway types
        schema_dict = {}
        for col_name, col_type in zip(rel.columns, rel.types):
            dtype_str = str(col_type).upper()

            # Map DuckDB types to standard types
            if 'INT' in dtype_str and 'BIGINT' not in dtype_str:
                dtype = 'INTEGER'
            elif 'BIGINT' in dtype_str or 'HUGEINT' in dtype_str:
                dtype = 'BIGINT'
            elif 'DOUBLE' in dtype_str or 'FLOAT' in dtype_str or 'DECIMAL' in dtype_str:
                dtype = 'DOUBLE'
            elif 'VARCHAR' in dtype_str or 'TEXT' in dtype_str or 'STRING' in dtype_str:
                dtype = 'STRING'
            elif 'TIMESTAMP' in dtype_str:
                dtype = 'TIMESTAMP'
            elif 'DATE' in dtype_str:
                dtype = 'DATE'
            elif 'BOOL' in dtype_str:
                dtype = 'BOOLEAN'
            else:
                dtype = 'STRING'  # Fallback

            schema_dict[col_name] = dtype

        return schema_dict
