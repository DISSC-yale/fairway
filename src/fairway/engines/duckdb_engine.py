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

    def ingest(self, input_path, output_path, format='csv', partition_by=None, metadata=None, naming_pattern=None, hive_partitioning=False, target_rows=None, schema=None, write_mode='overwrite', **kwargs):
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

    def _get_column_names_only(self, file_path, format, read_func):
        """Fast column name extraction (header only).

        Returns set of column names without reading all data rows.
        """
        try:
            # Use LIMIT 0 to get schema without reading data
            rel = self.con.sql(f"SELECT * FROM {read_func}('{file_path}') LIMIT 0")
            return set(rel.columns)
        except Exception:
            # Fallback: try with LIMIT 1 for formats that need at least one row
            try:
                rel = self.con.sql(f"SELECT * FROM {read_func}('{file_path}') LIMIT 1")
                return set(rel.columns)
            except Exception:
                return set()

    def _resolve_type_conflict(self, type_a, type_b):
        """Return broader type when conflict occurs.

        Type hierarchy: BOOLEAN < INTEGER < BIGINT < DOUBLE < STRING
        STRING always wins as it can represent any value.
        """
        TYPE_HIERARCHY = ['BOOLEAN', 'INTEGER', 'BIGINT', 'DOUBLE', 'STRING']

        # Normalize types
        type_a = type_a.upper() if type_a else 'STRING'
        type_b = type_b.upper() if type_b else 'STRING'

        # Get indices (unknown types default to STRING)
        idx_a = TYPE_HIERARCHY.index(type_a) if type_a in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1
        idx_b = TYPE_HIERARCHY.index(type_b) if type_b in TYPE_HIERARCHY else len(TYPE_HIERARCHY) - 1

        return TYPE_HIERARCHY[max(idx_a, idx_b)]

    def _map_duckdb_type(self, dtype_str):
        """Map DuckDB type string to Fairway standard type."""
        dtype_str = str(dtype_str).upper()

        if 'INT' in dtype_str and 'BIGINT' not in dtype_str:
            return 'INTEGER'
        elif 'BIGINT' in dtype_str or 'HUGEINT' in dtype_str:
            return 'BIGINT'
        elif 'DOUBLE' in dtype_str or 'FLOAT' in dtype_str or 'DECIMAL' in dtype_str:
            return 'DOUBLE'
        elif 'VARCHAR' in dtype_str or 'TEXT' in dtype_str or 'STRING' in dtype_str:
            return 'STRING'
        elif 'TIMESTAMP' in dtype_str:
            return 'TIMESTAMP'
        elif 'DATE' in dtype_str:
            return 'DATE'
        elif 'BOOL' in dtype_str:
            return 'BOOLEAN'
        else:
            return 'STRING'  # Fallback

    def infer_schema(self, path, format='csv', sampling_ratio=0.1, sample_files=50, rows_per_file=1000, **kwargs):
        """
        Infers schema from a dataset using DuckDB with two-phase approach.

        Two-Phase Approach:
            Phase 1: Column Discovery - Scan ALL files to get complete column set (fast, headers only)
            Phase 2: Type Inference - Sample files with coverage guarantee for accurate types

        Args:
            path: Input path (glob or directory)
            format: Input format (csv, tsv, json, parquet)
            sampling_ratio: Fraction of files to sample for type inference (0.0 to 1.0)
            sample_files: Max number of files to sample for type inference (default 50)
            rows_per_file: Rows to sample from each file for type inference (default 1000)
        Returns:
            dict: Schema dictionary compatible with Fairway config

        Note:
            - Phase 1 ensures ALL columns are captured (no missing columns)
            - Phase 2 ensures at least one file per unique column is sampled (coverage guarantee)
            - Deterministic: files are sorted before processing
        """
        import glob as glob_module

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

        # Discover all files (sorted for determinism)
        all_files = sorted(glob_module.glob(glob_pattern, recursive=True))
        if not all_files:
            raise ValueError(f"No files found matching pattern: {glob_pattern}")

        read_func = {'csv': 'read_csv_auto', 'json': 'read_json_auto', 'parquet': 'read_parquet'}[format]

        # ============================================================
        # PHASE 1: Column Discovery (scan ALL files, headers only)
        # ============================================================
        all_columns = set()
        column_sources = {}  # {column_name: [files_that_have_it]}
        errors = []

        print(f"INFO: Phase 1 - Discovering columns from {len(all_files)} files...")
        for f in all_files:
            try:
                cols = self._get_column_names_only(f, format, read_func)
                if cols:  # Skip empty results (empty files)
                    all_columns.update(cols)
                    for col in cols:
                        column_sources.setdefault(col, []).append(f)
            except Exception as e:
                errors.append((f, str(e)))
                continue

        if errors:
            print(f"WARNING: Failed to read {len(errors)} files during column discovery")

        if not all_columns:
            raise ValueError(f"No columns found in any files matching: {glob_pattern}")

        print(f"INFO: Phase 1 complete - Found {len(all_columns)} unique columns across all files")

        # ============================================================
        # PHASE 2: Type Inference with Coverage Guarantee
        # ============================================================
        # Step 2a: Ensure at least one file per column (minimum required set)
        required_files = set()
        columns_covered = set()

        for col in sorted(all_columns):  # Sorted for determinism
            if col not in columns_covered:
                # Pick first file that has this column
                source_file = column_sources[col][0]
                required_files.add(source_file)
                # This file covers all its columns
                file_cols = self._get_column_names_only(source_file, format, read_func)
                columns_covered.update(file_cols)

        # Step 2b: Add additional samples for type diversity (up to sample_files limit)
        remaining_budget = sample_files - len(required_files)
        if remaining_budget > 0:
            remaining_files = [f for f in all_files if f not in required_files]
            additional = remaining_files[:remaining_budget]  # Deterministic, not random
            sample = list(required_files) + additional
        else:
            sample = list(required_files)

        # Sort for deterministic processing order
        sample = sorted(sample)
        print(f"INFO: Phase 2 - Sampling {len(sample)} files to infer types for {len(all_columns)} columns")

        # Step 2c: Infer types from sampled files using UNION ALL BY NAME
        if len(sample) == 1:
            query = f"SELECT * FROM {read_func}('{sample[0]}') LIMIT {rows_per_file}"
        else:
            subqueries = [f"SELECT * FROM {read_func}('{f}') LIMIT {rows_per_file}" for f in sample]
            query = " UNION ALL BY NAME ".join(subqueries)

        try:
            rel = self.con.sql(query)

            # Extract types from the union result
            column_types = {}
            for col_name, col_type in zip(rel.columns, rel.types):
                column_types[col_name] = self._map_duckdb_type(col_type)
        except Exception as e:
            print(f"WARNING: Union query failed ({e}), falling back to per-file inference")
            # Fallback: infer types per-file and merge
            column_types = {}
            for f in sample:
                try:
                    rel = self.con.sql(f"SELECT * FROM {read_func}('{f}') LIMIT {rows_per_file}")
                    for col_name, col_type in zip(rel.columns, rel.types):
                        mapped_type = self._map_duckdb_type(col_type)
                        if col_name not in column_types:
                            column_types[col_name] = mapped_type
                        else:
                            column_types[col_name] = self._resolve_type_conflict(column_types[col_name], mapped_type)
                except Exception:
                    continue

        # ============================================================
        # Combine: ALL columns from Phase 1, types from Phase 2
        # ============================================================
        schema_dict = {}
        for col in sorted(all_columns):  # Sorted for deterministic output
            if col in column_types:
                schema_dict[col] = column_types[col]
            else:
                # Should rarely happen with coverage guarantee, but safety fallback
                print(f"WARNING: Column '{col}' has no type - defaulting to STRING")
                schema_dict[col] = 'STRING'

        print(f"INFO: Schema inference complete - {len(schema_dict)} columns")
        return schema_dict
