try:
    import duckdb
except ImportError as e:
    duckdb = None
    _duckdb_import_error = e

import os
import re
import logging

logger = logging.getLogger("fairway.engines.duckdb")


def _validate_sql_identifier(name):
    """Validate that a string is a safe SQL identifier to prevent injection."""
    if not name or not isinstance(name, str):
        raise ValueError(f"Invalid SQL identifier: {name}")
    # Allow only alphanumeric and underscores, must start with letter or underscore
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid SQL identifier (contains unsafe characters): {name}")
    if len(name) > 128:
        raise ValueError(f"SQL identifier too long: {name}")
    return name


def _escape_sql_string(value):
    """Escape a string value for safe inclusion in SQL."""
    if not isinstance(value, str):
        value = str(value)
    # Escape single quotes by doubling them
    return value.replace("'", "''")


def _sql_quote_str(s: str) -> str:
    """Return a SQL single-quoted string literal with internal quotes escaped."""
    return "'" + str(s).replace("'", "''") + "'"


class DuckDBEngine:
    @staticmethod
    def _format_path_sql(input_path):
        """Format input_path (str or list) as a safe DuckDB SQL expression."""
        if isinstance(input_path, list):
            escaped = [p.replace("'", "''") for p in input_path]
            return "[" + ", ".join(f"'{p}'" for p in escaped) + "]"
        else:
            escaped = input_path.replace("'", "''")
            return f"'{escaped}'"

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
        # Extract format-specific kwargs before stripping (needed by handlers)
        fixed_width_spec = kwargs.pop('fixed_width_spec', None)
        min_line_length = kwargs.pop('min_line_length', None)

        # Strip fairway-specific kwargs that shouldn't be passed to DuckDB read functions
        _fairway_keys = {'balanced', 'target_file_size_mb', 'compression',
                         'max_records_per_file', 'output_format', 'target_rows_per_file'}
        for key in _fairway_keys:
            kwargs.pop(key, None)

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
        elif format == 'fixed_width':
            if min_line_length is not None:
                kwargs['min_line_length'] = min_line_length
            return self._ingest_fixed_width(input_path, output_path, fixed_width_spec, partition_by, metadata, write_mode, **kwargs)
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
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(k)):
                raise ValueError(f"Invalid DuckDB option key: {k!r}")
            if isinstance(v, bool):
                val = str(v).lower() # true/false
            elif isinstance(v, str):
                escaped_v = v.replace("'", "''")
                val = f"'{escaped_v}'"
            elif isinstance(v, list):
                # list -> ['a', 'b']
                val = f"[{', '.join(_sql_quote_str(x) if isinstance(x, str) else str(x) for x in v)}]"
            else:
                val = v
            options.append(f"{k}={val}")
        
        options_str = ", ".join(options)
        if options_str:
            options_str = ", " + options_str

        # Handle list of file paths (from partition-aware batching) or string
        if isinstance(input_path, list):
            read_path = input_path
        elif '*' not in input_path and os.path.isdir(input_path):
            read_path = os.path.join(input_path, "**/*.csv")
        else:
            read_path = input_path

        path_sql = self._format_path_sql(read_path)
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_csv_auto({path_sql}{options_str})")
        
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _ingest_json(self, input_path, output_path, partition_by=None, metadata=None, write_mode='overwrite', **kwargs):
        """
        Converts JSON to Parquet.
        """
        path_sql = self._format_path_sql(input_path)
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_json_auto({path_sql})")
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _ingest_parquet(self, input_path, output_path, partition_by=None, metadata=None, write_mode='overwrite', **kwargs):
        """
        Pass-through Parquet ingestion (useful for unifying pipeline logic).
        """
        path_sql = self._format_path_sql(input_path)
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_parquet({path_sql})")
        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _ingest_fixed_width(self, input_path, output_path, spec_path, partition_by=None, metadata=None, write_mode='overwrite', **kwargs):
        """
        Converts fixed-width text files to Parquet using DuckDB.

        Fixed-width files have columns at specific character positions (no delimiters).
        A spec file defines column names, start positions, lengths, and types.

        Args:
            input_path: Path to fixed-width data file(s)
            output_path: Output Parquet path
            spec_path: Path to YAML spec file defining columns
            partition_by: Optional partition columns
            metadata: Optional metadata to inject
            write_mode: 'overwrite' or 'append'
        """
        from fairway.fixed_width import load_spec

        if not spec_path:
            raise ValueError("Fixed-width format requires 'fixed_width_spec' path")

        # Load and validate spec
        spec = load_spec(spec_path)

        # Allow config-level min_line_length to override spec value
        config_min_line_length = kwargs.pop('min_line_length', None)
        if config_min_line_length is not None:
            spec['min_line_length'] = config_min_line_length

        columns = spec['columns']
        line_length = spec['line_length']

        input_desc = f"{len(input_path)} files" if isinstance(input_path, list) else input_path
        logger.info("DuckDB reading fixed-width file from %s", input_desc)
        logger.info("Spec defines %d columns, expected line length: %d", len(columns), line_length)

        # Handle glob patterns and list input (multi-archive)
        if isinstance(input_path, list):
            read_path = input_path
        elif '*' not in input_path and os.path.isdir(input_path):
            read_path = os.path.join(input_path, "**/*.txt")
        else:
            read_path = input_path

        # Step 1: Read file as single text column (no header, no delimiter parsing)
        # Use a delimiter that won't appear in fixed-width data (ASCII unit separator)
        path_sql = self._format_path_sql(read_path)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW raw_lines_unfiltered AS
            SELECT column0 AS line
            FROM read_csv({path_sql}, header=false, sep=E'\\x1F', columns={{'column0': 'VARCHAR'}})
        """)

        # Step 1b: Filter by record type if specified (for hierarchical fixed-width files)
        # When filtering, materialize to TEMP TABLE to avoid re-evaluating filter on every access
        record_filter = spec.get('record_type_filter')
        if record_filter:
            pos = int(record_filter['position']) + 1  # DuckDB substr is 1-indexed
            length = int(record_filter['length'])
            value = record_filter['value']
            logger.info("Filtering to record_type='%s' at position %d (length %d)", value, record_filter['position'], length)
            # Use TEMP TABLE to materialize filtered rows (filter applied once, not on every access)
            self.con.execute("DROP TABLE IF EXISTS raw_lines")
            safe_value = value.replace("'", "''")
            self.con.execute(f"""
                CREATE TEMP TABLE raw_lines AS
                SELECT line FROM raw_lines_unfiltered
                WHERE substr(line, {pos}, {length}) = '{safe_value}'
            """)
        else:
            self.con.execute("CREATE OR REPLACE TEMP VIEW raw_lines AS SELECT line FROM raw_lines_unfiltered")

        # Step 1c: Filter out short/corrupted lines if min_line_length specified
        min_line_length = spec.get('min_line_length')
        if min_line_length:
            logger.info("Filtering lines shorter than min_line_length=%d", min_line_length)
            # Re-create raw_lines with additional length filter
            # raw_lines could be a TABLE (with record_type_filter) or VIEW (without)
            self.con.execute("DROP TABLE IF EXISTS raw_lines_filtered")
            self.con.execute(f"""
                CREATE TEMP TABLE raw_lines_filtered AS
                SELECT line FROM raw_lines
                WHERE length(line) >= {min_line_length}
            """)
            self.con.execute("DROP VIEW IF EXISTS raw_lines")
            self.con.execute("DROP TABLE IF EXISTS raw_lines")
            self.con.execute("ALTER TABLE raw_lines_filtered RENAME TO raw_lines")

        # Step 2: Validate line lengths (fail strict per RULE-115)
        validation_query = f"""
            SELECT COUNT(*) as short_lines
            FROM raw_lines
            WHERE length(line) < {line_length}
        """
        result = self.con.execute(validation_query).fetchone()
        short_lines = result[0] if result else 0

        if short_lines > 0:
            # Get sample of short lines for error message
            sample = self.con.execute(f"""
                SELECT line, length(line) as len
                FROM raw_lines
                WHERE length(line) < {line_length}
                LIMIT 3
            """).fetchall()
            sample_str = "; ".join([f"len={s[1]}: '{s[0][:50]}...'" for s in sample])
            raise ValueError(
                f"[RULE-115] Data Integrity Error: {short_lines} lines are shorter than expected "
                f"line length {line_length}. Samples: {sample_str}"
            )

        # Step 3: Build column extraction query using substr
        # DuckDB substr is 1-indexed: substr(string, start, length)
        select_parts = []
        # Allowed SQL types for fixed-width columns
        ALLOWED_TYPES = {'VARCHAR', 'STRING', 'INTEGER', 'INT', 'BIGINT', 'DOUBLE', 'FLOAT', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'DECIMAL'}
        for col in columns:
            name = _validate_sql_identifier(col['name'])
            start = int(col['start']) + 1  # Convert 0-indexed to 1-indexed
            length = int(col['length'])
            col_type = col['type'].upper()
            # Validate column type against allowlist
            base_type = col_type.split('(')[0]  # Handle DECIMAL(10,2) etc
            if base_type not in ALLOWED_TYPES:
                raise ValueError(f"Invalid column type: {col_type}")
            trim = col.get('trim', False)

            # Extract substring
            extract_expr = f"substr(line, {start}, {length})"

            # Apply trim if requested
            if trim:
                extract_expr = f"trim({extract_expr})"

            # Store as VARCHAR — casting happens in enforce_types() (curated layer)
            select_parts.append(f"{extract_expr} AS {name}")

        select_clause = ", ".join(select_parts)

        # Step 4: Create view with extracted columns
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW raw_data AS
            SELECT {select_clause}
            FROM raw_lines
        """)

        return self._write_to_parquet(output_path, partition_by, metadata, write_mode)

    def _write_to_parquet(self, output_path, partition_by=None, metadata=None, write_mode='overwrite'):
        # Inject metadata columns if provided
        select_clause = "*"
        if metadata:
            # Validate metadata keys as SQL identifiers and escape values
            meta_parts = []
            for key, val in metadata.items():
                safe_key = _validate_sql_identifier(key)
                safe_val = _escape_sql_string(val)
                meta_parts.append(f"'{safe_val}' AS {safe_key}")
            meta_cols = ", ".join(meta_parts)
            select_clause = f"*, {meta_cols}"

        # Write to Parquet with optional Hive partitioning
        partition_clause = ""
        if partition_by:
            # Validate partition column names
            safe_partitions = [_validate_sql_identifier(p) for p in partition_by]
            partition_clause = f", PARTITION_BY ({', '.join(safe_partitions)})"
            
        # Determine overwrite behavior
        # DuckDB's COPY ... (OVERWRITE TRUE) replaces the directory/file
        # If write_mode='append', we should NOT use OVERWRITE TRUE.
        # However, DuckDB 0.9.x/1.0 COPY to parquet directory behavior works as append if OVERWRITE is not specified?
        # Let's verify standard DuckDB behavior:
        # COPY ... TO 'dir' (FORMAT PARQUET, PARTITION_BY ...) -> Writes new files into dir.
        overwrite_val = "TRUE" if write_mode == 'overwrite' else "FALSE"

        escaped_output = output_path.replace("'", "''")
        self.con.execute(f"""
            COPY (SELECT {select_clause} FROM raw_data)
            TO '{escaped_output}'
            (FORMAT PARQUET{partition_clause}, OVERWRITE {overwrite_val})
        """)
        return True

    def enforce_types(self, input_path, output_path, columns, on_fail='null',
                      partition_by=None, write_mode='overwrite'):
        """
        Reads the STRING-preserved processed layer and writes a typed curated layer.

        For each column declared with a non-string type:
          - on_fail='null'   → TRY_CAST: non-castable values become NULL (default)
          - on_fail='strict' → CAST: non-castable values raise an error

        Per-column cast_mode='strict' overrides on_fail='null' for that column.

        Args:
            input_path:  Path to the processed (all-STRING) Parquet.
            output_path: Path to write the typed curated Parquet.
            columns:     List of column dicts with 'name', 'type', 'cast_mode' keys
                         (from fixed_width_spec or built from schema config).
            on_fail:     Global default: 'null' or 'strict'.
            partition_by: Optional list of partition column names.
            write_mode:  'overwrite' or 'append'.
        """
        ALLOWED_TYPES = {
            'VARCHAR', 'STRING', 'INTEGER', 'INT', 'BIGINT', 'DOUBLE', 'FLOAT',
            'DATE', 'TIMESTAMP', 'BOOLEAN', 'DECIMAL',
        }

        # Resolve the parquet source (file or directory glob)
        if os.path.isdir(input_path):
            parquet_source = os.path.join(input_path, '**', '*.parquet').replace('\\', '/')
        else:
            parquet_source = input_path

        select_parts = []
        for col in columns:
            name = _validate_sql_identifier(col['name'])
            col_type = col['type'].upper()
            # Validate full col_type: allow TYPE, TYPE(N), TYPE(N,M), TYPE(N,M,K) only
            if not re.match(r'^[A-Z][A-Z0-9_ ]*(\(\d+(?:,\d+)*\))?$', col_type):
                raise ValueError(f"Unsafe column type rejected: {col_type!r}")
            base_type = col_type.split('(')[0]
            if base_type not in ALLOWED_TYPES:
                raise ValueError(f"enforce_types: invalid column type '{col_type}'")

            col_cast_mode = col.get('cast_mode', 'adaptive')
            effective_strict = (on_fail == 'strict') or (col_cast_mode == 'strict')

            if base_type in ('VARCHAR', 'STRING'):
                select_parts.append(name)
            elif effective_strict:
                select_parts.append(f"CAST({name} AS {col_type}) AS {name}")
            else:
                select_parts.append(f"TRY_CAST({name} AS {col_type}) AS {name}")

        select_clause = ', '.join(select_parts)

        parquet_sql = self._format_path_sql(parquet_source)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW raw_data AS
            SELECT {select_clause}
            FROM read_parquet({parquet_sql}, union_by_name=true)
        """)

        return self._write_to_parquet(output_path, partition_by=partition_by,
                                      write_mode=write_mode)

    def read_result(self, path):
        """
        Reads a Parquet result from the given path into a DuckDB Relation (Lazy).
        """
        escaped_path = path.replace("'", "''")
        if os.path.isfile(path):
            return self.con.sql(f"SELECT * FROM '{escaped_path}'")
        return self.con.sql(f"SELECT * FROM '{escaped_path}/**/*.parquet'")

    def _get_column_names_only(self, file_path, format, read_func):
        """Fast column name extraction (header only).

        Returns set of column names without reading all data rows.
        """
        try:
            # Use LIMIT 0 to get schema without reading data
            rel = self.con.sql(f"SELECT * FROM {read_func}({self._format_path_sql(file_path)}) LIMIT 0")
            return set(rel.columns)
        except Exception:
            # Fallback: try with LIMIT 1 for formats that need at least one row
            try:
                rel = self.con.sql(f"SELECT * FROM {read_func}({self._format_path_sql(file_path)}) LIMIT 1")
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

        rows_per_file = int(rows_per_file)

        # Normalize TSV/tab to CSV
        if format in ('tsv', 'tab'):
            format = 'csv'

        logger.info("Inferring schema from %s (format=%s)", path, format)

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

        logger.info("Phase 1 - Discovering columns from %d files...", len(all_files))
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
            logger.warning("Failed to read %d files during column discovery", len(errors))

        if not all_columns:
            raise ValueError(f"No columns found in any files matching: {glob_pattern}")

        logger.info("Phase 1 complete - Found %d unique columns across all files", len(all_columns))

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
        logger.info("Phase 2 - Sampling %d files to infer types for %d columns", len(sample), len(all_columns))

        # Step 2c: Infer types from sampled files using UNION ALL BY NAME
        if len(sample) == 1:
            query = f"SELECT * FROM {read_func}({self._format_path_sql(sample[0])}) LIMIT {rows_per_file}"
        else:
            subqueries = [f"SELECT * FROM {read_func}({self._format_path_sql(f)}) LIMIT {rows_per_file}" for f in sample]
            query = " UNION ALL BY NAME ".join(subqueries)

        try:
            rel = self.con.sql(query)

            # Extract types from the union result
            column_types = {}
            for col_name, col_type in zip(rel.columns, rel.types):
                column_types[col_name] = self._map_duckdb_type(col_type)
        except Exception as e:
            logger.warning("Union query failed (%s), falling back to per-file inference", e)
            # Fallback: infer types per-file and merge
            column_types = {}
            for f in sample:
                try:
                    rel = self.con.sql(f"SELECT * FROM {read_func}({self._format_path_sql(f)}) LIMIT {rows_per_file}")
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
                logger.warning("Column '%s' has no type - defaulting to STRING", col)
                schema_dict[col] = 'STRING'

        logger.info("Schema inference complete - %d columns", len(schema_dict))
        return schema_dict
