import yaml
import os
import glob
import re


class ConfigValidationError(Exception):
    """Raised when config validation fails."""
    def __init__(self, errors):
        self.errors = errors
        message = "Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)


class Config:
    def __init__(self, config_path):
        self.config_path = config_path # Store for relative path resolution
        with open(config_path, 'r') as f:
            self.data = yaml.safe_load(f)
        
        self.dataset_name = self.data.get('dataset_name')
        
        # Engine Validation
        self.engine = self.data.get('engine', 'duckdb')
        valid_engines = {'duckdb', 'pyspark'}
        if self.engine not in valid_engines:
            raise ValueError(f"Invalid engine: '{self.engine}'. Must be one of {valid_engines}")

        self.storage = self.data.get('storage', {})
        
        # Support both root-level 'tables' and 'data: tables' structures
        raw_tables = self.data.get('tables')
        if not raw_tables:
             raw_tables = self.data.get('data', {}).get('tables', [])

        self.tables = self._expand_tables(raw_tables)
        self._validate()  # Fail fast on config errors
        self.validations = self.data.get('validations', {})
        self.enrichment = self.data.get('enrichment', {})
        self.partition_by = self.data.get('partition_by', [])
        # Temporary location for global file writes
        # Priority: Env Var > Root Config > Storage Config
        self.temp_location = os.environ.get('FAIRWAY_TEMP') or \
                             self.data.get('temp_location') or \
                             self.storage.get('temp_location')
        self.redivis = self.data.get('redivis', {})
        self.output_format = self.storage.get('format', 'parquet').lower()
        
        # Performance/Optimizations
        performance = self.data.get('performance', {})
        self.target_rows = performance.get('target_rows') or self.data.get('target_rows', 500000)



    def _load_schema(self, schema_ref):
        """Loads schema from a file if schema_ref is a path string, otherwise returns it as-is."""
        if isinstance(schema_ref, str):
            # Assume it's a file path
            if not os.path.exists(schema_ref):
                # Try relative to config file if not absolute
                # Note: This is simplified, might need config dir context
                raise FileNotFoundError(f"Schema file not found: {schema_ref}")
            
            with open(schema_ref, 'r') as f:
                data = None
                if schema_ref.endswith('.yaml') or schema_ref.endswith('.yml'):
                    data = yaml.safe_load(f)
                elif schema_ref.endswith('.json'):
                    import json
                    data = json.load(f)
                else:
                    raise ValueError(f"Unsupported schema file format: {schema_ref}")
                
                # Handle generate-schema output format
                if isinstance(data, dict) and 'columns' in data:
                    return data['columns']
                return data
        return schema_ref or {}

    def _expand_tables(self, raw_tables):
        """Discovers files and extracts metadata via regex if patterns are provided."""
        expanded = []
        # Base config directory for relative path resolution
        config_dir = os.path.dirname(os.path.abspath(self.config_path)) if hasattr(self, 'config_path') else os.getcwd()

        for tbl in raw_tables:
            path_pattern = tbl.get('path_pattern') or tbl.get('path')
            naming_pattern = tbl.get('naming_pattern')
            hive_partitioning = tbl.get('hive_partitioning', False)
            # New archive handling keys
            archives_pattern = tbl.get('archives')
            files_pattern = tbl.get('files')

            # Skip if neither path nor archives specified
            if not path_pattern and not archives_pattern:
                continue

            # Resolve path relative to config file if it's not absolute
            # SKIP if 'root' is defined (let pipeline handle it)
            if not os.path.isabs(path_pattern) and not tbl.get('root'):
                # Try relative to config dir first
                resolved_path = os.path.join(config_dir, path_pattern)
                # If that doesn't exist, maybe it is relative to CWD?
                # But prioritizing config_dir is safer for reproducibility.
                if not os.path.exists(resolved_path) and not glob.glob(resolved_path):
                     # If not found relative to config, try CWD (backward compatibility)
                     if os.path.exists(path_pattern) or glob.glob(path_pattern):
                         resolved_path = path_pattern
            else:
                resolved_path = path_pattern

            raw_schema = tbl.get('schema')
            resolved_schema = self._load_schema(raw_schema)

            # Glob expansion behavior:
            # - expand_glob=False (default): glob pattern → one table entry (standard behavior)
            # - expand_glob=True: each matching file → separate table entry (for per-file metadata extraction)
            expand_glob = tbl.get('expand_glob', False)

            if hive_partitioning or not expand_glob:
                 # Treat the directory/glob as a single table unit
                 # Logic is shared with Hive partitioning branch but without the hive flag if just expand_glob=False

                 # Check existence only if literal path (not a glob with wildcards, unless checking dir)
                 if '*' not in resolved_path and not os.path.exists(resolved_path):
                      print(f"WARNING: Table path not found: {resolved_path}")
                      continue

                 table_format = tbl.get('format', 'csv')
                 valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet'}
                 if table_format not in valid_formats:
                      raise ValueError(f"Invalid format: '{table_format}'. Must be one of {valid_formats}")

                 # Name is explicit or basename of path (which might be a pattern like *.csv)
                 # If pattern, basename might be '*.csv'. Ideally user provides name.
                 table_name = tbl.get('name', os.path.basename(resolved_path))

                 expanded.append({
                    'name': table_name,
                    'path': resolved_path,
                    'format': table_format,
                    'metadata': {},
                    'naming_pattern': naming_pattern,
                    'schema': resolved_schema,
                    'transformation': tbl.get('transformation'),
                    'hive_partitioning': hive_partitioning,
                    'partition_by': tbl.get('partition_by', []),
                    'read_options': tbl.get('read_options', {}),
                    'preprocess': tbl.get('preprocess', {}),
                    'write_mode': tbl.get('write_mode', 'overwrite'),
                    'root': tbl.get('root'),
                    'archives': archives_pattern,
                    'files': files_pattern
                })

            else:
                # Glob discovery (Eager expansion)
                search_path = resolved_path
                table_root = tbl.get('root')

                if table_root:
                    # Resolve root relative to config dir if needed
                    if not os.path.isabs(table_root):
                         table_root = os.path.join(config_dir, table_root)

                    # Fix: os.path.join ignores root if path_pattern starts with /
                    # We strip leading slash to ensure join works
                    rel_pattern = path_pattern.lstrip(os.sep)
                    search_path = os.path.join(table_root, rel_pattern)

                print(f"DEBUG: ConfigLoader globbing: {search_path}")
                files = glob.glob(search_path, recursive=True)
                if not files:
                    print(f"WARNING: ConfigLoader found no files for pattern: {search_path}")

                for f in files:
                    metadata = {}
                    if naming_pattern:
                        match = re.search(naming_pattern, os.path.basename(f))
                        if match:
                            metadata = match.groupdict()

                    # Create a specific table entry for each file
                    table_format = tbl.get('format', 'csv')
                    valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet'}
                    if table_format not in valid_formats:
                         raise ValueError(f"Invalid format: '{table_format}'. Must be one of {valid_formats}")


                    execution_mode = tbl.get('preprocess', {}).get('execution_mode', 'driver')
                    if execution_mode == 'cluster' and self.engine not in ['pyspark', 'spark']:
                         raise ValueError(f"Configuration Error: 'execution_mode: cluster' is only available with 'engine: pyspark'. Table: {tbl.get('name')}")

                    # Determine table root for relocatability
                    table_root_raw = tbl.get('root')
                    table_root = None
                    if table_root_raw:
                        if not os.path.isabs(table_root_raw):
                             table_root = os.path.join(config_dir, table_root_raw)
                        else:
                             table_root = table_root_raw

                    expanded.append({
                        'name': tbl.get('name', os.path.basename(f)),
                        'path': f,
                        'root': table_root,
                        'format': table_format,
                        'metadata': metadata,
                        'naming_pattern': naming_pattern,
                        'schema': resolved_schema,
                        'transformation': tbl.get('transformation'),
                        'hive_partitioning': False,
                        'partition_by': tbl.get('partition_by', []),
                        'read_options': tbl.get('read_options', {}),
                        'preprocess': tbl.get('preprocess', {}),
                        'write_mode': tbl.get('write_mode', 'overwrite'),
                        'archives': archives_pattern,
                        'files': files_pattern
                    })
        return expanded

    def _validate(self):
        """Validate config and fail fast on errors."""
        errors = []
        warnings = []
        config_dir = os.path.dirname(os.path.abspath(self.config_path)) if hasattr(self, 'config_path') else os.getcwd()

        # Check for empty tables
        if not self.tables:
            # This is a warning, not an error - user might be testing config
            warnings.append("No tables defined in config")

        table_names = set()
        for i, table in enumerate(self.tables):
            prefix = f"tables[{i}]"

            # Required: name
            name = table.get('name')
            if not name:
                errors.append(f"{prefix}: Missing required 'name' field")
            elif name in table_names:
                errors.append(f"{prefix}: Duplicate table name '{name}'")
            else:
                table_names.add(name)
                prefix = f"tables['{name}']"

            # Required: path OR archives (but not both)
            path = table.get('path')
            archives = table.get('archives')
            files = table.get('files')

            if path and archives:
                errors.append(f"{prefix}: Cannot specify both 'path' and 'archives'")
            elif not path and not archives:
                errors.append(f"{prefix}: Missing required 'path' or 'archives' field")

            # archives requires files and vice versa
            if archives and not files:
                errors.append(f"{prefix}: 'archives' specified without 'files' pattern")
            if files and not archives:
                errors.append(f"{prefix}: 'files' specified without 'archives' pattern")

            # Valid format
            fmt = table.get('format', 'csv')
            valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet'}
            if fmt not in valid_formats:
                errors.append(f"{prefix}: Invalid format '{fmt}'. Must be one of {valid_formats}")

            # Schema file exists (if specified as path)
            schema = table.get('schema')
            if isinstance(schema, str) and not os.path.exists(schema):
                # Try relative to config dir
                schema_path = os.path.join(config_dir, schema) if not os.path.isabs(schema) else schema
                if not os.path.exists(schema_path):
                    errors.append(f"{prefix}: Schema file not found: {schema}")

            # Root directory exists (if specified)
            root = table.get('root')
            if root:
                root_path = os.path.join(config_dir, root) if not os.path.isabs(root) else root
                if not os.path.isdir(root_path):
                    errors.append(f"{prefix}: Root directory does not exist: {root}")

            # Glob pattern must match at least one file
            if path and '*' in path:
                root = table.get('root')
                if root:
                    root_path = os.path.join(config_dir, root) if not os.path.isabs(root) else root
                    search_path = os.path.join(root_path, path.lstrip(os.sep))
                else:
                    search_path = os.path.join(config_dir, path) if not os.path.isabs(path) else path
                matched_files = glob.glob(search_path, recursive=True)
                if not matched_files:
                    errors.append(f"{prefix}: Glob pattern matches no files: {search_path}")

        # Print warnings
        for w in warnings:
            print(f"WARNING: {w}")

        # Raise on errors
        if errors:
            raise ConfigValidationError(errors)

    def get_table_by_name(self, name):
        for table in self.tables:
            if table['name'] == name:
                return table
        return None
