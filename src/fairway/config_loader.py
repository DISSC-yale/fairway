import yaml
import os
import glob
import re
import logging

logger = logging.getLogger("fairway.config")

# Valid SQL/column identifier pattern (RULE-119: prevent injection)
# Must start with letter or underscore, followed by letters, digits, or underscores
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


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

        # Medallion directory layout: raw / processed / curated
        self.output_root = self.storage.get('root', 'data')
        self.raw_dir = self.storage.get('raw', os.path.join(self.output_root, 'raw'))
        self.processed_dir = self.storage.get('processed', os.path.join(self.output_root, 'processed'))
        self.curated_dir = self.storage.get('curated', os.path.join(self.output_root, 'curated'))

        # Unified temp directory: FAIRWAY_TEMP env > storage.temp > storage.scratch_dir
        # scratch_dir is documented for HPC fast-scratch storage; treat as fallback for temp
        temp_raw = os.environ.get('FAIRWAY_TEMP') or self.storage.get('temp') or self.storage.get('scratch_dir')
        self.temp_dir = os.path.expandvars(temp_raw) if temp_raw else None

        # Support both root-level 'tables' and 'data: tables' structures
        raw_tables = self.data.get('tables')
        if not raw_tables:
             raw_tables = self.data.get('data', {}).get('tables', [])

        self.tables = self._expand_tables(raw_tables)
        self._validate()  # Fail fast on config errors
        self.validations = self.data.get('validations', {})
        self.enrichment = self.data.get('enrichment', {})
        self.partition_by = self.data.get('partition_by', [])
        self.redivis = self.data.get('redivis', {})
        self.output_format = self.storage.get('format', 'parquet').lower()

        # Performance/Optimizations
        performance = self.data.get('performance', {})
        self.target_rows = performance.get('target_rows') or self.data.get('target_rows', 500000)
        self.salting = performance.get('salting', False)  # D.1: Disabled by default
        self.target_file_size_mb = performance.get('target_file_size_mb', 128)  # D.2: Target ~128MB files
        self.compression = performance.get('compression', 'snappy')  # D.2: Default compression
        # Direct control over max records per file (overrides target_file_size_mb heuristic)
        self.max_records_per_file = performance.get('max_records_per_file')

        # Container settings
        container = self.data.get('container', {})
        self.apptainer_binds = container.get('apptainer_binds')



    def _resolve_path(self, path_ref, config_dir):
        """Resolve a relative path, checking CWD (project root) before config directory."""
        if not path_ref:
            return None
        if os.path.isabs(path_ref):
            return path_ref
        cwd_path = os.path.join(os.getcwd(), path_ref)
        if os.path.exists(cwd_path):
            return cwd_path
        cfg_path = os.path.join(config_dir, path_ref)
        if os.path.exists(cfg_path):
            return cfg_path
        return cwd_path

    def _resolve_script_path(self, script_path, config_dir):
        """Resolve a preprocessing script path, searching CWD then config_dir."""
        if os.path.isabs(script_path):
            return script_path
        cwd = os.getcwd()
        candidates = [
            os.path.join(cwd, script_path),
            os.path.join(cwd, 'scripts', os.path.basename(script_path)),
            os.path.join(cwd, 'src', 'preprocess', os.path.basename(script_path)),
            os.path.join(config_dir, script_path),
            os.path.join(config_dir, 'scripts', os.path.basename(script_path)),
            os.path.join(config_dir, 'src', 'preprocess', os.path.basename(script_path)),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return os.path.join(cwd, script_path)

    def _load_schema(self, schema_ref):
        """Loads schema from a file if schema_ref is a path string, otherwise returns it as-is."""
        if isinstance(schema_ref, str):
            # Assume it's a file path
            schema_path = schema_ref
            if not os.path.exists(schema_path):
                # Try relative to config file directory
                config_dir = os.path.dirname(os.path.abspath(self.config_path))
                schema_path = os.path.join(config_dir, schema_ref)
                if not os.path.exists(schema_path):
                    raise FileNotFoundError(f"Schema file not found: {schema_ref}")
            
            with open(schema_path, 'r') as f:
                data = None
                if schema_path.endswith('.yaml') or schema_path.endswith('.yml'):
                    data = yaml.safe_load(f)
                elif schema_path.endswith('.json'):
                    import json
                    data = json.load(f)
                else:
                    raise ValueError(f"Unsupported schema file format: {schema_path}")
                
                # Handle generate-schema output format
                if isinstance(data, dict) and 'columns' in data:
                    return data['columns']
                return data
        return schema_ref or {}

    def _resolve_table_validations(self, tbl):
        """Merge per-table validations with global defaults (shallow merge)."""
        global_validations = self.data.get('validations', {})
        table_validations = tbl.get('validations', {})
        if not global_validations and not table_validations:
            return {}
        merged = dict(global_validations)
        merged.update(table_validations)  # shallow: per-table key replaces global key
        return merged

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
                      logger.warning("Table path not found: %s", resolved_path)
                      continue

                 table_format = tbl.get('format', 'csv')
                 valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet', 'fixed_width'}
                 if table_format not in valid_formats:
                      raise ValueError(f"Invalid format: '{table_format}'. Must be one of {valid_formats}")

                 # Name is explicit or basename of path (which might be a pattern like *.csv)
                 # If pattern, basename might be '*.csv'. Ideally user provides name.
                 table_name = tbl.get('name', os.path.basename(resolved_path))

                 # Resolve fixed_width_spec relative to config dir (consistent with other path resolution)
                 fixed_width_spec = self._resolve_path(tbl.get('fixed_width_spec'), config_dir)
                 transformation = self._resolve_path(tbl.get('transformation'), config_dir)

                 # Resolve preprocess.action: CWD first, then config_dir
                 preprocess = tbl.get('preprocess', {}).copy() if tbl.get('preprocess') else {}
                 if preprocess.get('action', '').endswith('.py'):
                     preprocess['action'] = self._resolve_script_path(preprocess['action'], config_dir)

                 expanded.append({
                    'name': table_name,
                    'path': resolved_path,
                    'format': table_format,
                    'metadata': {},
                    'naming_pattern': naming_pattern,
                    'schema': resolved_schema,
                    'transformation': transformation,
                    'hive_partitioning': hive_partitioning,
                    'partition_by': tbl.get('partition_by', []),
                    'read_options': tbl.get('read_options', {}),
                    'preprocess': preprocess,
                    'write_mode': tbl.get('write_mode', 'overwrite'),
                    'root': tbl.get('root'),
                    'archives': archives_pattern,
                    'files': files_pattern,
                    'fixed_width_spec': fixed_width_spec,
                    'batch_strategy': tbl.get('batch_strategy', 'bulk'),
                    'type_enforcement': tbl.get('type_enforcement', {}),
                    'validations': self._resolve_table_validations(tbl),
                    'output_layer': tbl.get('output_layer', 'curated'),
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

                logger.debug("ConfigLoader globbing: %s", search_path)
                files = glob.glob(search_path, recursive=True)
                if not files:
                    logger.warning("ConfigLoader found no files for pattern: %s", search_path)

                for f in files:
                    metadata = {}
                    if naming_pattern:
                        match = re.search(naming_pattern, os.path.basename(f))
                        if match:
                            metadata = match.groupdict()

                    # Create a specific table entry for each file
                    table_format = tbl.get('format', 'csv')
                    valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet', 'fixed_width'}
                    if table_format not in valid_formats:
                         raise ValueError(f"Invalid format: '{table_format}'. Must be one of {valid_formats}")


                    execution_mode = tbl.get('preprocess', {}).get('execution_mode', 'driver')
                    if execution_mode == 'cluster' and self.engine not in ['pyspark', 'spark']:
                         raise ValueError(f"Configuration Error: 'execution_mode: cluster' is only available with 'engine: pyspark'. Table: {tbl.get('name')}")

                    # Resolve file paths relative to config dir
                    table_root = self._resolve_path(tbl.get('root'), config_dir)
                    fixed_width_spec = self._resolve_path(tbl.get('fixed_width_spec'), config_dir)
                    transformation = self._resolve_path(tbl.get('transformation'), config_dir)

                    # Resolve preprocess.action: CWD first, then config_dir
                    preprocess = tbl.get('preprocess', {}).copy() if tbl.get('preprocess') else {}
                    if preprocess.get('action', '').endswith('.py'):
                        preprocess['action'] = self._resolve_script_path(preprocess['action'], config_dir)

                    expanded.append({
                        'name': tbl.get('name', os.path.basename(f)),
                        'path': f,
                        'root': table_root,
                        'format': table_format,
                        'metadata': metadata,
                        'naming_pattern': naming_pattern,
                        'schema': resolved_schema,
                        'transformation': transformation,
                        'hive_partitioning': False,
                        'partition_by': tbl.get('partition_by', []),
                        'read_options': tbl.get('read_options', {}),
                        'preprocess': preprocess,
                        'write_mode': tbl.get('write_mode', 'overwrite'),
                        'archives': archives_pattern,
                        'files': files_pattern,
                        'fixed_width_spec': fixed_width_spec,
                        'batch_strategy': tbl.get('batch_strategy', 'bulk'),
                        'type_enforcement': tbl.get('type_enforcement', {}),
                        'validations': self._resolve_table_validations(tbl),
                        'output_layer': tbl.get('output_layer', 'curated'),
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
                # RULE-119: Validate table name is a safe identifier
                if not VALID_IDENTIFIER.match(name):
                    errors.append(f"{prefix}: Invalid table name '{name}' - must be valid identifier (letters, digits, underscores, starting with letter/underscore)")
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
            valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet', 'fixed_width'}
            if fmt not in valid_formats:
                errors.append(f"{prefix}: Invalid format '{fmt}'. Must be one of {valid_formats}")

            # Fixed-width format requires spec file
            if fmt == 'fixed_width':
                fixed_width_spec = table.get('fixed_width_spec')
                if not fixed_width_spec:
                    errors.append(f"{prefix}: format 'fixed_width' requires 'fixed_width_spec' path")
                else:
                    # Defer spec file existence check if preprocessing is configured
                    # (preprocessing may generate the spec file)
                    has_preprocessing = table.get('archives') or table.get('preprocess')
                    if not has_preprocessing:
                        spec_path = os.path.join(config_dir, fixed_width_spec) if not os.path.isabs(fixed_width_spec) else fixed_width_spec
                        if not os.path.exists(spec_path):
                            errors.append(f"{prefix}: fixed_width_spec file not found: {spec_path}")

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

            # batch_strategy validation
            batch_strategy = table.get('batch_strategy', 'bulk')
            valid_strategies = {'bulk', 'partition_aware'}
            if batch_strategy not in valid_strategies:
                errors.append(f"{prefix}: Invalid batch_strategy '{batch_strategy}'. Must be one of {valid_strategies}")

            if batch_strategy == 'partition_aware':
                naming_pattern = table.get('naming_pattern')
                partition_by = table.get('partition_by', [])

                if not naming_pattern:
                    errors.append(f"{prefix}: batch_strategy 'partition_aware' requires 'naming_pattern'")
                if not partition_by:
                    errors.append(f"{prefix}: batch_strategy 'partition_aware' requires 'partition_by'")

                # Verify all partition_by keys appear as named groups in naming_pattern
                if naming_pattern and partition_by:
                    regex_groups = set(re.findall(r'\?P<([^>]+)>', naming_pattern))
                    for key in partition_by:
                        if key not in regex_groups:
                            errors.append(
                                f"{prefix}: partition_by key '{key}' not found as named group in naming_pattern"
                            )

            # output_layer validation
            output_layer = table.get('output_layer', 'curated')
            valid_layers = {'processed', 'curated'}
            if output_layer not in valid_layers:
                errors.append(f"{prefix}: Invalid output_layer '{output_layer}'. Must be one of {valid_layers}")
            if output_layer == 'processed' and table.get('transformation'):
                errors.append(
                    f"{prefix}: output_layer 'processed' cannot be combined with transformation "
                    f"(transformations only run for output_layer 'curated')"
                )

            # Validate preprocess.resources
            preprocess = table.get('preprocess', {})
            resources = preprocess.get('resources', {}) if isinstance(preprocess, dict) else {}
            if resources:
                if 'cpus' in resources:
                    if not isinstance(resources['cpus'], int) or resources['cpus'] < 1:
                        errors.append(f"{prefix}: preprocess.resources.cpus must be a positive integer")
                if 'memory' in resources:
                    if not re.match(r'^\d+[KMGT]?$', str(resources['memory']), re.IGNORECASE):
                        errors.append(f"{prefix}: preprocess.resources.memory has invalid format (expected e.g. '16G', '1024M')")

            # RULE-119: Validate partition_by columns are safe identifiers
            partition_by = table.get('partition_by', [])
            for col in partition_by:
                if not VALID_IDENTIFIER.match(col):
                    errors.append(f"{prefix}: Invalid partition_by column '{col}' - must be valid identifier")

            # RULE-119: Validate naming_pattern capture groups are safe identifiers
            naming_pattern = table.get('naming_pattern')
            if naming_pattern:
                capture_groups = re.findall(r'\?P<([^>]+)>', naming_pattern)
                for group in capture_groups:
                    if not VALID_IDENTIFIER.match(group):
                        errors.append(f"{prefix}: Invalid naming_pattern group '{group}' - must be valid identifier")

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

        # RULE-119: Validate global partition_by columns
        global_partition_by = self.data.get('partition_by', [])
        for col in global_partition_by:
            if not VALID_IDENTIFIER.match(col):
                errors.append(f"Global partition_by: Invalid column '{col}' - must be valid identifier")

        # Print warnings
        for w in warnings:
            logger.warning("%s", w)

        # Raise on errors
        if errors:
            raise ConfigValidationError(errors)
