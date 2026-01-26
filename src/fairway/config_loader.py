import yaml
import os
import glob
import re

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
        
        # Support both root-level 'sources' and 'data: sources' structures
        raw_srcs = self.data.get('sources')
        if not raw_srcs:
             raw_srcs = self.data.get('data', {}).get('sources', [])
             
        self.sources = self._expand_sources(raw_srcs)
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

    def _expand_sources(self, raw_sources):
        """Discovers files and extracts metadata via regex if patterns are provided."""
        expanded = []
        # Base config directory for relative path resolution
        config_dir = os.path.dirname(os.path.abspath(self.config_path)) if hasattr(self, 'config_path') else os.getcwd()

        for src in raw_sources:
            path_pattern = src.get('path_pattern') or src.get('path')
            naming_pattern = src.get('naming_pattern')
            hive_partitioning = src.get('hive_partitioning', False)
            
            if not path_pattern:
                continue

            # Resolve path relative to config file if it's not absolute
            # SKIP if 'root' is defined (let pipeline handle it)
            if not os.path.isabs(path_pattern) and not src.get('root'):
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

            raw_schema = src.get('schema')
            resolved_schema = self._load_schema(raw_schema)

            raw_schema = src.get('schema')
            resolved_schema = self._load_schema(raw_schema)
            
            # Glob expansion behavior:
            # - expand_glob=False (default): glob pattern → one source → one table (standard behavior)
            # - expand_glob=True: each matching file → separate source (for per-file metadata extraction)
            expand_glob = src.get('expand_glob', False)

            if hive_partitioning or not expand_glob:
                 # Treat the directory/glob as a single source unit
                 # Logic is shared with Hive partitioning branch but without the hive flag if just expand_glob=False
                 
                 # Check existence only if literal path (not a glob with wildcards, unless checking dir)
                 if '*' not in resolved_path and not os.path.exists(resolved_path):
                      print(f"WARNING: Source path not found: {resolved_path}")
                      continue
                 
                 source_format = src.get('format', 'csv')
                 # Normalize 'tab' to 'tsv'
                 if source_format == 'tab':
                      source_format = 'tsv'
                 valid_formats = {'csv', 'tsv', 'json', 'parquet'}
                 if source_format not in valid_formats:
                      raise ValueError(f"Invalid format: '{source_format}'. Must be one of {valid_formats} (or 'tab')")
                 
                 # Name is explicit or basename of path (which might be a pattern like *.csv)
                 # If pattern, basename might be '*.csv'. Ideally user provides name.
                 source_name = src.get('name', os.path.basename(resolved_path))

                 expanded.append({
                    'name': source_name,
                    'path': resolved_path,
                    'format': source_format,
                    'metadata': {},
                    'schema': resolved_schema,
                    'transformation': src.get('transformation'),
                    'hive_partitioning': hive_partitioning,
                    'partition_by': src.get('partition_by', []),
                    'read_options': src.get('read_options', {}),
                    'preprocess': src.get('preprocess', {}),
                    'write_mode': src.get('write_mode', 'overwrite'),
                    'root': src.get('root')
                })

            else:
                # Glob discovery (Eager expansion)
                search_path = resolved_path
                source_root = src.get('root')
                
                if source_root:
                    # Resolve root relative to config dir if needed
                    if not os.path.isabs(source_root):
                         source_root = os.path.join(config_dir, source_root)
                    
                    # Fix: os.path.join ignores root if path_pattern starts with /
                    # We strip leading slash to ensure join works
                    rel_pattern = path_pattern.lstrip(os.sep)
                    search_path = os.path.join(source_root, rel_pattern)

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
                    
                    # Create a specific source entry for each file
                    source_format = src.get('format', 'csv')
                    # Normalize 'tab' to 'tsv'
                    if source_format == 'tab':
                         source_format = 'tsv'
                    valid_formats = {'csv', 'tsv', 'json', 'parquet'}
                    if source_format not in valid_formats:
                         raise ValueError(f"Invalid format: '{source_format}'. Must be one of {valid_formats} (or 'tab')")

                    
                    execution_mode = src.get('preprocess', {}).get('execution_mode', 'driver')
                    if execution_mode == 'cluster' and self.engine not in ['pyspark', 'spark']:
                         raise ValueError(f"Configuration Error: 'execution_mode: cluster' is only available with 'engine: pyspark'. Source: {src.get('name')}")
                    
                    # Determine source root for relocatability
                    source_root_raw = src.get('root')
                    source_root = None
                    if source_root_raw:
                        if not os.path.isabs(source_root_raw):
                             source_root = os.path.join(config_dir, source_root_raw)
                        else:
                             source_root = source_root_raw
                    
                    expanded.append({
                        'name': src.get('name', os.path.basename(f)),
                        'path': f,
                        'root': source_root,
                        'format': source_format,
                        'metadata': metadata,
                        'naming_pattern': naming_pattern,
                        'schema': resolved_schema,
                        'transformation': src.get('transformation'),
                        'hive_partitioning': False,
                        'partition_by': src.get('partition_by', []),
                        'read_options': src.get('read_options', {}),
                        'preprocess': src.get('preprocess', {}),
                        'write_mode': src.get('write_mode', 'overwrite')
                    })
        return expanded

    def get_source_by_name(self, name):
        for source in self.sources:
            if source['name'] == name:
                return source
        return None
