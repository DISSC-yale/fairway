import yaml
import os
import glob
import re
import logging
import warnings
from dataclasses import dataclass, field, fields
from typing import Any

logger = logging.getLogger("fairway.config")

# Valid SQL/column identifier pattern (RULE-119: prevent injection)
# Must start with letter or underscore, followed by letters, digits, or underscores
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Single source of truth for validation keys lives in validations/checks.py;
# re-use it here so config-load validation and runtime-check validation can't drift.
from fairway.validations.checks import KNOWN_VALIDATION_KEYS, LEGACY_LEVEL_KEYS
from fairway.engines import VALID_ENGINES, normalize_engine_name
from fairway.paths import PathResolver, PathResolverError

# Keys declared in KNOWN_VALIDATION_KEYS but not yet implemented at runtime
# (checks.py raises NotImplementedError). Caught at config load so users
# get a file-name/line context instead of a mid-run stack trace.
UNSUPPORTED_VALIDATION_KEYS = frozenset({"check_unique", "check_custom"})

# Top-level config keys that are recognised. Anything else triggers a warning
# so users catch typos before they cause silent misbehavior. Audited against
# every self.data.get('<key>') callsite plus external config.data consumers
# in pipeline.py (transformation, target_rows at top level are tolerated for
# legacy configs).
KNOWN_TOP_LEVEL_KEYS = frozenset({
    "project",
    "dataset_name", "engine", "storage", "tables", "data",
    "enrichment", "performance", "validations", "logging",
    "partition_by", "container", "schema_pipeline",
    "version", "transformation", "target_rows",
})


def _derive_project_from_dataset_name(name: str) -> str:
    """Sanitize a dataset_name into a valid project id.

    Used as a transitional default when `project:` is absent from the
    YAML so existing fixtures keep working. The regex in PathResolver
    is strict; this lowercases, replaces runs of non-[a-z0-9_-] with
    '_', and trims leading non-alpha. Truncates to 64 chars.
    """
    s = name.lower()
    s = re.sub(r"[^a-z0-9_-]+", "_", s)
    s = re.sub(r"^[^a-z]+", "", s)
    return (s or "project")[:64]


class ConfigValidationError(Exception):
    """Raised when config validation fails."""
    def __init__(self, errors):
        self.errors = errors
        message = "Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)


@dataclass
class TableConfig:
    """Typed configuration for a single table.

    Supports both attribute access (t.name) and dict-style access (t['name'])
    for backward compatibility with code that treats tables as dicts.
    """
    name: str = "unnamed"
    path: str | None = None
    format: str = "csv"
    schema: dict | None = field(default_factory=dict)
    partition_by: list = field(default_factory=list)
    write_mode: str = "overwrite"
    preprocess: dict | None = None
    validations: dict = field(default_factory=dict)
    naming_pattern: str | None = None
    enrichment: dict = field(default_factory=dict)
    output_layer: str = "curated"
    type_enforcement: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    hive_partitioning: bool = False
    read_options: dict = field(default_factory=dict)
    root: str | None = None
    archives: str | None = None
    files: str | None = None
    fixed_width_spec: str | None = None
    batch_strategy: str = "bulk"
    transformation: str | None = None
    min_line_length: int | None = None
    # Runtime fields set by pipeline (not in config YAML)
    _extra: dict = field(default_factory=dict, repr=False)

    @classmethod
    def _field_names(cls) -> set:
        return {f.name for f in fields(cls)} - {'_extra'}

    @classmethod
    def from_dict(cls, d: dict) -> "TableConfig":
        known = cls._field_names()
        known_kwargs = {k: v for k, v in d.items() if k in known}
        extra = {k: v for k, v in d.items() if k not in known and k != '_extra'}
        obj = cls(**known_kwargs)
        obj._extra = extra
        return obj

    # Dict-protocol methods for backward compatibility
    def __getitem__(self, key: str) -> Any:
        if key in self._field_names():
            return getattr(self, key)
        return self._extra[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if key in self._field_names():
            object.__setattr__(self, key, value)
        else:
            self._extra[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._field_names() or key in self._extra

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._field_names():
            return getattr(self, key)
        return self._extra.get(key, default)

    def keys(self):
        for name in self._field_names():
            yield name
        yield from self._extra

    def __iter__(self):
        return self.keys()

    def __len__(self) -> int:
        return len(self._field_names()) + len(self._extra)

    def items(self):
        for name in self._field_names():
            yield name, getattr(self, name)
        yield from self._extra.items()


@dataclass
class HPCConfig:
    """Configuration for Slurm/HPC/Spark cluster settings."""
    account: str | None = None
    partition: str = "day"
    time: str = "04:00:00"
    nodes: int = 2
    cpus_per_node: int = 4
    mem_per_node: str = "32G"
    dynamic_allocation: dict = field(default_factory=dict)
    spark_conf: dict = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, d: dict) -> "HPCConfig":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in known})


class Config:
    def __init__(self, config_path, overrides=None):
        self.config_path = config_path # Store for relative path resolution
        self.overrides = overrides or {}
        with open(config_path, 'r') as f:
            self.data = yaml.safe_load(f)
        
        self.dataset_name = self.data.get('dataset_name')

        # Project identity — explicit field preferred; derive from
        # dataset_name as a transitional fallback so existing fixtures
        # keep loading. Phase 3 hardens this to fail without project:.
        raw_project = self.data.get('project')
        if raw_project:
            self.project = str(raw_project)
        elif self.dataset_name:
            self.project = _derive_project_from_dataset_name(str(self.dataset_name))
        else:
            raise ConfigValidationError([
                "Config must define 'project:' (or 'dataset_name:' for derivation). "
                "Add 'project: <name>' at the top level of your YAML."
            ])

        # Engine Validation
        raw_engine = self.data.get('engine', 'duckdb')
        self.engine = normalize_engine_name(raw_engine)
        if self.engine not in VALID_ENGINES:
            raise ValueError(f"Invalid engine: '{raw_engine}'. Must be one of {sorted(VALID_ENGINES)}")

        self.storage = self.data.get('storage', {})
        # storage.root is user-visible data output location. Default
        # retained for Phase 2a; Phase 3 will require it explicitly.
        self.output_root = self.storage.get('root', 'data')
        self.raw_dir = self.storage.get('raw', os.path.join(self.output_root, 'raw'))
        self.processed_dir = self.storage.get('processed', os.path.join(self.output_root, 'processed'))
        self.curated_dir = self.storage.get('curated', os.path.join(self.output_root, 'curated'))

        temp_raw = os.environ.get('FAIRWAY_TEMP') or self.storage.get('temp') or self.storage.get('scratch_dir')
        self.temp_dir = os.path.expandvars(temp_raw) if temp_raw else None

        # Construct PathResolver — single source of truth for fairway
        # state/scratch paths. run_id is bound later by IngestionPipeline.
        self.paths = PathResolver.from_config(self)

        # Load HPC/Spark defaults from config/spark.yaml if it exists
        self.hpc = self._load_hpc_config()

        # Warn on unknown top-level keys (likely typos)
        if isinstance(self.data, dict):
            for key in self.data:
                if key not in KNOWN_TOP_LEVEL_KEYS:
                    warnings.warn(
                        f"Unknown config key '{key}' — check for typos",
                        UserWarning,
                        stacklevel=2,
                    )

        # Alias deprecated 'data.sources' → 'data.tables'. If both are set,
        # error out rather than silently dropping one.
        data_block = self.data.get('data') if isinstance(self.data, dict) else None
        if isinstance(data_block, dict):
            has_sources = 'sources' in data_block
            has_tables = 'tables' in data_block
            if has_sources and has_tables:
                raise ConfigValidationError([
                    "Config defines both 'data.sources' (deprecated) and "
                    "'data.tables'. Use only 'data.tables'."
                ])
            if has_sources and not has_tables:
                warnings.warn(
                    "Config uses 'data.sources' which is deprecated. "
                    "Rename to 'data.tables'.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                data_block['tables'] = data_block.pop('sources')

        raw_tables = self.data.get('tables') or self.data.get('data', {}).get('tables', [])
        self.tables = self._expand_tables(raw_tables)
        self._validate()

        self.enrichment = self.data.get('enrichment', {})
        self.partition_by = self.data.get('partition_by', [])
        self.output_format = self.storage.get('format', 'parquet').lower()

        # Performance
        performance = self.data.get('performance', {})
        self.target_rows = performance.get('target_rows') or self.data.get('target_rows', 500000)
        self.salting = performance.get('salting', False)
        self.target_file_size_mb = performance.get('target_file_size_mb', 128)
        self.compression = performance.get('compression', 'snappy')
        self.max_records_per_file = performance.get('max_records_per_file')

        # Container
        container = self.data.get('container', {})
        self.apptainer_binds = container.get('apptainer_binds')

    def _load_hpc_config(self) -> HPCConfig:
        """Load spark.yaml from the same directory as fairway.yaml or the config/ subfolder."""
        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        candidates = [
            os.path.join(config_dir, 'spark.yaml'),
            os.path.join(os.path.dirname(config_dir), 'config', 'spark.yaml'),
        ]
        for path in candidates:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return HPCConfig.from_dict(yaml.safe_load(f) or {})
        return HPCConfig()

    def resolve_resources(self):
        """Resolve Slurm/Spark resources with correct precedence."""
        res = {
            'account': self.hpc.account,
            'partition': self.hpc.partition,
            'time': self.hpc.time,
            'nodes': self.hpc.nodes,
            'cpus': self.hpc.cpus_per_node,
            'mem': self.hpc.mem_per_node,
        }
        for t in self.tables:
            hints = t.preprocess.get('resources', {}) if t.preprocess else {}
            if hints:
                if 'cpus' in hints: res['cpus'] = hints['cpus']
                if 'memory' in hints: res['mem'] = hints['memory']
                if 'time' in hints: res['time'] = hints['time']
                break
        for key in ['account', 'partition', 'time', 'nodes', 'cpus', 'mem']:
            if self.overrides.get(key) is not None:
                res[key] = self.overrides[key]
        return res

    @property
    def binds_list(self) -> str:
        bind_paths = set()
        bind_paths.add(os.path.dirname(os.path.abspath(self.config_path)))
        for path in [self.raw_dir, self.processed_dir, self.curated_dir]:
            if path:
                abs_p = os.path.abspath(path)
                if os.path.exists(abs_p):
                    bind_paths.add(os.path.dirname(abs_p) if os.path.isfile(abs_p) else abs_p)
        if self.temp_dir: bind_paths.add(os.path.abspath(self.temp_dir))
        for tbl in self.tables:
            if tbl.root: bind_paths.add(os.path.abspath(tbl.root))
        if self.apptainer_binds:
            for p in self.apptainer_binds.split(','):
                if p.strip(): bind_paths.add(p.strip())
        return ','.join(sorted(bind_paths))

    def _resolve_path(self, path_ref, config_dir):
        """Resolve a relative path against config_dir.

        CWD is NEVER consulted — reproducibility requires that the same
        fairway.yaml resolves identically regardless of where the user
        invoked fairway from. Absolute paths pass through unchanged.
        """
        if not path_ref:
            return None
        return str(PathResolver.resolve_config_path(path_ref, config_dir))

    def _resolve_script_path(self, script_path, config_dir):
        """Resolve a preprocessing script path, searching config_dir only.

        Tries the bare ref, then `scripts/` and `src/preprocess/`
        subdirectories under config_dir (common project layouts). CWD
        fallback was removed in Phase 3 — configs must be
        invocation-directory-independent.
        """
        if os.path.isabs(script_path):
            return script_path
        basename = os.path.basename(script_path)
        candidates = [
            os.path.join(config_dir, script_path),
            os.path.join(config_dir, 'scripts', basename),
            os.path.join(config_dir, 'src', 'preprocess', basename),
        ]
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        # Default: resolve against config_dir even if missing. Downstream
        # validators surface the clear "not found" error rather than a
        # silent CWD-relative resolution that only the invoking user sees.
        return os.path.join(config_dir, script_path)

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

            # Archives-only tables (no path key) bypass path resolution entirely
            if not path_pattern:
                table_format = tbl.get('format', 'csv')
                valid_formats = {'csv', 'tsv', 'tab', 'json', 'parquet', 'fixed_width'}
                if table_format not in valid_formats:
                    raise ValueError(f"Invalid format: '{table_format}'. Must be one of {valid_formats}")
                preprocess = tbl.get('preprocess', {}).copy() if tbl.get('preprocess') else {}
                expanded.append(TableConfig.from_dict({
                    'name': tbl.get('name', 'unnamed'),
                    'path': None,
                    'format': table_format,
                    'metadata': {},
                    'naming_pattern': naming_pattern,
                    'schema': self._load_schema(tbl.get('schema')),
                    'transformation': self._resolve_path(tbl.get('transformation'), config_dir),
                    'hive_partitioning': tbl.get('hive_partitioning', False),
                    'partition_by': tbl.get('partition_by', []),
                    'read_options': tbl.get('read_options', {}),
                    'preprocess': preprocess,
                    'write_mode': tbl.get('write_mode', 'overwrite'),
                    'root': tbl.get('root'),
                    'archives': archives_pattern,
                    'files': files_pattern,
                    'fixed_width_spec': self._resolve_path(tbl.get('fixed_width_spec'), config_dir),
                    'batch_strategy': tbl.get('batch_strategy', 'bulk'),
                    'transformation': self._resolve_path(tbl.get('transformation'), config_dir),
                    'min_line_length': tbl.get('min_line_length'),
                    'type_enforcement': tbl.get('type_enforcement', {}),
                    'validations': self._resolve_table_validations(tbl),
                    'output_layer': tbl.get('output_layer', 'curated'),
                }))
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

                 expanded.append(TableConfig.from_dict({
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
                 }))

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
                    if execution_mode == 'cluster' and normalize_engine_name(self.engine) != 'pyspark':
                         raise ValueError(f"Configuration Error: 'execution_mode: cluster' is only available with 'engine: pyspark'. Table: {tbl.get('name')}")

                    # Resolve file paths relative to config dir
                    table_root = self._resolve_path(tbl.get('root'), config_dir)
                    fixed_width_spec = self._resolve_path(tbl.get('fixed_width_spec'), config_dir)
                    transformation = self._resolve_path(tbl.get('transformation'), config_dir)

                    # Resolve preprocess.action: CWD first, then config_dir
                    preprocess = tbl.get('preprocess', {}).copy() if tbl.get('preprocess') else {}
                    if preprocess.get('action', '').endswith('.py'):
                        preprocess['action'] = self._resolve_script_path(preprocess['action'], config_dir)

                    expanded.append(TableConfig.from_dict({
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
                    }))
        return expanded

    def _validate(self):
        """Validate config and fail fast on errors."""
        errors = []
        warn_messages = []
        config_dir = os.path.dirname(os.path.abspath(self.config_path)) if hasattr(self, 'config_path') else os.getcwd()

        # Check for empty tables
        if not self.tables:
            # This is a warning, not an error - user might be testing config
            warn_messages.append("No tables defined in config")

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

            # Validation keys: reject unsupported and shape-mismatched values,
            # warn on unknown keys, allow legacy level1/level2 nesting which
            # Validator._normalize_validation_config() handles at runtime.
            table_validations = table.get('validations', {})
            if isinstance(table_validations, dict):
                for vkey, vval in table_validations.items():
                    if vkey in UNSUPPORTED_VALIDATION_KEYS:
                        errors.append(
                            f"{prefix}: validation '{vkey}' is not yet implemented. "
                            f"Remove it from your config."
                        )
                    elif vkey in LEGACY_LEVEL_KEYS:
                        # Legacy level1/level2 nesting is supported at runtime;
                        # don't warn so existing configs load clean.
                        pass
                    elif vkey in KNOWN_VALIDATION_KEYS:
                        expected_type = KNOWN_VALIDATION_KEYS[vkey]
                        if not isinstance(vval, expected_type):
                            type_names = (
                                ", ".join(t.__name__ for t in expected_type)
                                if isinstance(expected_type, tuple)
                                else expected_type.__name__
                            )
                            errors.append(
                                f"{prefix}: validation '{vkey}' must be "
                                f"{type_names}, got {type(vval).__name__}"
                            )
                    elif isinstance(vval, dict):
                        errors.append(
                            f"{prefix}: validations must be flat — "
                            f"found nested dict under unknown key '{vkey}'. "
                            f"Use e.g. 'min_rows: 5' not '{vkey}: {{min_rows: 5}}'."
                        )
                    else:
                        warnings.warn(
                            f"Unknown validation key '{vkey}' in {prefix}",
                            UserWarning,
                            stacklevel=2,
                        )

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
        for w in warn_messages:
            logger.warning("%s", w)

        # Raise on errors
        if errors:
            raise ConfigValidationError(errors)


def validate_config_paths(config):
    """Assert every config path field is absolute or resolved.

    Phase 3 rule: configs are portable. A fairway.yaml that resolves
    correctly when invoked from its own directory must also resolve
    correctly when invoked from /tmp, from Slurm, from an Apptainer
    container, etc. That requires every path to be either absolute
    or deterministically relative to the config file — never to CWD.

    Escape hatch: tables with `preprocess` or `archives` set may
    reference outputs that don't exist until preprocessing runs. The
    `has_preprocessing` flag suppresses the existence check for those
    specific fields (fixed_width_spec, transformation output).
    """
    errors = []
    for t in config.tables:
        has_preprocessing = bool(t.get('archives') or t.get('preprocess'))
        for field_name in ('path', 'fixed_width_spec', 'transformation', 'root'):
            val = t.get(field_name)
            if not val:
                continue
            # Paths with glob wildcards are expanded at runtime; their
            # literal form does not need to exist.
            if '*' in str(val):
                continue
            if has_preprocessing and field_name in ('fixed_width_spec', 'transformation'):
                continue
            if not os.path.isabs(val):
                errors.append(
                    f"tables['{t.get('name')}'].{field_name} is not absolute: {val!r}. "
                    f"Relative paths must be resolved at config-load time."
                )
    if errors:
        raise ConfigValidationError(errors)
