import os
import sys
import glob
import shutil
import zipfile
import tarfile
import hashlib
import importlib.util
import logging
import tempfile
from .config_loader import Config
from .manifest import ManifestStore, _get_file_hash_static
from .batcher import PartitionBatcher
from .validations.checks import Validator
from .summarize import Summarizer
from .enrichments.geospatial import Enricher
from .logging_config import BatchLogger

logger = logging.getLogger("fairway.pipeline")


def _recover_atomic_swap(final_output_path: str, old_backup: str, temp_final: str) -> None:
    """Recover from an interrupted atomic swap.

    Safe recovery order:
    1. If final_output_path is missing AND old_backup exists → restore from backup.
    2. Only delete old_backup after confirming final_output_path is valid.
    3. Clean up temp_final only after old_backup is safely handled.
    """
    if not os.path.exists(final_output_path) and os.path.exists(old_backup):
        os.rename(old_backup, final_output_path)
        logger.warning("Recovered curated data from interrupted swap: %s", final_output_path)

    if os.path.exists(old_backup) and os.path.exists(final_output_path):
        if os.path.isdir(old_backup):
            shutil.rmtree(old_backup)
        else:
            os.remove(old_backup)

    if os.path.exists(temp_final):
        if os.path.isdir(temp_final):
            shutil.rmtree(temp_final)
        else:
            os.remove(temp_final)


def _is_preprocess_script_allowed(script_path):
    """Check if preprocessing script path is within an allowed directory.

    Prevents path traversal attacks by restricting custom script execution
    to project directories only.
    """
    real_path = os.path.realpath(script_path)
    cwd = os.path.realpath(os.getcwd())

    allowed_dirs = [
        os.path.join(cwd, 'src'),
        os.path.join(cwd, 'src', 'preprocess'),
        os.path.join(cwd, 'scripts'),
        os.path.join(cwd, 'transformations'),
    ]

    for allowed_dir in allowed_dirs:
        allowed_dir = os.path.realpath(allowed_dir)
        if real_path.startswith(allowed_dir + os.sep):
            return True
    return False


class ArchiveCache:
    """Persistent cache for extracted archives.

    Extracts archives to .fairway_cache/archives/{name}_{hash}/ and tracks
    extractions in the manifest. Multiple tables can share the same archive
    extraction.
    """

    def __init__(self, config, global_manifest, table_name=None):
        self.config = config
        self.global_manifest = global_manifest
        self.table_name = table_name
        self._session_cache = {}  # in-memory cache for current run

    def get_extracted_path(self, archive_path):
        """Return path to extracted contents, extracting if needed."""
        abs_path = os.path.abspath(archive_path)

        # Check in-memory cache first (same-run optimization)
        if abs_path in self._session_cache:
            return self._session_cache[abs_path]

        # Check manifest for valid existing extraction
        if self.global_manifest.is_extraction_valid(archive_path):
            entry = self.global_manifest.get_extraction(archive_path)
            logger.debug("Using cached extraction for: %s", archive_path)
            self._session_cache[abs_path] = entry['extracted_dir']
            return entry['extracted_dir']

        # Need to extract
        extraction_dir = self._get_extraction_dir(archive_path)
        os.makedirs(extraction_dir, exist_ok=True)

        logger.info("Extracting archive: %s → %s", archive_path, extraction_dir)
        self._extract(archive_path, extraction_dir)

        # Record in manifest
        archive_hash = _get_file_hash_static(archive_path, fast_check=True)
        self.global_manifest.record_extraction(archive_path, extraction_dir, archive_hash, self.table_name)

        self._session_cache[abs_path] = extraction_dir
        return extraction_dir

    def _get_extraction_dir(self, archive_path):
        """Get deterministic extraction location for an archive."""
        archive_name = os.path.basename(archive_path).replace('.', '_')
        # Use mtime+size hash for speed (consistent with manifest fast_check)
        archive_hash = _get_file_hash_static(archive_path, fast_check=True)
        # Create short hash for directory name
        short_hash = hashlib.md5(archive_hash.encode()).hexdigest()[:8]

        # Use temp_dir from config, or default to .fairway_cache
        base_dir = self.config.temp_dir or os.path.join(os.getcwd(), '.fairway_cache')
        return os.path.join(base_dir, 'archives', f"{archive_name}_{short_hash}")

    def _safe_extract_path(self, dest_dir, member_path):
        """Validate extracted path is within destination (prevents Zip Slip attack)."""
        # Resolve the full path
        dest_dir = os.path.realpath(dest_dir)
        target_path = os.path.realpath(os.path.join(dest_dir, member_path))

        # Ensure target is within destination directory
        if not target_path.startswith(dest_dir + os.sep) and target_path != dest_dir:
            raise ValueError(f"Path traversal detected: {member_path}")
        return target_path

    def _extract(self, archive_path, dest_dir):
        """Extract archive based on type with path traversal protection."""
        dest_dir = os.path.realpath(dest_dir)

        if archive_path.endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zf:
                for member in zf.namelist():
                    # Validate each path before extraction
                    self._safe_extract_path(dest_dir, member)
                zf.extractall(dest_dir)
        elif archive_path.endswith(('.tar.gz', '.tgz')):
            with tarfile.open(archive_path, 'r:gz') as tf:
                for member in tf.getmembers():
                    self._safe_extract_path(dest_dir, member.name)
                tf.extractall(dest_dir)
        elif archive_path.endswith('.tar'):
            with tarfile.open(archive_path, 'r') as tf:
                for member in tf.getmembers():
                    self._safe_extract_path(dest_dir, member.name)
                tf.extractall(dest_dir)
        elif archive_path.endswith('.gz') and not archive_path.endswith('.tar.gz'):
            import gzip
            output_name = os.path.basename(archive_path)[:-3]
            # Validate output path
            output_file = self._safe_extract_path(dest_dir, output_name)
            with gzip.open(archive_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            raise ValueError(f"Unsupported archive type: {archive_path}")


def _build_enforcement_schema(table, fixed_width_spec_path):
    """
    Returns the list of column dicts (name, type, cast_mode) to use for type
    enforcement, or an empty list if type enforcement should be skipped.

    Rules:
      - fixed_width tables: auto-enabled when the spec has any non-STRING typed
        column, unless type_enforcement.enabled is explicitly False.
      - All other formats: only enabled when type_enforcement.enabled is True
        and a schema is declared.
    """
    type_enforcement = table.get('type_enforcement', {})
    explicitly_disabled = type_enforcement.get('enabled') is False

    if explicitly_disabled:
        return []

    # Fixed-width path: derive column list from the spec
    if fixed_width_spec_path:
        try:
            from fairway.fixed_width import load_spec
            spec = load_spec(fixed_width_spec_path)
            cols = spec.get('columns', [])
            typed_cols = [c for c in cols if c.get('type', 'VARCHAR').upper() not in ('VARCHAR', 'STRING')]
            if typed_cols:
                return cols  # pass all columns; enforce_types() skips VARCHAR ones
        except Exception:
            pass
        return []

    # Non-fixed-width path: only enabled when explicitly opted in
    if not type_enforcement.get('enabled'):
        return []

    raw_schema = table.get('schema', {})
    if not raw_schema:
        return []

    # Build column list from the schema dict {col_name: type_string}
    return [
        {'name': col_name, 'type': col_type, 'cast_mode': 'adaptive'}
        for col_name, col_type in raw_schema.items()
    ]


class IngestionPipeline:
    def __init__(self, config_path, spark_master=None, engine_override=None, spark_conf=None):
        logger.debug("Loading local fairway.pipeline")
        self.config = Config(config_path)
        # Use storage-root-based manifest dir when output_root is absolute (explicit config).
        # Fall back to "manifest" (CWD-relative) when output_root is relative (default "data").
        output_root = self.config.output_root
        if os.path.isabs(output_root):
            manifest_dir = os.path.join(output_root, "manifest")
        else:
            manifest_dir = "manifest"
        self.manifest_store = ManifestStore(manifest_dir)
        self._engine = None  # Lazy-initialized; use self.engine property
        self._engine_args = (spark_master, engine_override, spark_conf)
        self._hash_cache = {}  # Cache for distributed hash results
        self.archive_cache = ArchiveCache(self.config, self.manifest_store.global_manifest)

    @property
    def engine(self):
        """Lazy engine initialization — defers Spark cluster startup until first use."""
        if self._engine is None:
            self._engine = self._get_engine(*self._engine_args)
        return self._engine

    @engine.setter
    def engine(self, value):
        self._engine = value

    def _get_engine(self, spark_master=None, engine_override=None, spark_conf=None):
        # CLI override takes precedence over config
        engine_type = engine_override or (self.config.engine.lower() if self.config.engine else 'duckdb')

        if engine_type in ['pyspark', 'spark']:
            try:
                from .engines.pyspark_engine import PySparkEngine
                return PySparkEngine(spark_master, spark_conf=spark_conf)
            except ImportError:
                sys.exit("Error: PySpark is not installed. Please install using `pip install fairway[spark]`")
        elif engine_type == 'duckdb':
            from .engines.duckdb_engine import DuckDBEngine
            return DuckDBEngine()
        else:
            raise ValueError(f"Unknown engine: {engine_type}. Supported engines: 'duckdb', 'spark'") 

    def _preprocess(self, table):
        """
        Handles preprocessing (unzipping, custom scripts) before ingestion.
        Returns the path to the processed data (or original path if no preprocess).
        """
        # Check for archive-based processing (new C.3 feature)
        archives_pattern = table.get('archives')
        if archives_pattern:
            return self._preprocess_archives(table)

        preprocess_config = table.get('preprocess')
        if not preprocess_config:
            # No preprocessing needed - resolve root + path and return
            input_path = table['path']
            root = table.get('root')
            if root and not os.path.isabs(input_path):
                rel_input = input_path.lstrip(os.sep)
                return os.path.join(root, rel_input)
            return input_path

        action = preprocess_config.get('action')
        scope = preprocess_config.get('scope', 'per_file') # 'global' or 'per_file'
        mode = preprocess_config.get('execution_mode', 'driver') # 'driver' or 'cluster'

        # Derive file filter from table format
        # Explicit include in preprocess config takes priority
        include_pattern = preprocess_config.get('include')
        if not include_pattern:
            file_format = table.get('format')
            format_to_ext = {
                'tab': '*.tab',
                'tsv': '*.tsv',
                'csv': '*.csv',
                'json': '*.json',
                'jsonl': '*.jsonl',
                'parquet': '*.parquet',
            }
            include_pattern = format_to_ext.get(file_format)

        # Compute batch_dir early (before cache check) so we can set
        # _preprocess_root for manifest key generation even on cache hits.
        # Without this, files with the same basename in different extraction
        # subdirs get the same manifest key (basename-only fallback).
        temp_loc = self.config.temp_dir
        if not temp_loc:
            scratch_base = os.environ.get('SCRATCH')
            if scratch_base:
                temp_loc = os.path.join(scratch_base, 'fairway')
            else:
                temp_loc = os.path.join(tempfile.gettempdir(), f'fairway_{os.getenv("USER", "default")}')
        safe_name = "".join([c if c.isalnum() else "_" for c in table['name']])
        batch_dir = os.path.join(temp_loc, f"{safe_name}_v1")

        # Store batch_dir on table dict so manifest recording uses it as
        # table_root for preprocessed files (avoids basename key collisions)
        table['_preprocess_root'] = batch_dir

        # Check preprocessing cache first
        table_manifest = self.manifest_store.get_table_manifest(table['name'])
        cached = table_manifest.get_preprocessed_path(
            table['path'],
            table.get('root')
        )
        if cached:
            # Apply file filter to cached path if needed
            if include_pattern:
                cached = os.path.join(cached, "**", include_pattern)
            logger.debug("Reusing cached preprocessing: %s", cached)
            return cached

        logger.info("Preprocessing %s with action='%s', scope='%s', mode='%s'", table['name'], action, scope, mode)
        if include_pattern:
            logger.debug("File filter: %s", include_pattern)

        # If table has fixed_width_spec, derive specs_dir so preprocessing
        # writes the spec file where the config expects to find it
        if not preprocess_config.get('specs_dir') and table.get('fixed_width_spec'):
            specs_dir = os.path.dirname(table['fixed_width_spec'])
            if specs_dir:
                preprocess_config['specs_dir'] = specs_dir
                logger.debug("Derived specs_dir from fixed_width_spec: %s", specs_dir)

        os.makedirs(batch_dir, exist_ok=True)
        logger.info("Preprocessing scratch dir: %s", batch_dir)
        
        # Resolve input files
        # If input_path is a glob, we get all files.
        input_path = table['path']

        # New in V2: Handle Root Resolution
        root = table.get('root')
        full_input_path = input_path
        
        if root and not os.path.isabs(input_path):
             # Fix: Ensure input_path doesn't reset root if it starts with /
             rel_input = input_path.lstrip(os.sep)
             full_input_path = os.path.join(root, rel_input)
             
        # Resolve glob against the FULL path (root + relative_glob)
        files = glob.glob(full_input_path) if '*' in full_input_path else [full_input_path]
        
        logger.debug("Found %d candidates at %s", len(files), full_input_path)
        
        if not files:
             logger.warning("No files found for preprocessing at %s (CWD: %s)", full_input_path, os.getcwd())
             if root:
                 logger.warning("Root: %s, Path: %s", root, input_path)
             return input_path 

        # Define the work function
        def process_file(file_path):
             # Logic to process a single file. 
             # MUST be self-contained for Spark serialization if mode='cluster'
             import os
             import zipfile
             
             # Determine output location (temp dir usually)
             # We use a temp dir in the configured storage or system temp
             # For straightforward logic, let's assume we extract to a _preprocessed sibling dir
             
             base_dir = os.path.dirname(file_path)
             file_name = os.path.basename(file_path)
             name_no_ext = os.path.splitext(file_name)[0]
             
             if batch_dir:
                  # Use the global temp batch directory
                  output_dir = os.path.join(batch_dir, name_no_ext)
             else:
                  # Use default sibling directory
                  output_dir = os.path.join(base_dir, f".preprocessed_{name_no_ext}")
                  
             os.makedirs(output_dir, exist_ok=True)
             
             if action == 'unzip':
                 if zipfile.is_zipfile(file_path):
                     # Skip if already extracted (check for any files in output_dir)
                     if os.path.exists(output_dir) and os.listdir(output_dir):
                         return output_dir
                     with zipfile.ZipFile(file_path, 'r') as zip_ref:
                         realpath_output_dir = os.path.realpath(output_dir)
                         for member_info in zip_ref.infolist():
                             # Block symlink entries (Unix attr bits 0xA000 = symlink)
                             if (member_info.external_attr >> 16) & 0xFFFF == 0xA000:
                                 raise ValueError(
                                     f"Zip Slip blocked: {member_info.filename!r} is a symlink entry"
                                 )
                             member_path = os.path.realpath(
                                 os.path.join(output_dir, member_info.filename)
                             )
                             if not member_path.startswith(
                                 realpath_output_dir + os.sep
                             ) and member_path != realpath_output_dir:
                                 raise ValueError(
                                     f"Zip Slip blocked: {member_info.filename!r} would extract "
                                     f"outside {output_dir}"
                                 )
                         zip_ref.extractall(output_dir)
                     return output_dir
                 else:
                     return file_path # Not a zip, return original
             
             elif action.endswith('.py'):
                 # Custom script logic
                 # For cluster execution, we can't easily dynamic-import a path that doesn't exist on worker.
                 # Strategy: read script content on driver, exec() it inside the wrapper?
                 # Or assume file exists on shared FS.
                 # For now, let's assume shared FS or Driver mode.

                 # Security: Validate script is in an allowed directory
                 if not _is_preprocess_script_allowed(action):
                     raise ValueError(
                         f"Security error: Preprocessing script must be in project's src/, scripts/, or transformations/ directory. "
                         f"Attempted to load: {action}"
                     )

                 # Security: Only allow .py files (already checked by endswith above, but explicit)
                 if not action.endswith('.py'):
                     raise ValueError(f"Security error: Preprocessing script must be a .py file: {action}")

                 # Verify script exists before attempting import
                 if not os.path.exists(action):
                     raise FileNotFoundError(
                         f"Preprocessing script not found: {action} (CWD: {os.getcwd()})"
                     )

                 # Dynamic import
                 spec = importlib.util.spec_from_file_location("custom_module", action)
                 if not spec or not spec.loader:
                     raise ImportError(
                         f"Failed to load preprocessing script (importlib returned None): {action}"
                     )

                 module = importlib.util.module_from_spec(spec)
                 spec.loader.exec_module(module)
                 # Pass extra config options (e.g., password_file) to the script
                 extra_opts = {k: v for k, v in preprocess_config.items()
                               if k not in ('action', 'scope', 'execution_mode', 'include', 'resources')}
                 if hasattr(module, 'process_file'):
                     return module.process_file(file_path, output_dir, **extra_opts)
                 elif hasattr(module, 'process'):
                     return module.process(file_path, output_dir, **extra_opts)
                 else:
                     raise AttributeError(
                         f"Preprocessing script has no 'process_file' or 'process' function: {action}"
                     )
             
             return file_path

        # Execution
        processed_paths = []
        
        if mode == 'cluster':
             if not hasattr(self.engine, 'distribute_task'):
                 logger.warning("execution_mode='cluster' requested but engine doesn't support it. Falling back to 'driver' mode.")
                 mode = 'driver'

        if mode == 'cluster':

             # Dispatch to cluster
             # Note: For custom scripts, ensure the script file is accessible on workers
             logger.info("Distributing preprocessing task for %d files to Spark cluster...", len(files))
             results = self.engine.distribute_task(files, process_file)
             logger.info("Cluster task complete. Processed %d items.", len(results))
             processed_paths = results
        else:
             # Driver mode
             logger.info("Running preprocessing locally for %d files...", len(files))
             for i, f in enumerate(files):
                 if i % 10 == 0:
                     logger.debug("Processed %d/%d...", i, len(files))
                 processed_paths.append(process_file(f))
        
        # Return the new input path. 
        # If we processed multiple files into multiple dirs, we need to handle that.
        # Ideally we return a glob pattern matching the processed results, or the directory.
        
        # Simplification: If 1 file -> 1 dir, return dir.
        # If N files -> N dirs.
        # Fairway's configured 'path' usually expects a glob or dir.
        # If we extracted to .preprocessed_*, we can return a glob pattern for that.
        
        # Construct a sensible return path
        # If we extracted zips, we probably want to ingest the contents.
        # Use include_pattern to filter specific file types (e.g., "*.tab")
        file_glob = include_pattern if include_pattern else "*"

        if len(processed_paths) == 1:
             # Single extraction directory — return the directory itself.
             # PySpark uses recursiveFileLookup=true to find files;
             # DuckDB resolves globs internally. Embedding "**" in the path
             # breaks Hadoop's file:// protocol which doesn't support "**".
             if include_pattern:
                 result_path = os.path.join(processed_paths[0], include_pattern)
             else:
                 result_path = processed_paths[0]
        else:
             # If multiple, return the common structure...
             if batch_dir:
                  # If we used a batch dir, return everything inside it
                  # Structure: batch_dir / <file_dir> / <contents>
                  result_path = os.path.join(batch_dir, "*", file_glob)
             else:
                  # Fallback: sibling directories (use root-resolved path)
                  base = os.path.dirname(full_input_path)
                  result_path = os.path.join(base, ".preprocessed_*", file_glob)

        # Record result for future reuse
        table_manifest.record_preprocessing(
            original_path=table.get('path') or table.get('archives'),
            preprocessed_path=result_path,
            action=action,
            table_root=table.get('root')
        )

        return result_path

    def _preprocess_archives(self, table):
        """
        Handle archive-based preprocessing using archives/files config keys.
        Extracts archives using ArchiveCache and returns path pattern for files.
        """
        archives_pattern = table.get('archives')
        files_pattern = table.get('files', '**/*')
        root = table.get('root')

        logger.info("Processing archives for %s...", table['name'])

        # Resolve archive pattern
        config_dir = os.path.dirname(os.path.abspath(self.config.config_path))
        if root:
            root_path = os.path.join(config_dir, root) if not os.path.isabs(root) else root
            search_path = os.path.join(root_path, archives_pattern)
        else:
            search_path = os.path.join(config_dir, archives_pattern) if not os.path.isabs(archives_pattern) else archives_pattern

        archive_files = glob.glob(search_path)
        if not archive_files:
            raise ValueError(f"No archives match pattern: {search_path}")

        logger.info("Found %d archive(s)", len(archive_files))

        # Extract each archive (cached) and collect file paths
        all_extracted_dirs = []
        for archive in archive_files:
            extracted_dir = self.archive_cache.get_extracted_path(archive)
            all_extracted_dirs.append(extracted_dir)

        # Build result path pattern and collect all matched files
        all_matched_files = []
        for extracted_dir in all_extracted_dirs:
            pattern = os.path.join(extracted_dir, files_pattern)
            matched = glob.glob(pattern, recursive=True)
            all_matched_files.extend(matched)

        logger.info("Files matching '%s': %d", files_pattern, len(all_matched_files))

        if not all_matched_files:
            logger.warning("No files match pattern '%s' in extracted archives", files_pattern)
            # Return pattern for first dir (will be empty)
            return os.path.join(all_extracted_dirs[0], files_pattern)

        # Store all matched files for the engine to process
        table['_extracted_files'] = all_matched_files

        # Store common parent of extracted dirs so manifest recording uses
        # unique relative-path keys (avoids basename collision across archives)
        if len(all_extracted_dirs) > 1:
            table['_preprocess_root'] = os.path.commonpath(all_extracted_dirs)
        else:
            table['_preprocess_root'] = all_extracted_dirs[0]

        # Return path pattern for the extracted files
        if len(all_extracted_dirs) == 1:
            return os.path.join(all_extracted_dirs[0], files_pattern)
        else:
            # Multiple archives - return the first matched file's parent with glob
            # The engine will use _extracted_files list directly
            return os.path.join(all_extracted_dirs[0], files_pattern)

    def _run_partition_aware(self, table, input_path, table_manifest):
        """Execute partition-aware batched ingestion using coordinator pattern.

        Explicit Fetch → Group → Submit → Commit loop:
        - FETCH:  Resolve files, filter via manifest
        - GROUP:  PartitionBatcher.group_files()
        - SUBMIT: engine.ingest() per batch (engine is "dumb" I/O)
        - COMMIT: table_manifest.update_file() with batch_id + status
        """
        table_name = table['name']
        naming_pattern = table['naming_pattern']
        partition_by = table.get('partition_by', [])
        table_format = table.get('format', 'csv')
        schema = table.get('schema')
        read_options = table.get('read_options', {})
        write_mode = table.get('write_mode', 'overwrite')
        fixed_width_spec = table.get('fixed_width_spec')
        min_line_length = table.get('min_line_length')
        output_name = os.path.splitext(table_name)[0]
        intermediate_dir = self.config.processed_dir
        base_output = os.path.join(intermediate_dir, output_name)

        # ============================================================
        # FETCH: Resolve all input files from glob
        # ============================================================
        all_files = sorted(glob.glob(input_path, recursive=True))
        all_files = [f for f in all_files if os.path.isfile(f)]

        if not all_files:
            logger.warning("No files found matching %s", input_path)
            return

        # Filter through manifest -> pending files only
        # Use _preprocess_root for files in scratch space (avoids basename key collision)
        effective_root = table.get('_preprocess_root', table.get('root'))
        pending = table_manifest.get_pending_files(all_files, effective_root)
        if not pending:
            logger.info("Skipping %s - all %d files already processed", table_name, len(all_files))
            return

        # ============================================================
        # GROUP: Partition-aware batches
        # ============================================================
        batches = PartitionBatcher.group_files(pending, naming_pattern, partition_by)

        # Handle unmatched files
        unmatched = batches.pop(None, [])
        if unmatched:
            logger.warning("%d files didn't match naming_pattern and will be skipped", len(unmatched))
            for f in unmatched[:5]:
                logger.warning("  - %s", os.path.basename(f))
            if len(unmatched) > 5:
                logger.warning("  ... and %d more", len(unmatched) - 5)

        total_batches = len(batches)
        total_files = len(pending) - len(unmatched)
        logger.info("Organized %d files into %d partition batches", total_files, total_batches)

        # ============================================================
        # SUBMIT + COMMIT: Execute each batch with structured logging
        # ============================================================
        success_count = 0
        failed_count = 0

        for batch_num, (partition_values, batch_files) in enumerate(batches.items(), start=1):
            subpath = PartitionBatcher.get_output_subpath(partition_by, partition_values)
            batch_dir = os.path.join(base_output, subpath)
            os.makedirs(batch_dir, exist_ok=True)
            # DuckDB needs a file path (not dir) when partition_by=None
            batch_output = os.path.join(batch_dir, "part-0.parquet")

            # Generate deterministic batch ID
            batch_id = PartitionBatcher.generate_batch_id(table_name, subpath, batch_files)

            # Use BatchLogger for structured context
            with BatchLogger(logger, batch_id=batch_id, partition_key=subpath, file_count=len(batch_files)):
                logger.info(
                    "Processing Batch %d/%d: Partition [%s] - %d files",
                    batch_num, total_batches, subpath, len(batch_files)
                )

                # Build metadata from partition values
                metadata = dict(zip(partition_by, partition_values))
                metadata.update(table.get('metadata', {}))

                # SUBMIT: Engine ingestion
                success = self.engine.ingest(
                    batch_files,
                    batch_output,
                    format=table_format,
                    partition_by=None,  # Already partitioned by batcher
                    balanced=False,
                    metadata=metadata,
                    naming_pattern=None,  # Already extracted by batcher
                    target_rows=self.config.target_rows,
                    target_file_size_mb=self.config.target_file_size_mb,
                    compression=self.config.compression,
                    max_records_per_file=self.config.max_records_per_file,
                    hive_partitioning=False,
                    schema=schema,
                    write_mode=write_mode,
                    output_format=self.config.output_format,
                    fixed_width_spec=fixed_width_spec,
                    min_line_length=min_line_length,
                    **read_options
                )

                # COMMIT: Update manifest with batch_id
                if success:
                    with table_manifest.batch():
                        for f in batch_files:
                            table_manifest.update_file(
                                f, status="success",
                                metadata={"partition": subpath},
                                table_root=effective_root,
                                batch_id=batch_id
                            )
                    success_count += 1
                    logger.info("Batch %d/%d [%s] - SUCCESS", batch_num, total_batches, subpath)
                else:
                    with table_manifest.batch():
                        for f in batch_files:
                            table_manifest.update_file(
                                f, status="failed",
                                metadata={"partition": subpath, "error": "ingestion_failed"},
                                table_root=effective_root,
                                batch_id=batch_id
                            )
                    failed_count += 1
                    logger.error(
                        "Batch %d/%d [%s] - FAILED. Files: %s",
                        batch_num, total_batches, subpath,
                        [os.path.basename(f) for f in batch_files]
                    )

        # Summary
        logger.info(
            "Partition-aware ingestion complete for %s: %d/%d batches succeeded",
            table_name, success_count, total_batches
        )
        if failed_count > 0:
            logger.warning("%d batches failed for %s", failed_count, table_name)

    def dry_run(self):
        """Show matched files without processing - for config verification."""
        logger.info("Dataset: %s", self.config.dataset_name)
        logger.info("Engine: %s", self.config.engine)
        logger.info("Tables: %d", len(self.config.tables))

        for table in self.config.tables:
            logger.info("─────────────────────────────────────────────")
            logger.info("Table: %s", table['name'])
            logger.info("  Format: %s", table.get('format', 'csv'))
            logger.info("  Root: %s", table.get('root', '(not set)'))
            logger.info("  Path: %s", table.get('path'))

            # Resolve files
            path = table.get('path')
            root = table.get('root')

            if root:
                config_dir = os.path.dirname(os.path.abspath(self.config.config_path))
                root_path = os.path.join(config_dir, root) if not os.path.isabs(root) else root
                search_path = os.path.join(root_path, path.lstrip(os.sep)) if path else root_path
            else:
                search_path = path

            if search_path and '*' in search_path:
                matched_files = glob.glob(search_path, recursive=True)
            elif search_path and os.path.exists(search_path):
                if os.path.isdir(search_path):
                    matched_files = glob.glob(os.path.join(search_path, '**/*'), recursive=True)
                    matched_files = [f for f in matched_files if os.path.isfile(f)]
                else:
                    matched_files = [search_path]
            else:
                matched_files = []

            logger.info("  Matched files (%d):", len(matched_files))
            for f in matched_files[:20]:
                logger.info("    - %s", f)
            if len(matched_files) > 20:
                logger.info("    ... and %d more", len(matched_files) - 20)
            if not matched_files:
                logger.info("    (no files matched)")

            # Show preprocessing config if any
            preprocess = table.get('preprocess', {})
            if preprocess:
                logger.info("  Preprocessing: %s", preprocess)

    def summarize(self):
        """Generate summary stats, markdown reports, and export to Redivis.

        Runs independently after ingestion. Reads finalized parquet from curated_dir.
        """
        logger.info("Starting summarization for dataset: %s", self.config.dataset_name)

        for table in self.config.tables:
            table_name = table['name']
            output_name = os.path.splitext(table_name)[0]
            partition_by = table.get('partition_by') or self.config.partition_by

            # Determine finalized path
            if partition_by:
                final_basename = output_name
            else:
                final_basename = f"{output_name}.parquet"
            final_path = os.path.join(self.config.curated_dir, final_basename)

            if not os.path.exists(final_path):
                logger.warning("Skipping summary for %s — no finalized data at %s", table_name, final_path)
                continue

            logger.info("Generating summary for %s...", table_name)

            # Read finalized output (not the in-memory df from ingestion)
            df = self.engine.read_result(final_path)
            is_spark = self.config.engine == 'pyspark'
            if not is_spark and hasattr(df, 'df'):
                df = df.df()

            summary_path = os.path.join(self.config.curated_dir, f"{table_name}_summary.csv")
            report_path = os.path.join(self.config.curated_dir, f"{table_name}_report.md")

            if is_spark:
                summary_df, row_count = Summarizer.generate_summary_spark(df, summary_path)
                # row_count extracted from describe() — no extra df.count() needed
            else:
                summary_df = Summarizer.generate_summary_table(df, summary_path)
                row_count = len(df)

            stats = {
                "row_count": row_count,
                "config_path": getattr(self.config, 'config_path', 'unknown'),
                "status": "success"
            }
            Summarizer.generate_markdown_report(self.config.dataset_name, summary_df, stats, report_path)

            # Update manifest with row_count
            table_manifest = self.manifest_store.get_table_manifest(table_name)
            original_path = table.get('path') or table.get('archives')
            if original_path:
                table_manifest.update_file(original_path, status="success", metadata=stats, table_root=table.get('root'))

            logger.info("Summary complete for %s: %s rows, report at %s", table_name, f"{row_count:,}", report_path)

        logger.info("Summarization complete for dataset: %s", self.config.dataset_name)

    def run(self, skip_summary=False):
        logger.info("Starting ingestion for dataset: %s", self.config.dataset_name)

        if not self.config.tables:
             logger.warning("No tables found to process! Check your config path patterns and ensuring data exists.")
             logger.warning("Configured tables: %s", self.config.data.get('tables', []))

        # --- Phase 0: Run all preprocessing BEFORE engine startup ---
        # This avoids launching Spark (expensive) just for driver-mode preprocessing
        preprocessed_paths = {}
        for table in self.config.tables:
            preprocess_config = table.get('preprocess')
            archives_pattern = table.get('archives')
            if preprocess_config or archives_pattern:
                logger.info("Pre-run preprocessing for %s...", table['name'])
                preprocessed_paths[table['name']] = self._preprocess(table)

        # --- Phase 1: Engine startup and ingestion ---
        # Optimize: Distributed Manifest Check for Cluster Mode
        # Group tables by root to perform batch hashing on the cluster
        if hasattr(self.engine, 'calculate_hashes'):
            cluster_batches = {} # root -> list_of_paths

            for t in self.config.tables:
                 mode = t.get('preprocess', {}).get('execution_mode', 'driver')
                 if mode == 'cluster':
                     root = t.get('root')
                     if root not in cluster_batches:
                         cluster_batches[root] = []
                     cluster_batches[root].append(t['path'])

            for root, paths in cluster_batches.items():
                logger.info("Distributed Check: Calculating hashes for %d items under root '%s' via Spark...", len(paths), root)
                try:
                    results = self.engine.calculate_hashes(paths, table_root=root)
                    for res in results:
                        if not res.get('error'):
                            self._hash_cache[res['path']] = res['hash']
                        else:
                            logger.warning("Hash calculation failed for %s: %s", res['path'], res['error'])
                except Exception as e:
                    logger.error("Distributed hash check failed: %s. Falling back to driver check.", e)

        failed_tables = []
        failed_table_errors = {}
        for table in self.config.tables:
            try:
                # Get per-table manifest for this table
                table_manifest = self.manifest_store.get_table_manifest(table['name'])

                # 0. Preprocessing
                # Preprocess returns a modified path (e.g. to temp unzipped files)
                # Table dict is unmodified, we just change the variable we use for input
                # For archives-only tables (no path key), use archives as the manifest key
                original_path = table.get('path') or table.get('archives')

                # Use pre-computed preprocessing result if available, otherwise run now
                def get_preprocessed_path():
                    if table['name'] in preprocessed_paths:
                        return preprocessed_paths[table['name']]
                    return self._preprocess(table)

                # Partition-aware batching: skip whole-table manifest check,
                # do per-file checking instead in _run_partition_aware()
                if table.get('batch_strategy') == 'partition_aware':
                    input_path = get_preprocessed_path()
                    if input_path != original_path:
                        logger.info("Preprocessing complete. Ingesting from: %s", input_path)
                    self._run_partition_aware(table, input_path, table_manifest)
                    continue

                # --- Bulk mode (default) ---
                # Check manifest BEFORE preprocessing to avoid expensive work (unzipping) if table hasn't changed
                # Using fsspec for URI-aware existance check would be better,
                # for now keeping it simple but URI-ready in engines

                computed_hash = self._hash_cache.get(original_path)
                if not table_manifest.should_process(original_path, table_root=table.get('root'), computed_hash=computed_hash):
                     # Check against ORIGINAL path for manifest, as preprocessed path changes/is temp
                     logger.info("Skipping %s (already processed and hash matches)", table['name'])
                     continue

                input_path = get_preprocessed_path()

                if input_path != original_path:
                    logger.info("Preprocessing complete. Ingesting from: %s", input_path)

                # Discover individual files for manifest tracking (reusing schema_pipeline pattern)
                if '*' in str(input_path):
                    discovered_files = sorted(glob.glob(str(input_path), recursive=True))
                    discovered_files = [f for f in discovered_files if os.path.isfile(f)]
                else:
                    discovered_files = [input_path] if os.path.exists(input_path) else []

                # Use _preprocess_root for files in scratch space (avoids basename key collision)
                effective_root = table.get('_preprocess_root', table.get('root'))

                # Calculate hashes for tracking
                file_hashes = {
                    f: _get_file_hash_static(f, fast_check=True)
                    for f in discovered_files
                }
                logger.debug("Discovered %d files for ingestion", len(discovered_files))

                logger.info("Processing %s...", table['name'])

                output_name = os.path.splitext(table['name'])[0]
                intermediate_dir = self.config.processed_dir
                output_path = os.path.join(intermediate_dir, output_name)

                os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
                partition_by = table.get('partition_by') or self.config.partition_by
            
                if partition_by:
                    output_basename = output_name
                else:
                    # Use configured output format extension (parquet or delta? Delta is usually a directory, so no extension or .delta?)
                    # Delta tables are directories without extensions, others use format extension
                    if self.config.output_format == 'delta':
                         output_basename = output_name
                    else:
                         output_basename = f"{output_name}.{self.config.output_format}"
                
                output_path = os.path.join(intermediate_dir, output_basename)
                metadata = table.get('metadata', {})
                naming_pattern = table.get('naming_pattern')
                table_format = table.get('format', 'csv')
                hive_partitioning = table.get('hive_partitioning', False)
                schema = table.get('schema')
                read_options = table.get('read_options', {})
                write_mode = table.get('write_mode', 'overwrite')
                fixed_width_spec = table.get('fixed_width_spec')
                min_line_length = table.get('min_line_length')

                # For partitioning, DuckDB creates a directory.
                logger.info("Starting ingestion for %s from %s to %s", table['name'], input_path, output_path)
                success = self.engine.ingest(
                    input_path,
                    output_path,
                    format=table_format,
                    partition_by=partition_by,
                    balanced=self.config.salting,  # D.1: Use config value (default False)
                    metadata=metadata,
                    naming_pattern=naming_pattern,
                    target_rows=self.config.target_rows,
                    target_file_size_mb=self.config.target_file_size_mb,  # D.2: File size control
                    compression=self.config.compression,  # D.2: Compression (default snappy)
                    max_records_per_file=self.config.max_records_per_file,  # D.2: Direct control (optional)
                    hive_partitioning=hive_partitioning,
                    schema=schema,
                    write_mode=write_mode,
                    output_format=self.config.output_format,
                    fixed_width_spec=fixed_width_spec,
                    min_line_length=min_line_length,
                    **read_options
                )
            
                if success:
                    # 2. Load for validation and enrichment
                    df = self.engine.read_result(output_path)
                
                    is_spark = self.config.engine == 'pyspark'
                
                    if not is_spark and hasattr(df, 'df'):
                         df = df.df()

                    # 3. Enrichment
                    if self.config.enrichment.get('geocode'):
                        logger.info("Enriching %s with geospatial data...", table['name'])
                        if is_spark:
                            df = Enricher.enrich_spark(df)
                        else:
                            df = Enricher.enrich_dataframe(df)
                
                    # 4. Custom Transformations (Per-Table > Global)
                    transform_script = table.get('transformation') or self.config.data.get('transformation')
                
                    validation_target_path = output_path
                
                    if transform_script:
                        from .transformations.registry import load_transformer
                        TransformerClass = load_transformer(transform_script)
                        if TransformerClass:
                            logger.info("Applying custom transformations from %s...", transform_script)
                            df = TransformerClass(df).transform()
                        
                            if partition_by:
                                processed_basename = f"{output_name}_processed"
                            else:
                                processed_basename = f"{output_name}_processed.parquet"
                            
                            processed_path = os.path.join(self.config.processed_dir, processed_basename)
                        
                            if is_spark:
                                 # Spark writes are actions.
                                 if partition_by:
                                     df.write.mode("overwrite").partitionBy(*partition_by).parquet(processed_path)
                                 else:
                                     df.write.mode("overwrite").parquet(processed_path)
                                 # Reload for validation
                                 df = self.engine.read_result(processed_path)
                            else:
                                # Pandas/DuckDB path
                                if os.path.isdir(processed_path):
                                    shutil.rmtree(processed_path)
                                elif os.path.exists(processed_path):
                                    os.remove(processed_path)
                                
                                os.makedirs(os.path.dirname(processed_path), exist_ok=True)
                                df.to_parquet(processed_path, partition_cols=partition_by)
                        
                            validation_target_path = processed_path
                            # NOTE: For incremental write_mode, we might want to also append this transformation result?
                            # Current limitations: transformation result overwrite logic is simplified.
                            # Assuming transformations are per-batch for now.

                    # 5. Validations — use per-table config (merged with global in config_loader)
                    table_validations = table['validations']  # merged with global in _expand_tables
                    validation_result = Validator.run_all(df, table_validations, is_spark=is_spark)

                    if validation_result.passed:
                        logger.info("Validations passed for %s", table['name'])

                        output_layer = table.get('output_layer', 'curated')

                        if output_layer == 'curated':
                            if partition_by:
                                final_basename = output_name
                            else:
                                final_basename = f"{output_name}.parquet"

                            final_output_path = os.path.join(self.config.curated_dir, final_basename)

                            # Atomic write: write to temp location, then swap.
                            # This prevents data loss if the write fails (disk full, OOM, etc.)
                            temp_final = final_output_path + ".tmp_new"

                            # Recovery: if a previous run was interrupted mid-swap,
                            # restore curated data from the backup
                            old_backup = final_output_path + ".tmp_old"
                            _recover_atomic_swap(final_output_path, old_backup, temp_final)

                            os.makedirs(os.path.dirname(temp_final), exist_ok=True)

                            # Phase 1.5: Type enforcement — produce a typed curated layer.
                            # The processed layer (validation_target_path) has all-STRING columns
                            # for fixed-width tables. enforce_types() reads it and writes a typed
                            # Parquet to temp_final using TRY_CAST (or CAST for strict cols).
                            enforcement_columns = _build_enforcement_schema(table, fixed_width_spec)
                            if enforcement_columns:
                                type_enforcement = table.get('type_enforcement', {})
                                on_fail = type_enforcement.get('on_fail', 'null')
                                logger.info(
                                    "Enforcing types for %s (on_fail=%s, %d typed columns)",
                                    table['name'], on_fail, len(enforcement_columns)
                                )
                                self.engine.enforce_types(
                                    validation_target_path,
                                    temp_final,
                                    enforcement_columns,
                                    on_fail=on_fail,
                                    partition_by=partition_by,
                                    write_mode=write_mode,
                                )
                            else:
                                # No type enforcement — copy processed → curated as-is.
                                if os.path.isdir(validation_target_path):
                                    shutil.copytree(validation_target_path, temp_final)
                                else:
                                    shutil.copy2(validation_target_path, temp_final)

                            # Atomic swap: only remove old data after new data is written
                            if write_mode == 'overwrite' and os.path.exists(final_output_path):
                                os.rename(final_output_path, old_backup)
                                os.rename(temp_final, final_output_path)
                                if os.path.isdir(old_backup):
                                    shutil.rmtree(old_backup)
                                else:
                                    os.remove(old_backup)
                            else:
                                os.rename(temp_final, final_output_path)

                            logger.info("Data finalized at %s", final_output_path)
                        else:
                            # output_layer: processed — data stays in processed/, no curated write
                            logger.info("Table '%s' output_layer=processed, skipping curated write", table['name'])

                        # Record each individual file in manifest (like schema_pipeline does)
                        with table_manifest.batch():
                            for file_path in discovered_files:
                                table_manifest.update_file(
                                    file_path,
                                    status="success",
                                    metadata={
                                        "config_path": getattr(self.config, 'config_path', 'unknown'),
                                        "table_name": table['name'],
                                    },
                                    table_root=effective_root,
                                    computed_hash=file_hashes.get(file_path)
                                )
                        # Also record table-level entry for backward compatibility
                        table_manifest.update_file(
                            original_path,
                            status="success",
                            metadata={"config_path": getattr(self.config, 'config_path', 'unknown')},
                            table_root=table.get('root')
                        )
                    else:
                        errors = [e['message'] for e in validation_result.errors]
                        logger.error("Validations failed for %s: %s", table['name'], errors)
                        # Record each file as failed
                        with table_manifest.batch():
                            for file_path in discovered_files:
                                table_manifest.update_file(
                                    file_path,
                                    status="failed",
                                    metadata={"errors": errors},
                                    table_root=effective_root,
                                    computed_hash=file_hashes.get(file_path)
                                )
                        table_manifest.update_file(original_path, status="failed", metadata={"errors": errors}, table_root=table.get('root'))
                        raise ValueError(f"Validation failed for {table['name']}: {'; '.join(errors)}")

            except Exception as e:
                logger.error("Table '%s' failed: %s", table['name'], e, exc_info=True)
                try:
                    table_manifest = self.manifest_store.get_table_manifest(table['name'])
                    table_manifest.update_file(
                        table.get('path') or table.get('archives'),
                        status="failed",
                        metadata={"error": str(e)},
                        table_root=table.get('root')
                    )
                except Exception:
                    logger.warning("Could not record failure in manifest for table '%s'", table['name'])
                finally:
                    failed_tables.append(table['name'])
                    failed_table_errors[table['name']] = str(e)
                continue

        # Run summarization unless skipped (even if some tables failed)
        if not skip_summary:
            self.summarize()

        if failed_tables:
            error_details = "; ".join(
                f"{t}: {failed_table_errors[t]}" for t in failed_tables if t in failed_table_errors
            )
            raise RuntimeError(
                f"Pipeline completed with {len(failed_tables)} failed table(s): "
                f"{', '.join(failed_tables)}"
                + (f" -- {error_details}" if error_details else "")
            )
