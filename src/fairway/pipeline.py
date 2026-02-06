import os
import sys
import glob
import zipfile
import tarfile
import hashlib
import importlib.util
import logging
from .config_loader import Config
from .manifest import ManifestStore, _get_file_hash_static
from .batcher import PartitionBatcher
from .engines.duckdb_engine import DuckDBEngine
from .engines.pyspark_engine import PySparkEngine
from .validations.checks import Validator
from .summarize import Summarizer
from .enrichments.geospatial import Enricher
from .exporters.redivis_exporter import RedivisExporter
from .logging_config import BatchLogger

logger = logging.getLogger("fairway.pipeline")


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
            import shutil
            output_name = os.path.basename(archive_path)[:-3]
            # Validate output path
            output_file = self._safe_extract_path(dest_dir, output_name)
            with gzip.open(archive_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            raise ValueError(f"Unsupported archive type: {archive_path}")


class IngestionPipeline:
    def __init__(self, config_path, spark_master=None, engine_override=None):
        logger.debug("Loading local fairway.pipeline")
        self.config = Config(config_path)
        self.manifest_store = ManifestStore()
        self.engine = self._get_engine(spark_master, engine_override)
        self._hash_cache = {}  # Cache for distributed hash results
        self.archive_cache = ArchiveCache(self.config, self.manifest_store.global_manifest)

    def _get_engine(self, spark_master=None, engine_override=None):
        # CLI override takes precedence over config
        engine_type = engine_override or (self.config.engine.lower() if self.config.engine else 'duckdb')

        if engine_type in ['pyspark', 'spark']:
            try:
                from .engines.pyspark_engine import PySparkEngine
                return PySparkEngine(spark_master)
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

        # Temp Location Support
        temp_loc = self.config.temp_dir
        batch_dir = None
        if temp_loc:
             import hashlib
             # Generate a deterministic batch directory based on the table name
             # This allows reusing preprocessed files across runs (schema gen -> ingestion) and is human readable.
             safe_name = "".join([c if c.isalnum() else "_" for c in table['name']])
             batch_dir = os.path.join(temp_loc, f"{safe_name}_v1")
             # Ensure the batch root exists
             os.makedirs(batch_dir, exist_ok=True)
             logger.info("Using global temp location: %s", batch_dir)
        
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
             import shutil
             
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
                 
                 # Dynamic import
                 spec = importlib.util.spec_from_file_location("custom_module", action)
                 if spec and spec.loader:
                     module = importlib.util.module_from_spec(spec)
                     spec.loader.exec_module(module)
                     if hasattr(module, 'process'):
                         return module.process(file_path)
                 return file_path
             
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
             # Single extraction directory - apply filter directly
             if include_pattern:
                 result_path = os.path.join(processed_paths[0], "**", file_glob)
             else:
                 result_path = processed_paths[0]
        else:
             # If multiple, return the common structure...
             if batch_dir:
                  # If we used a batch dir, return everything inside it
                  # Structure: batch_dir / <file_dir> / <contents>
                  result_path = os.path.join(batch_dir, "*", file_glob)
             else:
                  # Original logic: sibling directories
                  base = os.path.dirname(input_path)
                  result_path = os.path.join(base, ".preprocessed_*", file_glob)

        # Record result for future reuse
        table_manifest.record_preprocessing(
            original_path=table['path'],
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

        # If single archive, return glob pattern
        return result_path

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
        pending = table_manifest.get_pending_files(all_files, table.get('root'))
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
                    **read_options
                )

                # COMMIT: Update manifest with batch_id
                if success:
                    with table_manifest.batch():
                        for f in batch_files:
                            table_manifest.update_file(
                                f, status="success",
                                metadata={"partition": subpath},
                                table_root=table.get('root'),
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
                                table_root=table.get('root'),
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

    def run(self):
        logger.info("Starting ingestion for dataset: %s", self.config.dataset_name)

        if not self.config.tables:
             logger.warning("No tables found to process! Check your config path patterns and ensuring data exists.")
             logger.warning("Configured tables: %s", self.config.data.get('tables', []))

        
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

        for table in self.config.tables:
            # Get per-table manifest for this table
            table_manifest = self.manifest_store.get_table_manifest(table['name'])

            # 0. Preprocessing
            # Preprocess returns a modified path (e.g. to temp unzipped files)
            # Table dict is unmodified, we just change the variable we use for input
            original_path = table['path']

            # Partition-aware batching: skip whole-table manifest check,
            # do per-file checking instead in _run_partition_aware()
            if table.get('batch_strategy') == 'partition_aware':
                input_path = self._preprocess(table)
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

            input_path = self._preprocess(table)

            if input_path != original_path:
                logger.info("Preprocessing complete. Ingesting from: %s", input_path)

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
                # Standard convention for Delta tables is just the directory name, but if we need an extension for clarity:
                ext = self.config.output_format if self.config.output_format != 'delta' else 'delta' 
                # Actually delta tables usually don't have extensions in path, they are directories.
                # But to avoid collision with file names...
                # Current logic: output_basename = f"{output_name}.parquet"
                
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
                             df.write.mode("overwrite").partitionBy(*partition_by) if partition_by else df.write.mode("overwrite").parquet(processed_path)
                             # Reload for validation
                             df = self.engine.read_result(processed_path)
                        else:
                            # Pandas/DuckDB path
                            import shutil
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

                # 5. Validations
                if is_spark:
                    l1 = Validator.level1_check_spark(df, self.config.validations)
                    l2 = Validator.level2_check_spark(df, self.config.validations)
                else:
                    l1 = Validator.level1_check(df, self.config.validations)
                    l2 = Validator.level2_check(df, self.config.validations)
                
                if l1['passed'] and l2['passed']:
                    logger.info("Validations passed for %s", table['name'])
                    
                    # Move/Copy to final directory logic ... (Simplified from previous viewing)
                    # For brevity in this replacement, retaining key parts
                    final_basename = f"{output_name}.{table_format}" if not partition_by else output_name
                    if partition_by:
                        final_basename = output_name
                    else:
                        final_basename = f"{output_name}.parquet"

                    final_output_path = os.path.join(self.config.curated_dir, final_basename)
                    
                    # Logic for write_mode in finalization?
                    # If append, we shouldn't wipe the final directory if it exists.
                    # But if we copy over... 
                    # If write_mode == append, we rely on the engine having already done the needful in intermediate?
                    # Actually, usually intermediate is transient. Final is persistent.
                    # If we ingested 1 file related to 'MyDataset', and we append...
                    # The intermediate output_path was likely specific to that file or batch.
                    # The final path is usually the dataset path.
                    # This logic needs refinement for full incremental support, 
                    # but for now we follow the existing pattern: 
                    # Intermediate -> Validate -> Copy to Final.
                    
                    # If overwrite (default), we clear final.
                    if write_mode == 'overwrite':
                        if os.path.exists(final_output_path):
                            import shutil
                            if os.path.isdir(final_output_path):
                                shutil.rmtree(final_output_path)
                            else:
                                os.remove(final_output_path)
                    
                    import shutil
                    # If append, and final exists, we merge? 
                    # Parquet directory merge is handled by engine logic usually.
                    # Simple Copy might overwrite if names conflict or directory structure differs.
                    # For now: Standard copy.
                    
                    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)

                    if os.path.isdir(validation_target_path):
                        shutil.copytree(validation_target_path, final_output_path, dirs_exist_ok=True)
                    else:
                        shutil.copy2(validation_target_path, final_output_path)
                    
                    logger.info("Data finalized at %s", final_output_path)

                    # 6. Summarization and Reporting
                    summary_path = os.path.join(self.config.curated_dir, f"{table['name']}_summary.csv")
                    report_path = os.path.join(self.config.curated_dir, f"{table['name']}_report.md")
                    
                    if is_spark:
                         summary_df = Summarizer.generate_summary_spark(df, summary_path)
                         # Count for stats
                         row_count = df.count()
                    else:
                         summary_df = Summarizer.generate_summary_table(df, summary_path)
                         row_count = len(df)
                    
                    stats = {
                        "row_count": row_count,
                        "config_path": getattr(self.config, 'config_path', 'unknown'),
                        "status": "success"
                    }
                    Summarizer.generate_markdown_report(self.config.dataset_name, summary_df, stats, report_path)

                    table_manifest.update_file(original_path, status="success", metadata=stats, table_root=table.get('root'))
                    
                    # 7. Redivis Export
                    if self.config.redivis:
                        try:
                            logger.info("Exporting %s to Redivis...", table['name'])
                            exporter = RedivisExporter(self.config.redivis)

                            # Combine stats and regex-extracted metadata
                            rich_metadata = stats.copy()
                            rich_metadata.update(table.get('metadata', {}))
                            rich_metadata['input_path'] = original_path

                            exporter.upload_table(
                                table_name=table['name'],
                                file_path=output_path,
                                metadata=rich_metadata,
                                schema=table.get('schema')
                            )
                            # Sync dataset level metadata if available
                            exporter.update_dataset_metadata(description=f"Dataset: {self.config.dataset_name}")
                        except Exception as e:
                            logger.error("Redivis export failed for %s: %s", table['name'], e)
                else:
                    errors = l1['errors'] + l2['errors']
                    logger.error("Validations failed for %s: %s", table['name'], errors)
                    table_manifest.update_file(original_path, status="failed", metadata={"errors": errors}, table_root=table.get('root'))
                    # raise Exception(...) # Optional blocking

