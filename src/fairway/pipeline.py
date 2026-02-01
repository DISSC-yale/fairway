import os
import sys
import glob
import zipfile
import tarfile
import hashlib
import importlib.util
from .config_loader import Config
from .manifest import ManifestManager, ManifestStore, _get_file_hash_static
from .engines.duckdb_engine import DuckDBEngine
from .engines.pyspark_engine import PySparkEngine
from .validations.checks import Validator
from .summarize import Summarizer
from .enrichments.geospatial import Enricher
from .exporters.redivis_exporter import RedivisExporter


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
            print(f"  Using cached extraction for: {archive_path}")
            self._session_cache[abs_path] = entry['extracted_dir']
            return entry['extracted_dir']

        # Need to extract
        extraction_dir = self._get_extraction_dir(archive_path)
        os.makedirs(extraction_dir, exist_ok=True)

        print(f"  Extracting archive: {archive_path} → {extraction_dir}")
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

        # Use temp_location from config, or default to .fairway_cache
        base_dir = self.config.temp_location or os.path.join(os.getcwd(), '.fairway_cache')
        return os.path.join(base_dir, 'archives', f"{archive_name}_{short_hash}")

    def _extract(self, archive_path, dest_dir):
        """Extract archive based on type."""
        if archive_path.endswith('.zip'):
            with zipfile.ZipFile(archive_path, 'r') as zf:
                zf.extractall(dest_dir)
        elif archive_path.endswith(('.tar.gz', '.tgz')):
            with tarfile.open(archive_path, 'r:gz') as tf:
                tf.extractall(dest_dir)
        elif archive_path.endswith('.tar'):
            with tarfile.open(archive_path, 'r') as tf:
                tf.extractall(dest_dir)
        elif archive_path.endswith('.gz') and not archive_path.endswith('.tar.gz'):
            import gzip
            import shutil
            output_file = os.path.join(dest_dir, os.path.basename(archive_path)[:-3])
            with gzip.open(archive_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            raise ValueError(f"Unsupported archive type: {archive_path}")


class IngestionPipeline:
    def __init__(self, config_path, spark_master=None):
        print("DEBUG: Loading local fairway.pipeline")
        self.config = Config(config_path)
        self.manifest_store = ManifestStore()
        # Keep self.manifest for backward compatibility during transition
        self.manifest = ManifestManager()
        self.engine = self._get_engine(spark_master)
        self._hash_cache = {}  # Cache for distributed hash results
        self.archive_cache = ArchiveCache(self.config, self.manifest_store.global_manifest)

    def _get_engine(self, spark_master=None):
        engine_type = self.config.engine.lower() if self.config.engine else 'duckdb'
        
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
            raise ValueError(f"Unknown engine: {self.config.engine}. Supported engines: 'duckdb', 'spark'") 

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
            return table['path']

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
            print(f"  Reusing cached preprocessing: {cached}")
            return cached

        print(f"Preprocessing {table['name']} with action='{action}', scope='{scope}', mode='{mode}'...")
        if include_pattern:
            print(f"  File filter: {include_pattern}")

        # Temp Location Support
        temp_loc = self.config.temp_location
        batch_dir = None
        if temp_loc:
             import hashlib
             # Generate a deterministic batch directory based on the table name
             # This allows reusing preprocessed files across runs (schema gen -> ingestion) and is human readable.
             safe_name = "".join([c if c.isalnum() else "_" for c in table['name']])
             batch_dir = os.path.join(temp_loc, f"{safe_name}_v1")
             # Ensure the batch root exists
             os.makedirs(batch_dir, exist_ok=True)
             print(f"INFO: Using global temp location: {batch_dir}")
        
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
        
        print(f"DEBUG: Found {len(files)} candidates at {full_input_path}")
        
        if not files:
             print(f"WARNING: No files found for preprocessing at {full_input_path} (CWD: {os.getcwd()})")
             if root:
                 print(f"  Root: {root}, Path: {input_path}")
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
                 raise ValueError("execution_mode='cluster' requires a distributed engine (PySpark).")
             
             # Dispatch to cluster
             # Note: For custom scripts, ensure the script file is accessible on workers
             print(f"INFO: Distributing preprocessing task for {len(files)} files to Spark cluster...")
             results = self.engine.distribute_task(files, process_file)
             print(f"INFO: Cluster task complete. Processed {len(results)} items.")
             processed_paths = results
        else:
             # Driver mode
             print(f"INFO: Running preprocessing locally for {len(files)} files...")
             for i, f in enumerate(files):
                 if i % 10 == 0:
                     print(f"  Processed {i}/{len(files)}...")
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

        print(f"Processing archives for {table['name']}...")

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

        print(f"  Found {len(archive_files)} archive(s)")

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

        print(f"  Files matching '{files_pattern}': {len(all_matched_files)}")

        if not all_matched_files:
            print(f"  WARNING: No files match pattern '{files_pattern}' in extracted archives")
            # Return pattern for first dir (will be empty)
            return os.path.join(all_extracted_dirs[0], files_pattern)

        # Store all matched files for the engine to process
        table['_extracted_files'] = all_matched_files

        # If single archive, return glob pattern
        return result_path

    def dry_run(self):
        """Show matched files without processing - for config verification."""
        print(f"Dataset: {self.config.dataset_name}")
        print(f"Engine: {self.config.engine}")
        print(f"Tables: {len(self.config.tables)}\n")

        for table in self.config.tables:
            print(f"─────────────────────────────────────────────")
            print(f"Table: {table['name']}")
            print(f"  Format: {table.get('format', 'csv')}")
            print(f"  Root: {table.get('root', '(not set)')}")
            print(f"  Path: {table.get('path')}")

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

            print(f"  Matched files ({len(matched_files)}):")
            for f in matched_files[:20]:
                print(f"    - {f}")
            if len(matched_files) > 20:
                print(f"    ... and {len(matched_files) - 20} more")
            if not matched_files:
                print(f"    (no files matched)")

            # Show preprocessing config if any
            preprocess = table.get('preprocess', {})
            if preprocess:
                print(f"  Preprocessing: {preprocess}")

            print()

    def run(self):
        print(f"Starting ingestion for dataset: {self.config.dataset_name}")

        if not self.config.tables:
             print("WARNING: No tables found to process! Check your config path patterns and ensuring data exists.")
             print(f"  Configured tables: {self.config.data.get('tables', [])}")

        
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
                print(f"Distributed Check: Calculating hashes for {len(paths)} items under root '{root}' via Spark...")
                try:
                    results = self.engine.calculate_hashes(paths, table_root=root)
                    for res in results:
                        if not res.get('error'):
                            self._hash_cache[res['path']] = res['hash']
                        else:
                            print(f"WARNING: Hash calculation failed for {res['path']}: {res['error']}")
                except Exception as e:
                    print(f"ERROR: Distributed hash check failed: {e}. Falling back to driver check.")

        for table in self.config.tables:
            # Get per-table manifest for this table
            table_manifest = self.manifest_store.get_table_manifest(table['name'])

            # 0. Preprocessing
            # Preprocess returns a modified path (e.g. to temp unzipped files)
            # Table dict is unmodified, we just change the variable we use for input
            original_path = table['path']
            # Check manifest BEFORE preprocessing to avoid expensive work (unzipping) if table hasn't changed
            # Using fsspec for URI-aware existance check would be better,
            # for now keeping it simple but URI-ready in engines

            computed_hash = self._hash_cache.get(original_path)
            if not table_manifest.should_process(original_path, table_root=table.get('root'), computed_hash=computed_hash):
                 # Check against ORIGINAL path for manifest, as preprocessed path changes/is temp
                 print(f"Skipping {table['name']} (already processed and hash matches)")
                 continue

            input_path = self._preprocess(table)

            if input_path != original_path:
                print(f"  Preprocessing complete. Ingesting from: {input_path}")

            print(f"Processing {table['name']}...")

            output_name = os.path.splitext(table['name'])[0]
            # D.4: Use scratch_dir for intermediate files if configured
            intermediate_dir = self.config.scratch_dir or self.config.storage['intermediate_dir']
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

            # For partitioning, DuckDB creates a directory.
            print(f"INFO: Starting ingestion for {table['name']} from {input_path} to {output_path}")
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
                    print(f"Enriching {table['name']} with geospatial data...")
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
                        print(f"Applying custom transformations from {transform_script}...")
                        df = TransformerClass(df).transform()
                        
                        if partition_by:
                            processed_basename = f"{output_name}_processed"
                        else:
                            processed_basename = f"{output_name}_processed.parquet"
                            
                        processed_path = os.path.join(self.config.storage['intermediate_dir'], processed_basename)
                        
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
                    print(f"Validations passed for {table['name']}")
                    
                    # Move/Copy to final directory logic ... (Simplified from previous viewing)
                    # For brevity in this replacement, retaining key parts
                    final_basename = f"{output_name}.{table_format}" if not partition_by else output_name
                    if partition_by:
                        final_basename = output_name
                    else:
                        final_basename = f"{output_name}.parquet"

                    final_output_path = os.path.join(self.config.storage['final_dir'], final_basename)
                    
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
                    
                    print(f"Data finalized at {final_output_path}")

                    # 6. Summarization and Reporting
                    summary_path = os.path.join(self.config.storage['final_dir'], f"{table['name']}_summary.csv")
                    report_path = os.path.join(self.config.storage['final_dir'], f"{table['name']}_report.md")
                    
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
                            print(f"Exporting {table['name']} to Redivis...")
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
                            print(f"Redivis export failed for {table['name']}: {e}")
                else:
                    errors = l1['errors'] + l2['errors']
                    print(f"Validations failed for {table['name']}: {errors}")
                    table_manifest.update_file(original_path, status="failed", metadata={"errors": errors}, table_root=table.get('root'))
                    # raise Exception(...) # Optional blocking

