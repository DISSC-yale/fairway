import os
import sys
import glob
import zipfile
import importlib.util
from .config_loader import Config
from .manifest import ManifestManager
from .engines.duckdb_engine import DuckDBEngine
from .engines.pyspark_engine import PySparkEngine
from .validations.checks import Validator
from .summarize import Summarizer
from .enrichments.geospatial import Enricher
from .exporters.redivis_exporter import RedivisExporter

class IngestionPipeline:
    def __init__(self, config_path, spark_master=None):
        print("DEBUG: Loading local fairway.pipeline")
        self.config = Config(config_path)
        self.manifest = ManifestManager()
        self.engine = self._get_engine(spark_master)
        self._hash_cache = {} # Cache for distributed hash results

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

    def _preprocess(self, source):
        """
        Handles preprocessing (unzipping, custom scripts) before ingestion.
        Returns the path to the processed data (or original path if no preprocess).
        """
        preprocess_config = source.get('preprocess')
        if not preprocess_config:
            return source['path']

        action = preprocess_config.get('action')
        scope = preprocess_config.get('scope', 'per_file') # 'global' or 'per_file'
        mode = preprocess_config.get('execution_mode', 'driver') # 'driver' or 'cluster'
        
        print(f"Preprocessing {source['name']} with action='{action}', scope='{scope}', mode='{mode}'...")
        
        # Resolve input files
        # If input_path is a glob, we get all files.
        input_path = source['path']
        
        # New in V2: Handle Root Resolution
        root = source.get('root')
        full_input_path = input_path
        
        if root and not os.path.isabs(input_path):
             full_input_path = os.path.join(root, input_path)
             
        # Resolve glob against the FULL path (root + relative_glob)
        files = glob.glob(full_input_path) if '*' in full_input_path else [full_input_path]
        
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
             results = self.engine.distribute_task(files, process_file)
             processed_paths = results
        else:
             # Driver mode
             for f in files:
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
        # Let's assume we return the glob of the preprocessed directories
        if len(processed_paths) == 1:
             return processed_paths[0]
        else:
             # If multiple, return the common structure... 
             # This is tricky if they are scattered. 
             # For common case: data/*.zip -> data/.preprocessed_*/
             # Return data/.preprocessed_*/* (contents)
             base = os.path.dirname(input_path)
             return os.path.join(base, ".preprocessed_*", "*") 

    def run(self):
        print(f"Starting ingestion for dataset: {self.config.dataset_name}")
        
        if not self.config.sources:
             print("WARNING: No sources found to process! Check your config path patterns and ensuring data exists.")
             print(f"  Configured sources: {self.config.data.get('sources', [])}")

        
        # Optimize: Distributed Manifest Check for Cluster Mode
        # Group sources by root to perform batch hashing on the cluster
        if hasattr(self.engine, 'calculate_hashes'):
            cluster_batches = {} # root -> list_of_paths
            
            for s in self.config.sources:
                 mode = s.get('preprocess', {}).get('execution_mode', 'driver')
                 if mode == 'cluster':
                     root = s.get('root')
                     if root not in cluster_batches:
                         cluster_batches[root] = []
                     cluster_batches[root].append(s['path'])
            
            for root, paths in cluster_batches.items():
                print(f"Distributed Check: Calculating hashes for {len(paths)} items under root '{root}' via Spark...")
                try:
                    results = self.engine.calculate_hashes(paths, source_root=root)
                    for res in results:
                        if not res.get('error'):
                            self._hash_cache[res['path']] = res['hash']
                        else:
                            print(f"WARNING: Hash calculation failed for {res['path']}: {res['error']}")
                except Exception as e:
                    print(f"ERROR: Distributed hash check failed: {e}. Falling back to driver check.")

        for source in self.config.sources:
            # 0. Preprocessing
            # Preprocess returns a modified path (e.g. to temp unzipped files)
            # Source dict is unmodified, we just change the variable we use for input
            original_path = source['path']
            # Check manifest BEFORE preprocessing to avoid expensive work (unzipping) if source hasn't changed
            # Using fsspec for URI-aware existance check would be better, 
            # for now keeping it simple but URI-ready in engines
            
            computed_hash = self._hash_cache.get(original_path)
            if not self.manifest.should_process(original_path, source_name=source['name'], source_root=source.get('root'), computed_hash=computed_hash):
                 # Check against ORIGINAL path for manifest, as preprocessed path changes/is temp
                 print(f"Skipping {source['name']} (already processed and hash matches)")
                 continue

            input_path = self._preprocess(source)
            
            if input_path != original_path:
                print(f"  Preprocessing complete. Ingesting from: {input_path}")

            print(f"Processing {source['name']}...")
            
            output_name = os.path.splitext(source['name'])[0]
            output_path = os.path.join(self.config.storage['intermediate_dir'], output_name)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            partition_by = source.get('partition_by') or self.config.partition_by
            
            if partition_by:
                output_basename = output_name
            else:
                output_basename = f"{output_name}.parquet"
                
            output_path = os.path.join(self.config.storage['intermediate_dir'], output_basename)
            metadata = source.get('metadata', {})
            source_format = source.get('format', 'csv')
            hive_partitioning = source.get('hive_partitioning', False)
            schema = source.get('schema')
            read_options = source.get('read_options', {})
            write_mode = source.get('write_mode', 'overwrite')
            
            # For partitioning, DuckDB creates a directory. 
            success = self.engine.ingest(
                input_path, 
                output_path,
                format=source_format,
                partition_by=partition_by,
                metadata=metadata,
                target_rows=self.config.target_rows,
                hive_partitioning=hive_partitioning,
                schema=schema,
                write_mode=write_mode,
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
                    print(f"Enriching {source['name']} with geospatial data...")
                    if is_spark:
                        df = Enricher.enrich_spark(df)
                    else:
                        df = Enricher.enrich_dataframe(df)
                
                # 4. Custom Transformations (Per-File > Global)
                transform_script = source.get('transformation') or self.config.data.get('transformation')
                
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
                    print(f"Validations passed for {source['name']}")
                    
                    # Move/Copy to final directory logic ... (Simplified from previous viewing)
                    # For brevity in this replacement, retaining key parts
                    final_basename = f"{output_name}.{source_format}" if not partition_by else output_name
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
                    summary_path = os.path.join(self.config.storage['final_dir'], f"{source['name']}_summary.csv")
                    report_path = os.path.join(self.config.storage['final_dir'], f"{source['name']}_report.md")
                    
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

                    self.manifest.update_manifest(original_path, status="success", metadata=stats, source_name=source['name'], source_root=source.get('root'))
                    
                    # 7. Redivis Export
                    if self.config.redivis:
                        try:
                            print(f"Exporting {source['name']} to Redivis...")
                            exporter = RedivisExporter(self.config.redivis)
                            
                            # Combine stats and regex-extracted metadata
                            rich_metadata = stats.copy()
                            rich_metadata.update(source.get('metadata', {}))
                            rich_metadata['input_path'] = original_path 
                            
                            exporter.upload_table(
                                table_name=source['name'],
                                file_path=output_path,
                                metadata=rich_metadata,
                                schema=source.get('schema')
                            )
                            # Sync dataset level metadata if available
                            exporter.update_dataset_metadata(description=f"Dataset: {self.config.dataset_name}")
                        except Exception as e:
                            print(f"Redivis export failed for {source['name']}: {e}")
                else:
                    errors = l1['errors'] + l2['errors']
                    print(f"Validations failed for {source['name']}: {errors}")
                    self.manifest.update_manifest(original_path, status="failed", metadata={"errors": errors}, source_name=source['name'], source_root=source.get('root'))
                    # raise Exception(...) # Optional blocking

