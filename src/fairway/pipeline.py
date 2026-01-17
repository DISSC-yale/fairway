import os
import sys
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

    def run(self):
        print(f"Starting ingestion for dataset: {self.config.dataset_name}")
        
        if not self.config.sources:
             print("WARNING: No sources found to process! Check your config path patterns and ensuring data exists.")
             print(f"  Configured sources: {self.config.data.get('sources', [])}")
             # We could raise an exception here if we want to force failure
             # raise ValueError("No sources found matching patterns in config.")

        for source in self.config.sources:
            input_path = source['path']
            # Using fsspec for URI-aware existance check would be better, 
            # for now keeping it simple but URI-ready in engines
            
            if not self.manifest.should_process(input_path, source_name=source['name']):
                print(f"Skipping {input_path} (already processed and hash matches)")
                continue

            print(f"Processing {source['name']}...")
            
            # Use source name (which is the basename of the file in expanded sources)
            # to create a unique output path
            # Remove extension from filename for output directory/file naming
            output_name = os.path.splitext(source['name'])[0]
            output_path = os.path.join(self.config.storage['intermediate_dir'], output_name)
            
            # Ensure parent storage directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            partition_by = self.config.partition_by
            
            # If partitioned, output path should be a directory without extension
            if partition_by:
                output_basename = output_name
            else:
                output_basename = f"{output_name}.{fmt}"
                
            output_path = os.path.join(self.config.storage['intermediate_dir'], output_basename)
            metadata = source.get('metadata', {})
            source_format = source.get('format', 'csv')
            hive_partitioning = source.get('hive_partitioning', False)
            
            # For partitioning, DuckDB creates a directory. 
            # We use the name of the source (already unique due to expansion)
            success = self.engine.ingest(
                input_path, 
                output_path,
                format=source_format,
                partition_by=partition_by,
                metadata=metadata,
                target_rows=self.config.target_rows,
                hive_partitioning=hive_partitioning
            )
            
            if success:
                # 2. Load for validation and enrichment
                df = self.engine.read_result(output_path)
                
                is_spark = self.config.engine == 'pyspark'
                
                # If DuckDB/Local, convert to Pandas for now to maintain compatibility with existing
                # enrichment/validation until we port those to pure SQL/DuckDB.
                if not is_spark and hasattr(df, 'df'):
                     df = df.df()

                # 3. Enrichment
                if self.config.enrichment.get('geocode'):
                    print(f"Enriching {source['name']} with geospatial data...")
                    if is_spark:
                        df = Enricher.enrich_spark(df)
                    else:
                        df = Enricher.enrich_dataframe(df)
                
                # 4. Custom Transformations (Phase III)
                transform_script = self.config.data.get('transformation')
                if transform_script:
                    from .transformations.registry import load_transformer
                    TransformerClass = load_transformer(transform_script)
                    if TransformerClass:
                        print(f"Applying custom transformations from {transform_script}...")
                        df = TransformerClass(df).transform()

                # Re-save after enrichment and transformation
                if is_spark:
                     # Spark writes are actions.
                     df.write.mode("overwrite").partitionBy(*partition_by) if partition_by else df.write.mode("overwrite").parquet(output_path)
                     # Reload for validation to ensure we validate the materialized data
                     df = self.engine.read_result(output_path)
                else:
                    # Pandas/DuckDB path
                    if os.path.isdir(output_path):
                        import shutil
                        shutil.rmtree(output_path)
                    df.to_parquet(output_path, partition_cols=partition_by)

                # 5. Validations
                if is_spark:
                    l1 = Validator.level1_check_spark(df, self.config.validations)
                    l2 = Validator.level2_check_spark(df, self.config.validations)
                else:
                    l1 = Validator.level1_check(df, self.config.validations)
                    l2 = Validator.level2_check(df, self.config.validations)
                
                if l1['passed'] and l2['passed']:
                    print(f"Validations passed for {source['name']}")
                    
                    # Move/Copy to final directory
                    final_output_path = os.path.join(self.config.storage['final_dir'], os.path.basename(output_path))
                    if os.path.exists(final_output_path):
                        import shutil
                        if os.path.isdir(final_output_path):
                            shutil.rmtree(final_output_path)
                        else:
                            os.remove(final_output_path)
                    
                    import shutil
                    if os.path.isdir(output_path):
                        shutil.copytree(output_path, final_output_path)
                    else:
                        shutil.copy2(output_path, final_output_path)
                    
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
                        "config_path": sys.argv[1],
                        "status": "success"
                    }
                    Summarizer.generate_markdown_report(self.config.dataset_name, summary_df, stats, report_path)

                    self.manifest.update_manifest(input_path, status="success", metadata=stats, source_name=source['name'])
                    
                    # 7. Redivis Export
                    if self.config.redivis:
                        try:
                            print(f"Exporting {source['name']} to Redivis...")
                            exporter = RedivisExporter(self.config.redivis)
                            
                            # Combine stats and regex-extracted metadata
                            rich_metadata = stats.copy()
                            rich_metadata.update(source.get('metadata', {}))
                            rich_metadata['input_path'] = input_path
                            
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
                    self.manifest.update_manifest(input_path, status="failed", metadata={"errors": errors}, source_name=source['name'])
                    raise Exception(f"Validations failed for {source['name']}. Errors: {errors}")

