import os
import sys
from .config_loader import Config
from .manifest import ManifestManager
from .engines.duckdb_engine import DuckDBEngine

from .validations.checks import Validator
from .summarize import Summarizer
from .enrichments.geospatial import Enricher
from .exporters.redivis_exporter import RedivisExporter

class IngestionPipeline:
    def __init__(self, config_path, spark_master=None):
        self.config = Config(config_path)
        self.manifest = ManifestManager()
        self.engine = self._get_engine(spark_master)

    def _get_engine(self, spark_master=None):
        if self.config.engine == 'pyspark':
            try:
                from .engines.pyspark_engine import PySparkEngine
                return PySparkEngine(spark_master)
            except ImportError:
                sys.exit("Error: PySpark is not installed. Please install using `pip install fairway[spark]`")
        return DuckDBEngine()

    def run(self):
        print(f"Starting ingestion for dataset: {self.config.dataset_name}")
        
        for source in self.config.sources:
            input_path = source['path']
            # Using fsspec for URI-aware existance check would be better, 
            # for now keeping it simple but URI-ready in engines
            
            if not self.manifest.should_process(input_path):
                print(f"Skipping {input_path} (already processed and hash matches)")
                continue

            print(f"Processing {source['name']}...")
            
            # Use source name (which is the basename of the file in expanded sources)
            # to create a unique output path
            # Remove extension from filename for output directory/file naming
            output_name = os.path.splitext(source['name'])[0]
            output_path = os.path.join(self.config.storage['intermediate_dir'], output_name)
            partition_by = self.config.partition_by
            metadata = source.get('metadata', {})
            source_format = source.get('format', 'csv')
            
            # For partitioning, DuckDB creates a directory. 
            # We use the name of the source (already unique due to expansion)
            success = self.engine.ingest(
                input_path, 
                output_path,
                format=source_format,
                partition_by=partition_by,
                metadata=metadata,
                target_rows=self.config.target_rows
            )
            
            if success:
                # 2. Load for validation and enrichment
                # If partitioned, output_path is a directory
                df = self.engine.query(f"SELECT * FROM '{output_path}/**/*.parquet'")
                
                # 3. Enrichment
                if self.config.enrichment.get('geocode'):
                    print(f"Enriching {source['name']} with geospatial data...")
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
                # For simplicity in local dev, we overwrite the directory if it exists
                # In production (Spark/Slurm), the engine Handles this
                if os.path.isdir(output_path):
                    import shutil
                    shutil.rmtree(output_path)
                df.to_parquet(output_path, partition_cols=partition_by)
                
                # 5. Validations (Level 1, 2, 3)
                l1 = Validator.level1_check(df, self.config.validations)
                l2 = Validator.level2_check(df, self.config.validations)
                
                if l1['passed'] and l2['passed']:
                    print(f"Validations passed for {source['name']}")
                    
                    # 6. Summarization and Reporting
                    summary_path = os.path.join(self.config.storage['final_dir'], f"{source['name']}_summary.csv")
                    report_path = os.path.join(self.config.storage['final_dir'], f"{source['name']}_report.md")
                    
                    summary_df = Summarizer.generate_summary_table(df, summary_path)
                    
                    stats = {
                        "row_count": len(df),
                        "config_path": sys.argv[1],
                        "status": "success"
                    }
                    Summarizer.generate_markdown_report(self.config.dataset_name, summary_df, stats, report_path)

                    self.manifest.update_manifest(input_path, status="success", metadata=stats)
                    
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
                            exporter.update_dataset_metadata(description=f"Frolf dataset: {self.config.dataset_name}")
                        except Exception as e:
                            print(f"Redivis export failed for {source['name']}: {e}")
                else:
                    errors = l1['errors'] + l2['errors']
                    print(f"Validations failed for {source['name']}: {errors}")
                    self.manifest.update_manifest(input_path, status="failed", metadata={"errors": errors})

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Path to config file")
    parser.add_argument("--spark_master", help="Spark master URL", default=None)
    args = parser.parse_args()
    
    pipeline = IngestionPipeline(args.config, spark_master=args.spark_master)
    pipeline.run()
