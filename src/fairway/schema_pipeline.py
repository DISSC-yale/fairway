import os
import yaml
from .pipeline import IngestionPipeline

class SchemaDiscoveryPipeline(IngestionPipeline):
    """
    A specialized pipeline for Schema Discovery.
    
    It reuses the IngestionPipeline's robust source discovery and preprocessing logic
    (unzipping, custom scripts, temp handling) but instead of ingesting data, 
    it infers the schema and outputs a consolidated YAML definition.
    """
    
    def run_inference(self, output_path=None, sampling_ratio=0.1):
        """
        Run the discovery pipeline.
        
        Args:
            output_path (str): Path to write the final schema YAML.
            sampling_ratio (float): Fraction of data to scan (Spark only).
        """
        print(f"Starting Schema Discovery Pipeline for dataset: {self.config.dataset_name}")
        
        consolidated_schema = {
            "dataset_name": self.config.dataset_name,
            "sources": []
        }
        
        # 1. Iterate over sources (just like ingestion)
        if not self.config.sources:
            print("WARNING: No sources found in configuration!")

        for source in self.config.sources:
            print(f"\nProcessing source: {source['name']}")
            print(f"  Source config: {source}")
            
            # 2. Preprocess (Unzip/Script) - Reuses Ingestion Logic!
            # Because of deterministic hashing in pipeline.py, this will REUSE 
            # files if they were already unzipped by a previous run.
            processed_path = self._preprocess(source)
            print(f"  Preprocess returned path: {processed_path}")
            
            # 3. Infer Schema
            print(f"Inferring schema from: {processed_path}")
            
            # We treat the processed path as the input for inference
            # If it's a list (glob results), we might need to handle it.
            # _preprocess returns a single path string (which might be a glob or dir).
            
            try:
                # Use the configured engine (Spark or DuckDB) to infer
                # We extend the engine interface slightly here
                schema_dict = self.engine.infer_schema(
                    path=processed_path,
                    format=source.get('format', 'parquet'), # heuristic, engine handles better usually
                    sampling_ratio=sampling_ratio
                )
                
                source_schema = {
                    "name": source['name'],
                    "schema": schema_dict
                }
                consolidated_schema["sources"].append(source_schema)
                
            except Exception as e:
                print(f"ERROR: Failed to infer schema for source {source['name']}: {e}")
                # Continue to next source?
        
        # 4. Output Result
        if output_path:
            with open(output_path, 'w') as f:
                yaml.dump(consolidated_schema, f, sort_keys=False)
            print(f"\nSchema successfully written to: {output_path}")
        else:
            print("\n--- Inferred Schema ---")
            print(yaml.dump(consolidated_schema, sort_keys=False))
            
        return consolidated_schema
