import os
import glob
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
                              Defaults to data/schemas/{dataset_name}_schema.yaml relative to config file.
            sampling_ratio (float): Fraction of data to scan (Spark only).
        """
        print(f"Starting Schema Discovery Pipeline for dataset: {self.config.dataset_name}")

        # Default output path: data/schemas/{dataset_name}_schema.yaml relative to config file
        if output_path is None:
            config_dir = os.path.dirname(os.path.abspath(self.config.config_path))
            output_path = os.path.join(config_dir, "data", "schemas", f"{self.config.dataset_name}_schema.yaml")
        
        consolidated_schema = {
            "dataset_name": self.config.dataset_name,
            "sources": []
        }

        # Track source info for manifest
        sources_info = []

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

            # Track files used for this source
            if '*' in processed_path:
                files_used = glob.glob(processed_path, recursive=True)
            else:
                files_used = [processed_path] if os.path.exists(processed_path) else []
            file_hashes = [
                self.manifest.get_file_hash(f, fast_check=True)
                for f in files_used if os.path.isfile(f)
            ]

            sources_info.append({
                "name": source['name'],
                "files_used": files_used,
                "file_hashes": file_hashes
            })

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

        # 4. Output Result (always writes to file - default path computed above)
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(output_path, 'w') as f:
            yaml.dump(consolidated_schema, f, sort_keys=False)
        print(f"\nSchema successfully written to: {output_path}")

        # 5. Record schema run in manifest
        self.manifest.record_schema_run(
            dataset_name=self.config.dataset_name,
            sources_info=sources_info,
            output_path=output_path
        )

        return consolidated_schema
