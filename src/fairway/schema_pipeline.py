import os
import glob
import re
import yaml
from .pipeline import IngestionPipeline
from .manifest import _get_file_hash_static


def _get_metadata_columns_from_pattern(naming_pattern):
    """Extract named group names from a naming_pattern regex.

    Args:
        naming_pattern: A regex pattern with named groups like (?P<name>...)

    Returns:
        List of named group names extracted from the pattern.
    """
    if not naming_pattern:
        return []
    return re.findall(r'\?P<([^>]+)>', naming_pattern)

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
            output_path (str): Base path for schema output. Each source schema is written to
                              {output_path}/{source_name}/schema.yaml.
                              Defaults to data/schemas relative to config file.
            sampling_ratio (float): Fraction of data to scan (Spark only).
        """
        print(f"Starting Schema Discovery Pipeline for dataset: {self.config.dataset_name}")

        # Default output path: schema/ relative to config file
        if output_path is None:
            config_dir = os.path.dirname(os.path.abspath(self.config.config_path))
            output_path = os.path.join(config_dir, "schema")
        
        consolidated_schema = {
            "dataset_name": self.config.dataset_name,
            "tables": []
        }

        # Track table info for manifest
        tables_info = []

        # 1. Iterate over tables (just like ingestion)
        if not self.config.tables:
            print("WARNING: No tables found in configuration!")

        for table in self.config.tables:
            print(f"\nProcessing table: {table['name']}")
            print(f"  Table config: {table}")

            # 2. Preprocess (Unzip/Script) - Reuses Ingestion Logic!
            # Because of deterministic hashing in pipeline.py, this will REUSE
            # files if they were already unzipped by a previous run.
            processed_path = self._preprocess(table)
            print(f"  Preprocess returned path: {processed_path}")

            # Track files used for this table
            if '*' in processed_path:
                files_used = glob.glob(processed_path, recursive=True)
            else:
                files_used = [processed_path] if os.path.exists(processed_path) else []
            file_hashes = [
                _get_file_hash_static(f, fast_check=True)
                for f in files_used if os.path.isfile(f)
            ]

            # Store for later recording in per-table manifest
            tables_info.append({
                "name": table['name'],
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
                    format=table.get('format', 'parquet'), # heuristic, engine handles better usually
                    sampling_ratio=sampling_ratio
                )

                # Add metadata columns from naming_pattern (as STRING, matching injection behavior)
                naming_pattern = table.get('naming_pattern')
                metadata_columns = _get_metadata_columns_from_pattern(naming_pattern)
                for col in metadata_columns:
                    if col not in schema_dict:
                        schema_dict[col] = 'STRING'

                # Also ensure partition_by columns are in the schema
                partition_by = table.get('partition_by', [])
                for col in partition_by:
                    if col not in schema_dict:
                        schema_dict[col] = 'STRING'

                table_schema = {
                    "name": table['name'],
                    "schema": schema_dict
                }
                consolidated_schema["tables"].append(table_schema)

                # 4. Write each table schema to flat structure: schema/<table>.yaml
                tbl_name = table['name']
                os.makedirs(output_path, exist_ok=True)
                table_schema_path = os.path.join(output_path, f"{tbl_name}.yaml")
                with open(table_schema_path, 'w') as f:
                    yaml.dump(table_schema, f, sort_keys=False)
                print(f"  Schema written to: {table_schema_path}")

                # Record schema in per-table manifest
                table_manifest = self.manifest_store.get_table_manifest(tbl_name)
                table_manifest.record_schema(files_used, file_hashes, table_schema_path)

            except Exception as e:
                print(f"ERROR: Failed to infer schema for table {table['name']}: {e}")
                # Continue to next source?

        print(f"\nAll schemas written to: {output_path}")

        return consolidated_schema
