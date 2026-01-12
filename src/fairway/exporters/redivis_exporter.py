try:
    import redivis
except ImportError:
    redivis = None

import os
import json
from pathlib import Path

class RedivisExporter:
    def __init__(self, config):
        if redivis is None:
            raise ImportError("Redivis is not installed. Please install fairway[redivis] or fairway[all].")
        """
        Initialize the Redivis exporter.
        
        Args:
            config (dict): The redivis sub-section of the fairway config.
                          Should contain 'user', 'dataset', and optionally 'public_access_level'.
        """
        self.user_name = config.get('user')
        self.dataset_name = config.get('dataset')
        self.public_access_level = config.get('public_access_level', 'none')
        
        if not self.user_name or not self.dataset_name:
            raise ValueError("Redivis export requires 'user' and 'dataset' in config.")
        
        # Ensure API token is set
        if 'REDIVIS_API_TOKEN' not in os.environ:
            print("Warning: REDIVIS_API_TOKEN environment variable not set. Redivis export may fail.")

    def get_or_create_dataset(self):
        """Gets or creates the dataset on Redivis."""
        user = redivis.user(self.user_name)
        dataset = user.dataset(self.dataset_name)
        
        try:
            dataset.get()
            print(f"Dataset '{self.dataset_name}' found on Redivis.")
        except Exception:
            print(f"Dataset '{self.dataset_name}' not found. Creating...")
            dataset.create(public_access_level=self.public_access_level)
        
        return dataset

    def upload_table(self, table_name, file_path, metadata=None, schema=None):
        """
        Uploads a parquet file to a table in the dataset and applies metadata.
        
        Args:
            table_name (str): The name of the table to create/update.
            file_path (str): Path to the parquet file or directory of parquet files.
            metadata (dict): Rich metadata to include in the table description.
            schema (dict): Schema information including potential descriptions for variables.
        """
        dataset = self.get_or_create_dataset()
        table = dataset.table(table_name)
        
        try:
            table.get()
            print(f"Table '{table_name}' exists. Deleting existing table to replace...")
            table.delete()
        except Exception:
            pass

        # Construct a rich description from metadata
        description_parts = ["Ingested via fairway."]
        if metadata:
            description_parts.append("\n### Processing Metadata")
            for key, value in metadata.items():
                description_parts.append(f"- **{key}**: {value}")
        
        full_description = "\n".join(description_parts)

        print(f"Creating table '{table_name}'...")
        table.create(description=full_description)
        
        print(f"Uploading data from {file_path} to '{table_name}'...")
        upload = table.upload(table_name)
        upload.create(file_path, type="parquet")
        
        # Apply variable-level metadata if schema is provided
        if schema:
            print(f"Applying variable metadata for '{table_name}'...")
            try:
                # Refresh table to get variables after upload
                table.get()
                for var_name, var_info in schema.items():
                    # var_info can be a string (type) or a dict with 'type' and 'description'
                    description = None
                    label = None
                    
                    if isinstance(var_info, dict):
                        description = var_info.get('description')
                        label = var_info.get('label')
                    
                    if description or label:
                        try:
                            variable = table.variable(var_name)
                            variable.update(description=description, label=label)
                        except Exception as e:
                            print(f"  Warning: Could not update metadata for variable '{var_name}': {e}")
            except Exception as e:
                print(f"  Error accessing table variables: {e}")

        print(f"Successfully uploaded and annotated '{table_name}' on Redivis.")

    def update_dataset_metadata(self, description=None):
        """Updates dataset level metadata."""
        dataset = self.get_or_create_dataset()
        dataset.update(description=description)
        print(f"Updated dataset metadata for '{self.dataset_name}'.")
