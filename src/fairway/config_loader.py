import yaml
import os
import glob
import re

class Config:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.data = yaml.safe_load(f)
        
        self.dataset_name = self.data.get('dataset_name')
        self.engine = self.data.get('engine', 'duckdb')
        self.storage = self.data.get('storage', {})
        self.sources = self._expand_sources(self.data.get('sources', []))
        self.validations = self.data.get('validations', {})
        self.enrichment = self.data.get('enrichment', {})
        self.partition_by = self.data.get('partition_by', [])
        self.redivis = self.data.get('redivis', {})
        self.output_format = self.storage.get('format', 'parquet').lower()
        
        # Performance/Optimizations
        performance = self.data.get('performance', {})
        self.target_rows = performance.get('target_rows') or self.data.get('target_rows', 500000)



    def _load_schema(self, schema_ref):
        """Loads schema from a file if schema_ref is a path string, otherwise returns it as-is."""
        if isinstance(schema_ref, str):
            # Assume it's a file path
            if not os.path.exists(schema_ref):
                # Try relative to config file if not absolute
                # Note: This is simplified, might need config dir context
                raise FileNotFoundError(f"Schema file not found: {schema_ref}")
            
            with open(schema_ref, 'r') as f:
                data = None
                if schema_ref.endswith('.yaml') or schema_ref.endswith('.yml'):
                    data = yaml.safe_load(f)
                elif schema_ref.endswith('.json'):
                    import json
                    data = json.load(f)
                else:
                    raise ValueError(f"Unsupported schema file format: {schema_ref}")
                
                # Handle generate-schema output format
                if isinstance(data, dict) and 'columns' in data:
                    return data['columns']
                return data
        return schema_ref or {}

    def _expand_sources(self, raw_sources):
        """Discovers files and extracts metadata via regex if patterns are provided."""
        expanded = []
        for src in raw_sources:
            path_pattern = src.get('path_pattern') or src.get('path')
            naming_pattern = src.get('naming_pattern')
            
            if not path_pattern:
                continue

            # Glob discovery
            files = glob.glob(path_pattern, recursive=True)
            
            # Resolve schema once if possible, or for each if dynamic?
            # For now, simplistic: resolve usage of schema field
            raw_schema = src.get('schema')
            resolved_schema = self._load_schema(raw_schema)

            for f in files:
                metadata = {}
                if naming_pattern:
                    match = re.search(naming_pattern, os.path.basename(f))
                    if match:
                        metadata = match.groupdict()
                
                # Create a specific source entry for each file
                expanded.append({
                    'name': src.get('name', os.path.basename(f)),
                    'path': f,
                    'format': src.get('format', 'csv'),
                    'metadata': metadata,
                    'schema': resolved_schema
                })
        return expanded

    def get_source_by_name(self, name):
        for source in self.sources:
            if source['name'] == name:
                return source
        return None
