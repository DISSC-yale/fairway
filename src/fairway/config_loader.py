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
                    'schema': src.get('schema', {})
                })
        return expanded

    def get_source_by_name(self, name):
        for source in self.sources:
            if source['name'] == name:
                return source
        return None
