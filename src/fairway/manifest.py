import json
import hashlib
import os
from datetime import datetime

class ManifestManager:
    def __init__(self, manifest_path='data/fmanifest.json'):
        self.manifest_path = manifest_path
        self.manifest = self._load_manifest()

    def _load_manifest(self):
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        return {"files": {}}

    def _save_manifest(self):
        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)

    def get_file_hash(self, path):
        sha256_hash = hashlib.sha256()
        
        if os.path.isdir(file_path):
            # For directories, hash all files recursively, sorted by path
            for root, dirs, files in sorted(os.walk(file_path)):
                for names in sorted(files):
                    filepath = os.path.join(root, names)
                    # Update hash with filename to detect rename/move
                    relpath = os.path.relpath(filepath, file_path)
                    sha256_hash.update(relpath.encode('utf-8'))
                    try:
                        with open(filepath, "rb") as f:
                            for byte_block in iter(lambda: f.read(4096), b""):
                                sha256_hash.update(byte_block)
                    except (IOError, OSError):
                        # Skip files we can't read
                        pass
        else:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
                    
        return sha256_hash.hexdigest()

    def should_process(self, file_path, source_name=None):
        file_name = os.path.basename(file_path)
        if file_name not in self.manifest["files"]:
            return True
        
        file_entry = self.manifest["files"][file_name]
        
        # Check source name change
        if source_name:
            stored_source_name = file_entry.get("source_name")
            
            if stored_source_name != source_name:
                print(f"WARNING: Duplicate file content detected under new source name '{source_name}'. Previous source name: '{stored_source_name}'.")
                return True
        
        current_hash = self.get_file_hash(file_path)
        return file_entry["hash"] != current_hash

    def update_manifest(self, file_path, status="success", metadata=None, source_name=None):
        file_name = os.path.basename(file_path)
        current_hash = self.get_file_hash(file_path)
        
        entry = {
            "hash": current_hash,
            "last_processed": datetime.now().isoformat(),
            "status": status,
            "metadata": metadata or {}
        }
        
        if source_name:
            entry["source_name"] = source_name
            
        self.manifest["files"][file_name] = entry
        self._save_manifest()
