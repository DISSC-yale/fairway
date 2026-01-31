import json
import hashlib
import os
import glob
from datetime import datetime
from contextlib import contextmanager

MANIFEST_VERSION = "2.0"

class ManifestManager:
    def __init__(self, manifest_path='data/fmanifest.json'):
        self.manifest_path = manifest_path
        self.manifest = self._load_manifest()

    def _load_manifest(self):
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, 'r') as f:
                data = json.load(f)
            if "version" not in data:
                data = self._migrate_v1_to_v2(data)
            return data
        return {"version": MANIFEST_VERSION, "files": {}, "schemas": {}, "preprocessing": {}}

    def _migrate_v1_to_v2(self, old_data):
        """Migrate v1 manifest (no version field) to v2."""
        return {
            "version": MANIFEST_VERSION,
            "files": old_data.get("files", {}),
            "schemas": {},
            "preprocessing": {}
        }

    def _save_manifest(self):
        """Save manifest atomically. Deferred if in batch mode."""
        if getattr(self, '_batch_mode', False):
            return  # Deferred until batch completes

        temp_path = f"{self.manifest_path}.tmp"
        os.makedirs(os.path.dirname(self.manifest_path) or '.', exist_ok=True)
        with open(temp_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)
        os.replace(temp_path, self.manifest_path)  # Atomic on POSIX

    @contextmanager
    def batch(self):
        """Defer saves until batch completes."""
        self._batch_mode = True
        try:
            yield
        finally:
            self._batch_mode = False
            self._save_manifest()

    def get_file_key(self, file_path, source_name, source_root=None):
        """
        Generates a stable logical key for the file in the manifest.
        Format: {source_name}/{relative_path}
        """
        if source_root and file_path.startswith(source_root):
            rel_path = os.path.relpath(file_path, source_root)
        else:
            # Fallback for backward compatibility or missing root
            # Just use basename if no root (legacy behavior was basename-based)
            # OR use source_name + basename
            rel_path = os.path.basename(file_path)
            
        # Ensure forward slashes for consistency across OS
        rel_path = rel_path.replace(os.sep, '/')
        
        if source_name:
            return f"{source_name}/{rel_path}"
        return rel_path

    def get_file_hash(self, file_path, fast_check=True):
        """
        Calculates file fingerprint.
        If fast_check=True, returns 'mtime:size' string.
        If fast_check=False, returns SHA256 of content.
        """
        if not os.path.exists(file_path):
            return None

        if fast_check:
            stats = os.stat(file_path)
            return f"mtime:{stats.st_mtime}_size:{stats.st_size}"
        
        sha256_hash = hashlib.sha256()
        
        # Check if glob pattern (legacy support, though V2 favors explicit file lists)
        if '*' in file_path:
             files = sorted(glob.glob(file_path, recursive=True))
             if not files:
                 sha256_hash.update(b"empty_glob")
             
             for fpath in files:
                 if os.path.isfile(fpath):
                     relpath = os.path.relpath(fpath, os.path.dirname(file_path) or '.')
                     sha256_hash.update(relpath.encode('utf-8'))
                     try:
                         with open(fpath, "rb") as f:
                             for byte_block in iter(lambda: f.read(4096), b""):
                                 sha256_hash.update(byte_block)
                     except (IOError, OSError):
                         pass
        
        elif os.path.isdir(file_path):
            # For directories, hash all files recursively
            for root, dirs, files in sorted(os.walk(file_path)):
                for names in sorted(files):
                    filepath = os.path.join(root, names)
                    relpath = os.path.relpath(filepath, file_path)
                    sha256_hash.update(relpath.encode('utf-8'))
                    try:
                        with open(filepath, "rb") as f:
                            for byte_block in iter(lambda: f.read(4096), b""):
                                sha256_hash.update(byte_block)
                    except (IOError, OSError):
                        pass
        else:
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)
            else:
                 pass
                    
        return sha256_hash.hexdigest()

    def should_process(self, file_path, source_name=None, source_root=None, fast_check=True, computed_hash=None):
        """
        Determines if a file needs processing by comparing its current fingerprint 
        to the manifest.
        """
        key = self.get_file_key(file_path, source_name, source_root)
        
        # Check legacy keys (basename) for migration/interop
        if key not in self.manifest["files"]:
            # Try basename fallback if source_root wasn't used/available before
            basename = os.path.basename(file_path)
            if basename in self.manifest["files"]:
                # Migration opportunity? For now, just treat as 'not found under new key' implies process
                # Or we could map it. Let's start fresh for new keys to be safe.
                pass
            return True
        
        file_entry = self.manifest["files"][key]
        
        # Check source name (implicit in key now, but keeping check)
        entry_source = file_entry.get("source_name")
        if source_name and entry_source and entry_source != source_name:
             # Key collision?
             return True
        
        current_hash = computed_hash if computed_hash else self.get_file_hash(file_path, fast_check=fast_check)
        
        # If stored hash is sha256 and we do fast_check, we mismatch.
        # Strategy: Validations should be consistent. 
        # If manifest has "mtime:...", compare. 
        # If manifest has "sha256...", and we requested fast_check, we can't be sure unless we upgrade manifest.
        # But for V2, let's assume we stick to the configured mode.
        
        return file_entry["hash"] != current_hash

    def update_manifest(self, file_path, status="success", metadata=None, source_name=None, source_root=None, computed_hash=None, fast_check=True):
        """
        Updates the manifest with the processing result.
        """
        key = self.get_file_key(file_path, source_name, source_root)
        
        if computed_hash:
            current_hash = computed_hash
        else:
            current_hash = self.get_file_hash(file_path, fast_check=fast_check)
        
        entry = {
            "hash": current_hash,
            "last_processed": datetime.now().isoformat(),
            "status": status,
            "metadata": metadata or {}
        }
        
        if source_name:
            entry["source_name"] = source_name
            
        self.manifest["files"][key] = entry
        self._save_manifest()
        
    def check_files_bulk(self, file_status_list):
        """
        Updates manifest based on a list of (key, hash, status) tuples from distributed workers.
        Useful for batch updates without re-saving every time.
        """
        changed = False
        for item in file_status_list:
            # item structure depends on worker return.
            # flexible: dict or object
            key = item.get('key')
            entry = item.get('entry')
            if key and entry:
                self.manifest["files"][key] = entry
                changed = True

        if changed:
            self._save_manifest()

    # --- Schema Tracking Methods ---

    def _compute_sources_hash(self, sources_info):
        """Hash all source file hashes together."""
        combined = hashlib.sha256()
        for src in sorted(sources_info, key=lambda x: x['name']):
            for h in sorted(src.get('file_hashes', [])):
                if h:  # Skip None hashes
                    combined.update(h.encode())
        return combined.hexdigest()[:16]

    def record_schema_run(self, dataset_name, sources_info, output_path):
        """Record a schema inference run."""
        if "schemas" not in self.manifest:
            self.manifest["schemas"] = {}

        # Compute combined hash of all source files
        sources_hash = self._compute_sources_hash(sources_info)

        self.manifest["schemas"][dataset_name] = {
            "generated_at": datetime.now().isoformat(),
            "output_path": output_path,
            "sources_hash": sources_hash,
            "sources": sources_info  # List of {name, files_used, file_hashes}
        }
        self._save_manifest()

    def is_schema_stale(self, dataset_name, current_sources_info):
        """Check if schema needs regeneration."""
        schema_entry = self.manifest.get("schemas", {}).get(dataset_name)
        if not schema_entry:
            return True

        current_hash = self._compute_sources_hash(current_sources_info)
        return schema_entry.get("sources_hash") != current_hash

    def get_latest_schema_run(self, dataset_name):
        """Get info about most recent schema run."""
        return self.manifest.get("schemas", {}).get(dataset_name)

    # --- Preprocessing Cache Methods ---

    def record_preprocessing(self, original_path, preprocessed_path, action, source_name, source_root=None):
        """Record preprocessing result for cache sharing."""
        if "preprocessing" not in self.manifest:
            self.manifest["preprocessing"] = {}

        key = self.get_file_key(original_path, source_name, source_root)
        source_hash = self.get_file_hash(original_path, fast_check=True)

        self.manifest["preprocessing"][key] = {
            "source_hash": source_hash,
            "preprocessed_path": preprocessed_path,
            "action": action,
            "processed_at": datetime.now().isoformat()
        }
        self._save_manifest()

    def get_preprocessed_path(self, original_path, source_name, source_root=None):
        """Get cached preprocessed path if still valid."""
        key = self.get_file_key(original_path, source_name, source_root)
        entry = self.manifest.get("preprocessing", {}).get(key)

        if not entry:
            return None

        # Check source hasn't changed
        current_hash = self.get_file_hash(original_path, fast_check=True)
        if entry.get("source_hash") != current_hash:
            return None

        # Check preprocessed files still exist
        preprocessed = entry.get("preprocessed_path")
        if not preprocessed or not os.path.exists(preprocessed):
            return None

        return preprocessed
