import json
import hashlib
import os
import glob
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
