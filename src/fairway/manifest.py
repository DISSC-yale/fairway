import json
import hashlib
import os
import glob
from datetime import datetime
from contextlib import contextmanager

MANIFEST_VERSION = "2.0"
TABLE_MANIFEST_VERSION = "3.0"
GLOBAL_MANIFEST_VERSION = "3.0"

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

    def get_file_key(self, file_path, table_name, table_root=None):
        """
        Generates a stable logical key for the file in the manifest.
        Format: {table_name}/{relative_path}
        """
        if table_root and file_path.startswith(table_root):
            rel_path = os.path.relpath(file_path, table_root)
        else:
            # Fallback for backward compatibility or missing root
            # Just use basename if no root (legacy behavior was basename-based)
            # OR use table_name + basename
            rel_path = os.path.basename(file_path)

        # Ensure forward slashes for consistency across OS
        rel_path = rel_path.replace(os.sep, '/')

        if table_name:
            return f"{table_name}/{rel_path}"
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

    def should_process(self, file_path, table_name=None, table_root=None, fast_check=True, computed_hash=None):
        """
        Determines if a file needs processing by comparing its current fingerprint
        to the manifest.
        """
        key = self.get_file_key(file_path, table_name, table_root)

        # Check legacy keys (basename) for migration/interop
        if key not in self.manifest["files"]:
            # Try basename fallback if table_root wasn't used/available before
            basename = os.path.basename(file_path)
            if basename in self.manifest["files"]:
                # Migration opportunity? For now, just treat as 'not found under new key' implies process
                # Or we could map it. Let's start fresh for new keys to be safe.
                pass
            return True

        file_entry = self.manifest["files"][key]

        # Check table name (implicit in key now, but keeping check)
        entry_table = file_entry.get("table_name")
        if table_name and entry_table and entry_table != table_name:
             # Key collision?
             return True
        
        current_hash = computed_hash if computed_hash else self.get_file_hash(file_path, fast_check=fast_check)
        
        # If stored hash is sha256 and we do fast_check, we mismatch.
        # Strategy: Validations should be consistent. 
        # If manifest has "mtime:...", compare. 
        # If manifest has "sha256...", and we requested fast_check, we can't be sure unless we upgrade manifest.
        # But for V2, let's assume we stick to the configured mode.
        
        return file_entry["hash"] != current_hash

    def update_manifest(self, file_path, status="success", metadata=None, table_name=None, table_root=None, computed_hash=None, fast_check=True):
        """
        Updates the manifest with the processing result.
        """
        key = self.get_file_key(file_path, table_name, table_root)

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

        if table_name:
            entry["table_name"] = table_name

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

    def _compute_tables_hash(self, tables_info):
        """Hash all table file hashes together."""
        combined = hashlib.sha256()
        for tbl in sorted(tables_info, key=lambda x: x['name']):
            for h in sorted(tbl.get('file_hashes', [])):
                if h:  # Skip None hashes
                    combined.update(h.encode())
        return combined.hexdigest()[:16]

    def record_schema_run(self, dataset_name, tables_info, output_path):
        """Record a schema inference run."""
        if "schemas" not in self.manifest:
            self.manifest["schemas"] = {}

        # Compute combined hash of all table files
        tables_hash = self._compute_tables_hash(tables_info)

        self.manifest["schemas"][dataset_name] = {
            "generated_at": datetime.now().isoformat(),
            "output_path": output_path,
            "tables_hash": tables_hash,
            "tables": tables_info  # List of {name, files_used, file_hashes}
        }
        self._save_manifest()

    def is_schema_stale(self, dataset_name, current_tables_info):
        """Check if schema needs regeneration."""
        schema_entry = self.manifest.get("schemas", {}).get(dataset_name)
        if not schema_entry:
            return True

        current_hash = self._compute_tables_hash(current_tables_info)
        return schema_entry.get("tables_hash") != current_hash

    def get_latest_schema_run(self, dataset_name):
        """Get info about most recent schema run."""
        return self.manifest.get("schemas", {}).get(dataset_name)

    # --- Preprocessing Cache Methods ---

    def record_preprocessing(self, original_path, preprocessed_path, action, table_name, table_root=None):
        """Record preprocessing result for cache sharing."""
        if "preprocessing" not in self.manifest:
            self.manifest["preprocessing"] = {}

        key = self.get_file_key(original_path, table_name, table_root)
        file_hash = self.get_file_hash(original_path, fast_check=True)

        self.manifest["preprocessing"][key] = {
            "file_hash": file_hash,
            "preprocessed_path": preprocessed_path,
            "action": action,
            "processed_at": datetime.now().isoformat()
        }
        self._save_manifest()

    def get_preprocessed_path(self, original_path, table_name, table_root=None):
        """Get cached preprocessed path if still valid."""
        key = self.get_file_key(original_path, table_name, table_root)
        entry = self.manifest.get("preprocessing", {}).get(key)

        if not entry:
            return None

        # Check file hasn't changed
        current_hash = self.get_file_hash(original_path, fast_check=True)
        if entry.get("file_hash") != current_hash:
            return None

        # Check preprocessed files still exist
        preprocessed = entry.get("preprocessed_path")
        if not preprocessed or not os.path.exists(preprocessed):
            return None

        return preprocessed

    # --- Archive Extraction Cache Methods ---

    def record_extraction(self, archive_path, extracted_dir, archive_hash):
        """Record that an archive was extracted."""
        if "extractions" not in self.manifest:
            self.manifest["extractions"] = {}

        key = f"archive:{os.path.abspath(archive_path)}"
        self.manifest["extractions"][key] = {
            "extracted_dir": extracted_dir,
            "archive_hash": archive_hash,
            "extracted_at": datetime.now().isoformat()
        }
        self._save_manifest()

    def get_extraction(self, archive_path):
        """Get existing extraction info if available."""
        key = f"archive:{os.path.abspath(archive_path)}"
        return self.manifest.get("extractions", {}).get(key)

    def is_extraction_valid(self, archive_path):
        """Check if existing extraction is still valid (archive unchanged, dir exists)."""
        entry = self.get_extraction(archive_path)
        if not entry:
            return False

        # Check archive hasn't changed (using mtime+size for speed)
        current_hash = self.get_file_hash(archive_path, fast_check=True)
        if entry.get("archive_hash") != current_hash:
            return False

        # Check extracted dir still exists
        extracted_dir = entry.get("extracted_dir")
        if not extracted_dir or not os.path.isdir(extracted_dir):
            return False

        return True


# --- New Per-Table Manifest Classes (v3) ---

def _get_file_hash_static(file_path, fast_check=True):
    """Static version of file hash calculation."""
    if not os.path.exists(file_path):
        return None
    if fast_check:
        stats = os.stat(file_path)
        return f"mtime:{stats.st_mtime}_size:{stats.st_size}"
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class TableManifest:
    """Per-table manifest for tracking files, preprocessing, and schema."""

    def __init__(self, manifest_dir: str, table_name: str):
        self.manifest_dir = manifest_dir
        self.table_name = table_name
        self.manifest_path = os.path.join(manifest_dir, f"{table_name}.json")
        self._batch_mode = False
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        return {
            "version": TABLE_MANIFEST_VERSION,
            "table_name": self.table_name,
            "files": {},
            "preprocessing": {},
            "schema": None
        }

    def _save(self):
        if self._batch_mode:
            return
        os.makedirs(self.manifest_dir, exist_ok=True)
        temp_path = f"{self.manifest_path}.tmp"
        with open(temp_path, 'w') as f:
            json.dump(self.data, f, indent=2)
        os.replace(temp_path, self.manifest_path)

    @contextmanager
    def batch(self):
        self._batch_mode = True
        try:
            yield
        finally:
            self._batch_mode = False
            self._save()

    def get_file_key(self, file_path, table_root=None):
        """Generate relative path key for file."""
        if table_root and file_path.startswith(table_root):
            rel_path = os.path.relpath(file_path, table_root)
        else:
            rel_path = os.path.basename(file_path)
        return rel_path.replace(os.sep, '/')

    def should_process(self, file_path, table_root=None, fast_check=True, computed_hash=None):
        """Check if file needs processing."""
        key = self.get_file_key(file_path, table_root)
        if key not in self.data["files"]:
            return True
        entry = self.data["files"][key]
        current_hash = computed_hash or _get_file_hash_static(file_path, fast_check)
        return entry.get("hash") != current_hash

    def update_file(self, file_path, status="success", metadata=None,
                    table_root=None, computed_hash=None, fast_check=True):
        """Update file entry after processing."""
        key = self.get_file_key(file_path, table_root)
        current_hash = computed_hash or _get_file_hash_static(file_path, fast_check)
        self.data["files"][key] = {
            "hash": current_hash,
            "last_processed": datetime.now().isoformat(),
            "status": status,
            "metadata": metadata or {}
        }
        self._save()

    def record_preprocessing(self, original_path, preprocessed_path, action, table_root=None):
        """Record preprocessing result."""
        key = self.get_file_key(original_path, table_root)
        self.data["preprocessing"][key] = {
            "file_hash": _get_file_hash_static(original_path, fast_check=True),
            "preprocessed_path": preprocessed_path,
            "action": action,
            "processed_at": datetime.now().isoformat()
        }
        self._save()

    def get_preprocessed_path(self, original_path, table_root=None):
        """Get cached preprocessed path if valid."""
        key = self.get_file_key(original_path, table_root)
        entry = self.data.get("preprocessing", {}).get(key)
        if not entry:
            return None
        current_hash = _get_file_hash_static(original_path, fast_check=True)
        if entry.get("file_hash") != current_hash:
            return None
        preprocessed = entry.get("preprocessed_path")
        if not preprocessed or not os.path.exists(preprocessed):
            return None
        return preprocessed

    def record_schema(self, files_used, file_hashes, output_path):
        """Record schema generation."""
        self.data["schema"] = {
            "generated_at": datetime.now().isoformat(),
            "output_path": output_path,
            "files_used": files_used,
            "file_hashes": file_hashes,
            "combined_hash": self._compute_hash(file_hashes)
        }
        self._save()

    def is_schema_stale(self, current_file_hashes):
        """Check if schema needs regeneration."""
        schema = self.data.get("schema")
        if not schema:
            return True
        return schema.get("combined_hash") != self._compute_hash(current_file_hashes)

    def _compute_hash(self, file_hashes):
        combined = hashlib.sha256()
        for h in sorted(h for h in file_hashes if h):
            combined.update(h.encode())
        return combined.hexdigest()[:16]


class GlobalManifest:
    """Global manifest for cross-table state (extractions)."""

    def __init__(self, manifest_dir: str):
        self.manifest_dir = manifest_dir
        self.manifest_path = os.path.join(manifest_dir, "_global.json")
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.manifest_path):
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        return {
            "version": GLOBAL_MANIFEST_VERSION,
            "extractions": {}
        }

    def _save(self):
        os.makedirs(self.manifest_dir, exist_ok=True)
        temp_path = f"{self.manifest_path}.tmp"
        with open(temp_path, 'w') as f:
            json.dump(self.data, f, indent=2)
        os.replace(temp_path, self.manifest_path)

    def record_extraction(self, archive_path, extracted_dir, archive_hash, table_name=None):
        """Record archive extraction."""
        key = f"archive:{os.path.abspath(archive_path)}"
        entry = self.data["extractions"].get(key, {})
        used_by = set(entry.get("used_by_tables", []))
        if table_name:
            used_by.add(table_name)
        self.data["extractions"][key] = {
            "extracted_dir": extracted_dir,
            "archive_hash": archive_hash,
            "extracted_at": datetime.now().isoformat(),
            "used_by_tables": list(used_by)
        }
        self._save()

    def get_extraction(self, archive_path):
        """Get extraction info if available."""
        key = f"archive:{os.path.abspath(archive_path)}"
        return self.data.get("extractions", {}).get(key)

    def is_extraction_valid(self, archive_path):
        """Check if extraction is still valid."""
        entry = self.get_extraction(archive_path)
        if not entry:
            return False
        current_hash = _get_file_hash_static(archive_path, fast_check=True)
        if entry.get("archive_hash") != current_hash:
            return False
        extracted_dir = entry.get("extracted_dir")
        if not extracted_dir or not os.path.isdir(extracted_dir):
            return False
        return True


class ManifestStore:
    """Orchestrates per-table and global manifests."""

    def __init__(self, manifest_dir: str = "manifest"):
        self.manifest_dir = manifest_dir
        self._table_manifests = {}
        self._global_manifest = None

    def get_table_manifest(self, table_name: str) -> TableManifest:
        """Get or create a per-table manifest."""
        if table_name not in self._table_manifests:
            self._table_manifests[table_name] = TableManifest(self.manifest_dir, table_name)
        return self._table_manifests[table_name]

    @property
    def global_manifest(self) -> GlobalManifest:
        """Get the global manifest (lazy init)."""
        if self._global_manifest is None:
            self._global_manifest = GlobalManifest(self.manifest_dir)
        return self._global_manifest

    def list_tables(self):
        """List all tables with manifests."""
        if not os.path.exists(self.manifest_dir):
            return []
        tables = []
        for f in os.listdir(self.manifest_dir):
            if f.endswith('.json') and not f.startswith('_'):
                tables.append(f[:-5])  # Remove .json
        return sorted(tables)