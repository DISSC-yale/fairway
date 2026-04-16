import copy
import json
import hashlib
import logging
import os
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

TABLE_MANIFEST_VERSION = "3.0"
GLOBAL_MANIFEST_VERSION = "3.0"


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
            try:
                with open(self.manifest_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(
                    "Manifest file corrupted, starting fresh: %s",
                    self.manifest_path,
                )
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
        snapshot = copy.deepcopy(self.data)
        try:
            yield
            self._batch_mode = False
            self._save()
        except Exception:
            self.data = snapshot
            self._batch_mode = False
            raise

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
        # Retry files that previously failed
        if entry.get("status") == "failed":
            return True
        current_hash = computed_hash or _get_file_hash_static(file_path, fast_check)
        return entry.get("hash") != current_hash

    def update_file(self, file_path, status="success", metadata=None,
                    table_root=None, computed_hash=None, fast_check=True, batch_id=None):
        """Update file entry after processing.

        Args:
            file_path: Path to the file
            status: Processing status ('success' or 'failed')
            metadata: Additional metadata dict
            table_root: Root directory for relative path calculation
            computed_hash: Pre-computed file hash (optional)
            fast_check: Use fast mtime+size hash (default True)
            batch_id: Optional batch ID to store in metadata
        """
        key = self.get_file_key(file_path, table_root)
        current_hash = computed_hash or _get_file_hash_static(file_path, fast_check)

        # Merge batch_id into metadata if provided
        final_metadata = metadata.copy() if metadata else {}
        if batch_id:
            final_metadata["batch_id"] = batch_id

        self.data["files"][key] = {
            "hash": current_hash,
            "last_processed": datetime.now().isoformat(),
            "status": status,
            "metadata": final_metadata
        }
        self._save()

    def query_file(self, file_key):
        """Query a single file entry by its key.

        Args:
            file_key: The file key (relative path or basename)

        Returns:
            File entry dict or None if not found
        """
        return self.data["files"].get(file_key)

    def query_files(self, status=None, batch_id=None):
        """Query files with optional filters.

        Args:
            status: Filter by status ('success', 'failed')
            batch_id: Filter by batch_id in metadata

        Returns:
            List of file entries matching filters, each with 'file_key' added
        """
        results = []
        for key, entry in self.data["files"].items():
            # Apply status filter
            if status is not None and entry.get("status") != status:
                continue

            # Apply batch_id filter
            if batch_id is not None:
                entry_batch_id = entry.get("metadata", {}).get("batch_id")
                if entry_batch_id != batch_id:
                    continue

            # Include file_key in result for identification
            result = entry.copy()
            result["file_key"] = key
            results.append(result)

        return results

    def get_pending_files(self, file_paths, table_root=None, fast_check=True):
        """Filter file_paths to only those needing processing.

        Convenience wrapper around should_process() for batch use.
        Returns subset of file_paths where should_process() is True.
        """
        return [
            f for f in file_paths
            if self.should_process(f, table_root=table_root, fast_check=fast_check)
        ]

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

    def get_preprocessed_path(self, original_path, table_root=None, action=None):
        """Get cached preprocessed path if valid.

        Args:
            original_path: Original file path to look up.
            table_root: Root directory for relative path calculation.
            action: Preprocessing action that must match the cached entry.
                    If provided, a cache entry with a different action is stale.
        """
        key = self.get_file_key(original_path, table_root)
        entry = self.data.get("preprocessing", {}).get(key)
        if not entry:
            return None
        if action is not None and entry.get("action", "") != action:
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
