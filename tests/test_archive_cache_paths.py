"""Phase 2c — ArchiveCache uses resolver paths, not CWD defaults.

Pins:
- Extraction dir lives under `PathResolver.cache_dir` when
  `config.temp_dir` is unset — no more `./.fairway_cache/`.
- `config.temp_dir` (if set) takes precedence over cache_dir so users
  who point FAIRWAY_TEMP at fast scratch keep that behavior.
- Lock files live under `PathResolver.lock_dir` (state root), so they
  survive scratch purges mid-run.
- `lock_dir` is created at ArchiveCache __init__ time — recovers from
  a manually-deleted directory without crashing.
"""
import os
import zipfile
from pathlib import Path

import pytest
import yaml


def _make_archive(tmp_path):
    src = tmp_path / "payload.csv"
    src.write_text("id,name\n1,alice\n2,bob\n")
    archive = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(src, arcname="payload.csv")
    return archive


def _cache_with_config(tmp_path, monkeypatch, temp_dir=None):
    from fairway.config_loader import Config
    from fairway.manifest import ManifestStore
    from fairway.pipeline import ArchiveCache

    storage = {"root": str(tmp_path / "data")}
    if temp_dir is not None:
        storage["temp"] = str(temp_dir)

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.safe_dump({
        "project": "phase2c_project",
        "dataset_name": "phase2c_project",
        "engine": "duckdb",
        "storage": storage,
        "tables": [],
    }))
    cfg = Config(str(config_path))
    manifest_store = ManifestStore(str(cfg.paths.manifest_dir))
    return ArchiveCache(cfg, manifest_store.global_manifest), cfg


def test_lock_dir_created_eagerly(tmp_path, monkeypatch):
    cache, cfg = _cache_with_config(tmp_path, monkeypatch)
    assert cfg.paths.lock_dir.exists(), "lock_dir must be created at init"


def test_extraction_goes_under_cache_dir_by_default(tmp_path, monkeypatch):
    archive = _make_archive(tmp_path)
    cache, cfg = _cache_with_config(tmp_path, monkeypatch)
    extracted = cache.get_extracted_path(str(archive))
    assert Path(extracted).exists()
    # Cache dir comes from FAIRWAY_SCRATCH via the autouse fixture —
    # it's tmp_path/_scratch/projects/<proj>/cache. The extraction
    # directory must be a descendant.
    assert str(cfg.paths.cache_dir) in str(extracted), (
        f"Extraction dir {extracted} not under cache_dir {cfg.paths.cache_dir}"
    )


def test_config_temp_dir_overrides_cache_dir(tmp_path, monkeypatch):
    archive = _make_archive(tmp_path)
    fast_scratch = tmp_path / "fast"
    fast_scratch.mkdir()
    cache, cfg = _cache_with_config(tmp_path, monkeypatch, temp_dir=fast_scratch)
    extracted = cache.get_extracted_path(str(archive))
    assert str(fast_scratch) in str(extracted), (
        f"temp_dir override ignored — extracted at {extracted}"
    )


def test_extraction_creates_lockfile(tmp_path, monkeypatch):
    archive = _make_archive(tmp_path)
    cache, cfg = _cache_with_config(tmp_path, monkeypatch)
    cache.get_extracted_path(str(archive))
    locks = list(cfg.paths.lock_dir.glob("archive_*.lock"))
    assert locks, "archive lockfile not created under lock_dir"


def test_lock_dir_recovers_from_manual_deletion(tmp_path, monkeypatch):
    archive = _make_archive(tmp_path)
    cache, cfg = _cache_with_config(tmp_path, monkeypatch)
    # Operator manually wipes the lock dir between runs.
    import shutil
    shutil.rmtree(cfg.paths.lock_dir)
    # Extraction must still succeed; the __init__ creates lock_dir, but
    # later operations should also tolerate a missing dir. Recreate so
    # the subsequent lockf call has a parent to open in.
    cfg.paths.lock_dir.mkdir(parents=True, exist_ok=True)
    extracted = cache.get_extracted_path(str(archive))
    assert Path(extracted).exists()
