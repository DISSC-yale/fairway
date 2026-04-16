"""Tests for the constants module."""
import re
from pathlib import Path


def test_constants_module_has_required_keys():
    from fairway import constants
    assert hasattr(constants, 'DEFAULT_CONFIG_PATH')
    assert hasattr(constants, 'DEFAULT_SPARK_CONFIG_PATH')
    assert hasattr(constants, 'DEFAULT_MANIFEST_DIR')
    assert hasattr(constants, 'DEFAULT_CACHE_DIR')
    assert hasattr(constants, 'DEFAULT_LOG_DIR')
    assert hasattr(constants, 'FAIRWAY_SIF_ENV_VAR')
    assert hasattr(constants, 'VALID_ENGINES')


def test_valid_engines_is_frozen():
    from fairway.constants import VALID_ENGINES
    assert isinstance(VALID_ENGINES, frozenset)
    assert "duckdb" in VALID_ENGINES
    assert "pyspark" in VALID_ENGINES


def test_normalize_engine_name_handles_aliases():
    from fairway.constants import normalize_engine_name
    # 'spark' is a user-friendly alias for 'pyspark'
    assert normalize_engine_name("spark") == "pyspark"
    assert normalize_engine_name("SPARK") == "pyspark"
    # Canonical names pass through
    assert normalize_engine_name("pyspark") == "pyspark"
    assert normalize_engine_name("duckdb") == "duckdb"
    # Empty/None pass through unchanged
    assert normalize_engine_name(None) is None
    assert normalize_engine_name("") == ""
    # Unknown strings pass through so caller can validate with VALID_ENGINES
    assert normalize_engine_name("unknown_engine") == "unknown_engine"


def test_normalize_engine_name_handles_whitespace():
    from fairway.constants import normalize_engine_name
    assert normalize_engine_name("  spark  ") == "pyspark"
    assert normalize_engine_name("\tduckdb\n") == "duckdb"
    # All-whitespace collapses to empty (caller will reject)
    assert normalize_engine_name("   ") == ""


def test_normalize_engine_name_rejects_non_string():
    """Non-string input must not crash — must pass through so caller can validate."""
    from fairway.constants import normalize_engine_name
    assert normalize_engine_name(123) == 123
    assert normalize_engine_name(["spark"]) == ["spark"]


def test_normalize_engine_name_is_idempotent():
    """Calling normalize on an already-normalized name must be a no-op."""
    from fairway.constants import normalize_engine_name
    once = normalize_engine_name("spark")
    twice = normalize_engine_name(once)
    assert once == twice == "pyspark"


def test_container_python_version_is_defined():
    """Dev-mode bind path depends on this constant matching the container base image."""
    from fairway import constants
    assert constants.CONTAINER_PYTHON_VERSION.startswith("python3.")


def test_container_python_version_matches_dockerfile():
    """Dockerfile FROM line must match constants.CONTAINER_PYTHON_VERSION.

    If this fails, bumping the container base image broke the dev-mode bind
    path. Update constants.CONTAINER_PYTHON_VERSION in lockstep.
    """
    from fairway import constants
    dockerfile = Path(__file__).resolve().parents[2] / "src" / "fairway" / "data" / "Dockerfile"
    content = dockerfile.read_text()
    match = re.search(r"FROM python:(\d+\.\d+)", content)
    assert match, "Expected 'FROM python:X.Y' line in Dockerfile"
    dockerfile_version = f"python{match.group(1)}"
    assert dockerfile_version == constants.CONTAINER_PYTHON_VERSION, (
        f"Dockerfile uses {dockerfile_version} but "
        f"constants.CONTAINER_PYTHON_VERSION={constants.CONTAINER_PYTHON_VERSION}"
    )


def test_container_python_version_matches_apptainer_def():
    """Apptainer.def base image must match constants.CONTAINER_PYTHON_VERSION."""
    from fairway import constants
    apptainer = Path(__file__).resolve().parents[2] / "src" / "fairway" / "data" / "Apptainer.def"
    content = apptainer.read_text()
    match = re.search(r"From:\s*python:(\d+\.\d+)", content)
    assert match, "Expected 'From: python:X.Y' line in Apptainer.def"
    apptainer_version = f"python{match.group(1)}"
    assert apptainer_version == constants.CONTAINER_PYTHON_VERSION, (
        f"Apptainer.def uses {apptainer_version} but "
        f"constants.CONTAINER_PYTHON_VERSION={constants.CONTAINER_PYTHON_VERSION}"
    )
