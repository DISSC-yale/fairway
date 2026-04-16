"""Single source of truth for all path defaults and magic strings in fairway."""

# Config paths
DEFAULT_CONFIG_PATH = "config/fairway.yaml"
DEFAULT_SPARK_CONFIG_PATH = "config/spark.yaml"

# Storage defaults
DEFAULT_MANIFEST_DIR = "manifest"
DEFAULT_CACHE_DIR = ".fairway_cache"
DEFAULT_LOG_DIR = "logs/slurm"
DEFAULT_LOG_FILE = "logs/fairway.jsonl"
DEFAULT_PROCESSED_DIR = "processed"
DEFAULT_CURATED_DIR = "curated"

# Container
FAIRWAY_SIF_ENV_VAR = "FAIRWAY_SIF"
DEFAULT_SIF_NAME = "fairway.sif"
DEFAULT_CONTAINER_IMAGE = "docker://ghcr.io/dissc-yale/fairway:latest"
# Python version shipped in the container. Used to construct the
# site-packages bind path in dev mode. Must match FROM python:X.Y in
# src/fairway/data/Dockerfile and the venv in src/fairway/data/Apptainer.def.
CONTAINER_PYTHON_VERSION = "python3.10"

# Engine names. Canonical set; `ENGINE_ALIASES` maps user-facing aliases into
# this set so the rest of the codebase sees a single name per engine.
VALID_ENGINES = frozenset({"duckdb", "pyspark"})
ENGINE_ALIASES = {"spark": "pyspark"}


def normalize_engine_name(engine):
    """Return the canonical engine name for an input string.

    Accepts aliases (e.g. 'spark' -> 'pyspark'). Returns the input
    unchanged if already canonical. Does NOT validate; callers are
    expected to check membership in VALID_ENGINES afterwards.
    """
    if not engine:
        return engine
    key = engine.lower()
    return ENGINE_ALIASES.get(key, key)

# Slurm defaults (no personal account — must be set explicitly)
DEFAULT_SLURM_PARTITION = "day"
DEFAULT_SLURM_TIME = "24:00:00"
DEFAULT_SLURM_MEM = "16G"
DEFAULT_SLURM_CPUS = 4

# Spark module for bare-metal HPC
HPC_SPARK_MODULE = "Spark/3.5.1-foss-2022b-Scala-2.13"
