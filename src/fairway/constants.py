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

# Engine names
VALID_ENGINES = frozenset({"duckdb", "pyspark"})

# Slurm defaults (no personal account — must be set explicitly)
DEFAULT_SLURM_PARTITION = "day"
DEFAULT_SLURM_TIME = "24:00:00"
DEFAULT_SLURM_MEM = "16G"
DEFAULT_SLURM_CPUS = 4

# Spark module for bare-metal HPC
HPC_SPARK_MODULE = "Spark/3.5.1-foss-2022b-Scala-2.13"
