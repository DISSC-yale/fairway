#!/bin/bash
#SBATCH --job-name=fairway_driver
#SBATCH --output=logs/slurm/driver_%j.log
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

# =============================================================================
# Fairway Driver Job
# =============================================================================
# This script submits the fairway pipeline as a Slurm job with Spark cluster.
# It handles Spark cluster lifecycle (start/stop) and runs the pipeline.
#
# Supports two modes:
#   1. Apptainer mode: Spark and pipeline run inside containers
#   2. Bare-metal mode: Spark and pipeline run via host modules
#
# Spark cluster configuration is read from config/spark.yaml by `fairway spark start`.
# This includes: nodes, cpus_per_node, mem_per_node, partition, account, time.
#
# Usage:
#   sbatch scripts/driver.sh [config_path]
#
# Environment Variables:
#   USE_APPTAINER - Set to "yes" to run inside Apptainer container (auto-detected if fairway.sif exists)
#   FAIRWAY_SIF   - Path to Apptainer image (default: ./fairway.sif)
#   FAIRWAY_BINDS - Additional bind paths for Apptainer (default: /vast)
#   FAIRWAY_VENV  - Path to Python virtual environment (bare-metal mode only)
#
# Prefer using `fairway submit --with-spark` instead of this script directly.

set -e

CONFIG_PATH=${1:-config/fairway.yaml}

# =============================================================================
# Mode Detection
# =============================================================================

# Detect Apptainer: explicit env var or file presence
USE_APPTAINER=${USE_APPTAINER:-}
FAIRWAY_SIF=${FAIRWAY_SIF:-fairway.sif}

if [ -z "$USE_APPTAINER" ]; then
    if [ -f "$FAIRWAY_SIF" ]; then
        USE_APPTAINER="yes"
        echo "Auto-detected Apptainer mode (found $FAIRWAY_SIF)"
    else
        USE_APPTAINER="no"
        echo "Bare-metal mode (no container found)"
    fi
fi

# Default bind paths - adjust for your HPC environment
FAIRWAY_BINDS=${FAIRWAY_BINDS:-/vast}

# Export for SlurmSparkManager to detect
export FAIRWAY_SIF
export FAIRWAY_BINDS

# Ensure log directory exists
mkdir -p logs/slurm

# =============================================================================
# Environment Setup
# =============================================================================

if [ "$USE_APPTAINER" = "yes" ]; then
    echo "============================================================"
    echo "Apptainer Mode"
    echo "  Container: $FAIRWAY_SIF"
    echo "  Bind paths: $FAIRWAY_BINDS"
    echo "============================================================"

    # Verify container exists
    if [ ! -f "$FAIRWAY_SIF" ]; then
        echo "ERROR: Container not found: $FAIRWAY_SIF"
        echo "Build it with: fairway build"
        exit 1
    fi
else
    echo "============================================================"
    echo "Bare-Metal Mode"
    echo "============================================================"

    # Load required modules (Spark includes Java)
    echo "Loading Spark module..."
    module load Spark/3.5.1-foss-2022b-Scala-2.13 2>/dev/null || {
        echo "WARNING: Could not load Spark module. Ensure JAVA_HOME is set."
    }

    # Load virtual environment if specified
    if [ -n "$FAIRWAY_VENV" ]; then
        if [ -f "$FAIRWAY_VENV/bin/activate" ]; then
            echo "Activating virtual environment: $FAIRWAY_VENV"
            source "$FAIRWAY_VENV/bin/activate"
        fi
    fi
fi

# =============================================================================
# 1. Start Spark Cluster
# =============================================================================
# The 'fairway spark start' command reads config/spark.yaml for cluster sizing
# and submits a separate Slurm job to provision the Spark cluster.
# SlurmSparkManager auto-detects Apptainer mode based on FAIRWAY_SIF/fairway.sif.
#
# IMPORTANT: fairway spark start/stop run on HOST (not in container) because they:
#   1. Run sbatch/scancel (Slurm commands only available on host)
#   2. Read/write state files on host filesystem
#   3. The sbatch job they create handles containerization internally

# Use job-specific state directory to prevent race conditions with concurrent jobs
STATE_DIR="$HOME/.fairway-spark/$SLURM_JOB_ID"
mkdir -p "$STATE_DIR"

# Define cleanup function to ensure cluster is stopped when driver exits
cleanup() {
    echo "Stopping Spark Cluster..."
    fairway spark stop --driver-job-id $SLURM_JOB_ID || true
    # Clean up state directory
    rm -rf "$STATE_DIR"
}
trap cleanup EXIT

echo "Starting Spark Cluster (using config/spark.yaml)..."
fairway spark start --driver-job-id $SLURM_JOB_ID

# Wait for Master URL and conf dir (job-specific paths)
MASTER_URL_FILE="$STATE_DIR/master_url.txt"
CONF_DIR_FILE="$STATE_DIR/conf_dir.txt"
SPARK_ARGS=""

if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
    SPARK_ARGS="--spark-master $SPARK_MASTER"
else
    echo "WARNING: Spark Master URL file not found. Running in local mode."
fi

# Export SPARK_CONF_DIR so the driver picks up auth settings from spark-start
if [ -f "$CONF_DIR_FILE" ]; then
    export SPARK_CONF_DIR=$(cat "$CONF_DIR_FILE")
    echo "Spark Conf Dir: $SPARK_CONF_DIR"
fi

# =============================================================================
# 2. Run Pipeline
# =============================================================================
echo ""
echo "Running pipeline with config: $CONFIG_PATH"

if [ "$USE_APPTAINER" = "yes" ]; then
    echo "Executing inside Apptainer container..."

    # Build comprehensive bind paths
    # Include: data directories, spark config, project directory, home spark dirs
    # Use --no-home to prevent classpath pollution from user's home directory
    BIND_PATHS="${FAIRWAY_BINDS}"
    BIND_PATHS="${BIND_PATHS},${HOME}/.spark-local"
    BIND_PATHS="${BIND_PATHS},${HOME}/.fairway-spark"
    BIND_PATHS="${BIND_PATHS},${PWD}"

    # Add SPARK_CONF_DIR if set and different from above
    if [ -n "$SPARK_CONF_DIR" ]; then
        BIND_PATHS="${BIND_PATHS},${SPARK_CONF_DIR}"
    fi

    # Add /tmp for scratch space
    BIND_PATHS="${BIND_PATHS},/tmp"

    echo "Bind paths: $BIND_PATHS"

    # Run pipeline inside container
    # Use --no-home to prevent classpath pollution (e.g., corrupted ~/.ivy2)
    # Pass through SPARK_CONF_DIR so PySpark finds auth settings
    apptainer exec --no-home \
        --bind "$BIND_PATHS" \
        --env SPARK_CONF_DIR="${SPARK_CONF_DIR}" \
        "$FAIRWAY_SIF" \
        fairway run --config "$CONFIG_PATH" $SPARK_ARGS
else
    fairway run --config "$CONFIG_PATH" $SPARK_ARGS
fi

echo ""
echo "============================================================"
echo "Pipeline completed successfully."
echo "============================================================"
