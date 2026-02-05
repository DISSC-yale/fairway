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
# Spark cluster configuration is read from config/spark.yaml by `fairway spark start`.
# This includes: nodes, cpus_per_node, mem_per_node, partition, account, time.
#
# Usage:
#   sbatch scripts/driver.sh [config_path]
#
# Environment Variables:
#   USE_APPTAINER - Set to "yes" to run inside Apptainer container
#   FAIRWAY_VENV  - Path to Python virtual environment to activate
#
# Prefer using `fairway submit --with-spark` instead of this script directly.

set -e

CONFIG_PATH=${1:-config/fairway.yaml}

# Detect Apptainer: explicit env var or file presence
USE_APPTAINER=${USE_APPTAINER:-}
if [ -z "$USE_APPTAINER" ]; then
    if [ -f "fairway.sif" ]; then
        USE_APPTAINER="yes"
    else
        USE_APPTAINER="no"
    fi
fi

# Ensure log directory exists
mkdir -p logs/slurm

# Load virtual environment if specified (only if not using container)
if [ "$USE_APPTAINER" != "yes" ] && [ -n "$FAIRWAY_VENV" ]; then
    if [ -f "$FAIRWAY_VENV/bin/activate" ]; then
        echo "Activating virtual environment: $FAIRWAY_VENV"
        source "$FAIRWAY_VENV/bin/activate"
    fi
fi

# =============================================================================
# 1. Start Spark Cluster
# =============================================================================
# The 'fairway spark start' command reads config/spark.yaml for cluster sizing
# and submits a separate Slurm job to provision the Spark cluster.

# Define cleanup function to ensure cluster is stopped when driver exits
cleanup() {
    echo "Stopping Spark Cluster..."
    fairway spark stop || true
}
trap cleanup EXIT

echo "Starting Spark Cluster (using config/spark.yaml)..."
fairway spark start

# Wait for Master URL (written by fairway spark start)
MASTER_URL_FILE=~/spark_master_url.txt
SPARK_ARGS=""
if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
    SPARK_ARGS="--spark-master $SPARK_MASTER"
else
    echo "WARNING: Spark Master URL file not found. Running in local mode."
fi

# =============================================================================
# 2. Run Pipeline
# =============================================================================
echo "Running pipeline with config: $CONFIG_PATH"

if [ "$USE_APPTAINER" = "yes" ]; then
    echo "Running inside Apptainer container..."
    apptainer exec --bind /vast fairway.sif fairway run --config "$CONFIG_PATH" $SPARK_ARGS
else
    fairway run --config "$CONFIG_PATH" $SPARK_ARGS
fi

echo "Pipeline completed successfully."
