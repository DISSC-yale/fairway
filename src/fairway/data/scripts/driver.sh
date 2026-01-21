#!/bin/bash
#SBATCH --job-name=fairway_driver
#SBATCH --output=logs/driver_%j.log
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

# =============================================================================
# Fairway Driver Job
# =============================================================================
# This script is submitted to Slurm to run the Nextflow orchestrator on a
# compute node (preventing login node usage for long-running processes).

# 3. Run Pipeline via Shared Script
# Capture Makefile argument if provided (prioritize $1, fallback to env var)
HAS_APPTAINER=${1:-$HAS_APPTAINER}

# Fallback: If not explicitly set, check file existence (for manual submission)
if [ -z "$HAS_APPTAINER" ]; then
    if [ -f "Apptainer.def" ] || [ -f "fairway.sif" ]; then
        HAS_APPTAINER="yes"
    else
        HAS_APPTAINER="no"
    fi
fi
export FAIRWAY_VENV=$FAIRWAY_VENV

# =============================================================================
# 1. Start Spark Cluster (Orchestration)
# =============================================================================
# We attempt to start a Spark cluster. The 'fairway spark start' command
# reads config/spark.yaml and submits a separate Slurm job for the cluster.

# Define cleanup function to ensure cluster is stopped when driver exits
cleanup() {
    echo "Stopping Spark Cluster..."
    fairway spark stop
}
trap cleanup EXIT

echo "Starting Spark Cluster..."
fairway spark start

# Wait/Read Master URL
# 'fairway spark start' waits for the master to be ready, but we double check
# the file it produces.
MASTER_URL_FILE=~/spark_master_url.txt
if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
    SPARK_ARGS="--spark-master $SPARK_MASTER"
else
    echo "WARNING: Spark Master URL file not found after start. Running in local mode?"
    SPARK_ARGS=""
fi

# =============================================================================
# 2. Run Pipeline via Shared Script
# =============================================================================
if [ "$HAS_APPTAINER" = "yes" ]; then
    ./scripts/run_pipeline.sh -profile slurm,apptainer -resume $SPARK_ARGS
else
    ./scripts/run_pipeline.sh -profile slurm -resume $SPARK_ARGS
fi
