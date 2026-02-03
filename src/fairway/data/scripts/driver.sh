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
#
# Usage:
#   make submit-hpc TABLE=my_table [N_BATCHES=10]
#   make submit-schema   # Schema phase only (no Spark)
#   make submit-ingest   # Ingest phase only (starts Spark)
#   OR
#   FAIRWAY_TABLE=my_table FAIRWAY_ENTRY=schema sbatch scripts/driver.sh

# =============================================================================
# Configuration
# =============================================================================

# Table to process (from env var set by Makefile)
TABLE=${FAIRWAY_TABLE:-}
N_BATCHES=${FAIRWAY_N_BATCHES:-}
ENTRY=${FAIRWAY_ENTRY:-}  # 'schema', 'ingest', or empty for full pipeline

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

echo "=================================================="
echo "Fairway Driver Job Starting"
echo "=================================================="
echo "Table: ${TABLE:-'(all tables)'}"
echo "N_Batches: ${N_BATCHES:-'(auto)'}"
echo "Entry: ${ENTRY:-'(full pipeline)'}"
echo "Apptainer: $HAS_APPTAINER"
echo "=================================================="

# =============================================================================
# 1. Start Spark Cluster (only if needed)
# =============================================================================
SPARK_ARGS=""

# Schema phase doesn't need Spark
if [ "$ENTRY" != "schema" ]; then
    # Define cleanup function to ensure cluster is stopped when driver exits
    cleanup() {
        echo "Stopping Spark Cluster..."
        fairway spark stop || true
    }
    trap cleanup EXIT

    echo "Starting Spark Cluster..."
    fairway spark start

    # Wait/Read Master URL
    MASTER_URL_FILE=~/spark_master_url.txt
    if [ -f "$MASTER_URL_FILE" ]; then
        SPARK_MASTER=$(cat "$MASTER_URL_FILE")
        echo "Spark Master: $SPARK_MASTER"
        SPARK_ARGS="--spark-master $SPARK_MASTER"
    else
        echo "WARNING: Spark Master URL file not found. Running in local mode."
    fi
else
    echo "Schema phase - skipping Spark cluster startup"
fi

# =============================================================================
# 2. Build Nextflow Arguments
# =============================================================================
NF_ARGS=""

if [ -n "$TABLE" ]; then
    NF_ARGS="$NF_ARGS --table $TABLE"
fi

if [ -n "$N_BATCHES" ]; then
    NF_ARGS="$NF_ARGS --n_batches $N_BATCHES"
fi

if [ -n "$ENTRY" ]; then
    NF_ARGS="$NF_ARGS -entry $ENTRY"
fi

# =============================================================================
# 3. Run Pipeline
# =============================================================================
echo "Running Nextflow pipeline..."

if [ "$HAS_APPTAINER" = "yes" ]; then
    ./scripts/run_pipeline.sh -profile slurm,apptainer $NF_ARGS $SPARK_ARGS -resume
else
    ./scripts/run_pipeline.sh -profile slurm $NF_ARGS $SPARK_ARGS -resume
fi

echo "=================================================="
echo "Fairway Driver Job Complete"
echo "=================================================="
