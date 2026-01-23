#!/bin/bash
#SBATCH --job-name=fairway_schema_driver
#SBATCH --output=logs/schema_driver_%j.log
#SBATCH --time=24:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

# =============================================================================
# Fairway Schema Generation Driver Job
# =============================================================================
# This script submits a driver job that provisions a Spark cluster and then
# runs the schema generation pipeline distributedly.

export FAIRWAY_VENV=$FAIRWAY_VENV

# Source modules environment
source scripts/fairway-hpc.sh setup-spark

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
MASTER_URL_FILE=~/spark_master_url.txt
if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
else
    echo "WARNING: Spark Master URL file not found after start. Running in local mode?"
    SPARK_MASTER="local[*]"
fi

# =============================================================================
# 2. Run Schema Generation
# =============================================================================
echo "Running Schema Generation..."
# Note: We pass --internal-run to indicate we are already in a computed environment
# and pass the discovered spark master.
fairway generate-schema --config config/fairway.yaml --internal-run --spark-master "$SPARK_MASTER"
