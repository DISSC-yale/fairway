#!/bin/bash
#SBATCH --job-name=fairway_pipeline
#SBATCH --output=%(log_dir)s/fairway_%%j.log
#SBATCH --time=%(slurm_time)s
#SBATCH --mem=%(mem)s
#SBATCH --cpus-per-task=%(cpus)s
#SBATCH --partition=%(partition)s
#SBATCH --account=%(account)s

set -e

# Ensure log directory exists
mkdir -p %(log_dir)s

# =============================================================================
# Apptainer Mode Detection
# =============================================================================
# Detect if we should use Apptainer mode based on:
# 1. %(sif_env_var)s environment variable
# 2. Local %(default_sif)s file
USE_APPTAINER="no"
FAIRWAY_SIF="${%(sif_env_var)s:-%(default_sif)s}"
FAIRWAY_BINDS="%(apptainer_binds)s"

if [ -f "$FAIRWAY_SIF" ]; then
    USE_APPTAINER="yes"
    echo "Apptainer mode detected: $FAIRWAY_SIF"
else
    echo "Bare-metal mode (no container found at $FAIRWAY_SIF)"
fi

# =============================================================================
# Environment Setup
# =============================================================================
if [ "$USE_APPTAINER" = "yes" ]; then
    echo "Using Apptainer container for Spark cluster and driver"
    # Export for SlurmSparkManager to detect
    export FAIRWAY_SIF
    export FAIRWAY_BINDS
else
    # Load Spark module only in bare-metal mode (container has its own Spark)
    echo "Loading Spark module..."
    module load Spark/3.5.1-foss-2022b-Scala-2.13 2>/dev/null || echo "WARNING: Could not load Spark module"
fi

# Use job-specific state directory to prevent race conditions with concurrent jobs
STATE_DIR="$HOME/.fairway-spark/$SLURM_JOB_ID"
mkdir -p "$STATE_DIR"

# Cleanup function to stop Spark cluster on exit
# NOTE: fairway spark stop runs on HOST (not in container) because it:
#   1. Reads state files from host filesystem
#   2. Runs scancel (Slurm command) to cancel the cluster job
cleanup() {
    echo "Stopping Spark cluster..."
    fairway spark stop --driver-job-id $SLURM_JOB_ID || true
    # Clean up state directory
    rm -rf "$STATE_DIR"
}
trap cleanup EXIT

# Start Spark cluster with job isolation
echo "Starting Spark cluster..."
fairway spark start --driver-job-id $SLURM_JOB_ID

# Wait for master URL and conf dir (job-specific paths)
MASTER_URL_FILE="$STATE_DIR/master_url.txt"
CONF_DIR_FILE="$STATE_DIR/conf_dir.txt"
SPARK_ARGS=""
if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
    SPARK_ARGS="--spark-master $SPARK_MASTER"
else
    echo "WARNING: Spark master URL not found. Running in local mode."
fi

# Export SPARK_CONF_DIR so the driver picks up auth settings (SASL secret) from spark-start
if [ -f "$CONF_DIR_FILE" ]; then
    export SPARK_CONF_DIR=$(cat "$CONF_DIR_FILE")
    echo "Spark Conf Dir: $SPARK_CONF_DIR"
fi

# =============================================================================
# Run Pipeline
# =============================================================================
echo "Running pipeline..."
if [ "$USE_APPTAINER" = "yes" ]; then
    # Build bind paths for Apptainer
    # Use --no-home to prevent classpath pollution from user's home directory
    BIND_PATHS="${FAIRWAY_BINDS}"
    BIND_PATHS="${BIND_PATHS},${HOME}/.spark-local"
    BIND_PATHS="${BIND_PATHS},${HOME}/.fairway-spark"
    BIND_PATHS="${BIND_PATHS},${PWD}"
    BIND_PATHS="${BIND_PATHS},/tmp"
    if [ -n "$SPARK_CONF_DIR" ]; then
        BIND_PATHS="${BIND_PATHS},${SPARK_CONF_DIR}"
    fi
    echo "Running inside Apptainer with binds: $BIND_PATHS"

    # Ensure all bind-mount source directories exist on this node.
    IFS=',' read -ra _bind_dirs <<< "$BIND_PATHS"
    for _dir in "${_bind_dirs[@]}"; do
        _dir="${_dir%%%%:*}"
        _dir="$(echo "${_dir}" | xargs)"
        [ -n "${_dir}" ] && mkdir -p "${_dir}" 2>/dev/null || true
    done
    unset _bind_dirs _dir

    apptainer exec --no-home \
        --bind "$BIND_PATHS" \
        --env SPARK_CONF_DIR="${SPARK_CONF_DIR}" \
        "$FAIRWAY_SIF" \
        fairway run --config %(config)s $SPARK_ARGS%(summary_flag)s
else
    fairway run --config %(config)s $SPARK_ARGS%(summary_flag)s
fi

echo "Pipeline completed successfully."
