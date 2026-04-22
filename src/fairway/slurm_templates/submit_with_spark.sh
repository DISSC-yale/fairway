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
# Pin resolver roots so host-side spark start/stop, run-abandon, and
# the driver all resolve one shared fairway state tree.
export FAIRWAY_HOME="%(fairway_home)s"
export FAIRWAY_SCRATCH="%(fairway_scratch)s"

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
STATE_DIR="%(spark_coordination_dir)s/$SLURM_JOB_ID"
mkdir -p "$STATE_DIR"

# Pre-compute run_id + month shard so the pipeline and the EXIT trap
# both reference the same run.json. Use UTC so midnight-crossing jobs
# don't flip the month shard between pipeline start and trap fire.
# Include $$ (shell PID) to avoid collisions when SLURM_JOB_ID is unset.
export FAIRWAY_RUN_ID="${FAIRWAY_RUN_ID:-slurm_${SLURM_JOB_ID:-local}_$(date -u +%%s)_$$}"
export FAIRWAY_RUN_MONTH="${FAIRWAY_RUN_MONTH:-$(date -u +%%Y-%%m)}"

# The EXIT trap runs on the submit host. In pure-container deployments
# `fairway` may not be on host PATH, which makes run-abandon a no-op.
if ! command -v fairway >/dev/null 2>&1; then
    echo "WARNING: 'fairway' not on host PATH; OOM/walltime run-abandon trap will be a no-op." >&2
fi

# Cleanup function: stop Spark cluster AND finalize run.json on exit.
# NOTE: fairway spark stop runs on HOST (not in container) because it:
#   1. Reads state files from host filesystem
#   2. Runs scancel (Slurm command) to cancel the cluster job
# run-abandon is idempotent — no-op when run is already terminal.
cleanup() {
    echo "Stopping Spark cluster..."
    fairway spark stop --config %(config)s --driver-job-id $SLURM_JOB_ID || true
    # Clean up state directory
    rm -rf "$STATE_DIR"
    # Belt-and-suspenders finalization for OOM/walltime kills.
    fairway run-abandon --config %(config)s "${FAIRWAY_RUN_ID}" || true
}
trap cleanup EXIT

# Start Spark cluster with job isolation
echo "Starting Spark cluster..."
fairway spark start --driver-job-id $SLURM_JOB_ID%(spark_start_args)s

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
    BIND_PATHS="${BIND_PATHS},${FAIRWAY_HOME}"
    BIND_PATHS="${BIND_PATHS},${FAIRWAY_SCRATCH}"
    BIND_PATHS="${BIND_PATHS},${STATE_DIR}"
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

    # Forward run_id + run_month so the container's pipeline writes to
    # the same run.json the host trap will look up.
    apptainer exec --no-home \
        --bind "$BIND_PATHS" \
        --env FAIRWAY_HOME="${FAIRWAY_HOME}" \
        --env FAIRWAY_SCRATCH="${FAIRWAY_SCRATCH}" \
        --env SPARK_CONF_DIR="${SPARK_CONF_DIR}" \
        --env FAIRWAY_RUN_ID="${FAIRWAY_RUN_ID}" \
        --env FAIRWAY_RUN_MONTH="${FAIRWAY_RUN_MONTH}" \
        "$FAIRWAY_SIF" \
        fairway run --config %(config)s $SPARK_ARGS%(summary_flag)s
else
    fairway run --config %(config)s $SPARK_ARGS%(summary_flag)s
fi

echo "Pipeline completed successfully."
