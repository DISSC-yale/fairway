#!/bin/bash
#SBATCH --job-name=fairway_summary
#SBATCH --output=%(log_dir)s/summary_%%j.log
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
    echo "Using Apptainer container for summarization"
    export FAIRWAY_SIF
    export FAIRWAY_BINDS
else
    # Load Spark module only in bare-metal mode (container has its own Spark)
    echo "Loading Spark module..."
    module load Spark/3.5.1-foss-2022b-Scala-2.13 2>/dev/null || echo "WARNING: Could not load Spark module"
fi

# =============================================================================
# Run Summarization
# =============================================================================
echo "Running summarization for config: %(config)s"
if [ "$USE_APPTAINER" = "yes" ]; then
    # Build bind paths for Apptainer
    BIND_PATHS="${FAIRWAY_BINDS}"
    BIND_PATHS="${BIND_PATHS},${PWD}"
    BIND_PATHS="${BIND_PATHS},/tmp"
    echo "Running inside Apptainer with binds: $BIND_PATHS"

    # Ensure all bind-mount source directories exist on this node
    IFS=',' read -ra _bind_dirs <<< "$BIND_PATHS"
    for _dir in "${_bind_dirs[@]}"; do
        _dir="${_dir%%%%:*}"
        _dir="$(echo "${_dir}" | xargs)"
        [ -n "${_dir}" ] && mkdir -p "${_dir}" 2>/dev/null || true
    done
    unset _bind_dirs _dir

    apptainer exec --no-home \
        --bind "$BIND_PATHS" \
        "$FAIRWAY_SIF" \
        fairway summarize --config %(config)s
else
    fairway summarize --config %(config)s
fi

echo "Summary generation completed successfully."
