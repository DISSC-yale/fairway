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

# Pin resolver roots so every fairway subprocess in this job resolves
# the same state/scratch locations as the submit host.
export FAIRWAY_HOME="%(fairway_home)s"
export FAIRWAY_SCRATCH="%(fairway_scratch)s"

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

# Belt-and-suspenders: if the job is OOM-killed or hits walltime before
# the pipeline can finalize run.json, run-abandon idempotently marks
# the run as unfinished (no-op when already terminal).
trap 'fairway run-abandon --config %(config)s "${FAIRWAY_RUN_ID}" || true' EXIT

# Run pipeline (no Spark cluster)
echo "Running pipeline..."
fairway run --config %(config)s%(summary_flag)s

echo "Pipeline completed successfully."
