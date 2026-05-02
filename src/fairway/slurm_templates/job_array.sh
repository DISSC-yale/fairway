#!/bin/bash
# fairway job-array template — substituted by _sbatch.render_script.
#SBATCH --job-name=fairway-{dataset}
{account}{partition}#SBATCH --array=0-{last}%{conc}
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --output=logs/fairway-{dataset}-%A_%a.out
set -euo pipefail
mkdir -p logs
cd {root}
source .venv/bin/activate
exec python -m fairway run --shards-file {shards} --shard-index $SLURM_ARRAY_TASK_ID
