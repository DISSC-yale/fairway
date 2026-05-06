#!/bin/bash
# fairway job-array template — substituted by _sbatch.render_array_script.
#SBATCH --job-name=fairway-{table}
{account}{partition}#SBATCH --array=0-{last}%{conc}
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --output=logs/fairway-{table}-%A_%a.out
set -euo pipefail
mkdir -p logs
cd "{root}"
source .venv/bin/activate
exec python -m fairway _shard {table} --shards-file "{shards}" --shard-index $SLURM_ARRAY_TASK_ID
