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

# Run pipeline (no Spark cluster)
echo "Running pipeline..."
fairway run --config %(config)s%(summary_flag)s

echo "Pipeline completed successfully."
