#!/bin/bash
#SBATCH --job-name=fairway_schema
#SBATCH --output=logs/schema_%j.log
#SBATCH --time=04:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4

# =============================================================================
# Fairway Schema Generation Driver Job
# =============================================================================
# This script runs schema inference using DuckDB on a single compute node.
# No Spark cluster is required - DuckDB handles schema discovery efficiently
# with random sampling and union_by_name for multi-file schema merging.
#
# For scalability considerations with 1000s of files on slow storage,
# see docs/design_docs/schema_inference_scalability.md

set -e

export FAIRWAY_VENV=$FAIRWAY_VENV

# Load base modules (no Spark needed for schema inference)
source scripts/fairway-hpc.sh setup

# Activate virtual environment if specified
if [ -n "$FAIRWAY_VENV" ] && [ -d "$FAIRWAY_VENV" ]; then
    echo "Activating virtual environment: $FAIRWAY_VENV"
    source "$FAIRWAY_VENV/bin/activate"
fi

# =============================================================================
# Run Schema Generation (DuckDB-based)
# =============================================================================
echo "Running Schema Generation with DuckDB..."
echo "  - Uses random row sampling (default 10%)"
echo "  - Merges schemas across all files (union_by_name)"
echo ""

# Run schema generation - CLI will use engine from config (defaults to DuckDB)
# No --spark-master means DuckDB will be used
fairway generate-schema --config config/fairway.yaml --internal-run

echo ""
echo "Schema generation complete. Check output for generated schema files."
