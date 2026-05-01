# Fairway Project Makefile
.PHONY: help run ingest summary submit clean schema schema-hpc

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Standard Commands (container/spark targets removed in v0.3 Step 2)
# ============================================================================

run: ## Run full pipeline locally (ingest + summary)
	fairway run

ingest: ## Run ingestion only (skip summary generation)
	fairway run --skip-summary

summary: ## Generate summary stats locally
	fairway summarize

submit: ## Submit ingestion to Slurm
	fairway submit

schema: ## Generate schema from raw data
	fairway generate-schema --config config/fairway.yaml

schema-hpc: ## Submit schema generation to Slurm
	sbatch scripts/driver-schema.sh

clean: ## Remove logs and cache
	rm -rf logs/ .fairway_cache/

