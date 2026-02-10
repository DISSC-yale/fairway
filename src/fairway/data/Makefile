# Fairway Project Makefile
.PHONY: help run ingest summary submit summary-hpc shell build clean schema schema-hpc

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

run: ## Run full pipeline locally (ingest + summary)
	fairway run

ingest: ## Run ingestion only (skip summary generation)
	fairway run --skip-summary

summary: ## Generate summary stats locally (requires JAVA_HOME)
	fairway summarize

submit: ## Submit full pipeline to Slurm (with Spark cluster)
	fairway submit --with-spark

summary-hpc: ## Submit summary generation to Slurm (loads Java/Spark)
	fairway summarize --slurm

shell: ## Open shell in container
	fairway shell

build: ## Build container image
	fairway build

schema: ## Generate schema from raw data
	fairway generate-schema --config config/fairway.yaml

schema-hpc: ## Submit schema generation to Slurm
	sbatch scripts/driver-schema.sh

clean: ## Remove logs and cache
	rm -rf logs/ .fairway_cache/
