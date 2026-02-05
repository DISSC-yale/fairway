# Fairway Project Makefile
.PHONY: help run submit shell build clean schema schema-hpc

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

run: ## Run pipeline locally
	fairway run

submit: ## Submit pipeline to Slurm (with Spark cluster)
	fairway submit --with-spark

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
