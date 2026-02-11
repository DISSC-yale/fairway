# Fairway Project Makefile
.PHONY: help run ingest summary submit summary-hpc shell build clean schema schema-hpc
.PHONY: shell-dev run-dev build-from-branch

.DEFAULT_GOAL := help

# Git branch for container builds (override with: make build-from-branch BRANCH=my-branch)
BRANCH ?= main
FAIRWAY_SIF ?= fairway.sif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Standard Commands
# ============================================================================

run: ## Run full pipeline locally (ingest + summary)
	fairway run

ingest: ## Run ingestion only (skip summary generation)
	fairway run --skip-summary

summary: ## Generate summary stats locally (requires JAVA_HOME)
	fairway summarize

submit: ## Submit ingestion to Slurm (with Spark cluster, no summary)
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

# ============================================================================
# Development Commands (fast iteration without rebuilding container)
# ============================================================================

shell-dev: ## Open shell with local src/ mounted (no rebuild needed)
	fairway shell --dev

run-dev: ## Run pipeline with local src/ mounted (no rebuild needed)
	@echo "Running with local source mounted..."
	apptainer exec \
		--bind src/fairway:/opt/venv/lib/python3.10/site-packages/fairway \
		--bind $${PWD},/vast,$${HOME}/.spark-local \
		$(FAIRWAY_SIF) fairway run

# ============================================================================
# Container Build Variants
# ============================================================================

build-from-branch: ## Build container from specific branch (usage: make build-from-branch BRANCH=fix-apptainer)
	@echo "Building container from branch: $(BRANCH)"
	FAIRWAY_GIT_REF=$(BRANCH) apptainer build --force $(FAIRWAY_SIF) Apptainer.def
