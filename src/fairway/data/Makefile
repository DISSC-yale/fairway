# Fairway Project Makefile
.PHONY: help run submit shell build clean schema schema-hpc
.PHONY: shell-dev run-dev build-base build-from-branch

.DEFAULT_GOAL := help

# Git branch for container builds (override with: make build BRANCH=my-branch)
BRANCH ?= main
FAIRWAY_SIF ?= fairway.sif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Standard Commands
# ============================================================================

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

build-base: ## Build base image with Spark/Java (slow, do once)
	@echo "Building base image (this takes ~10 min)..."
	@if [ ! -f Apptainer-base.def ]; then \
		echo "Creating Apptainer-base.def..."; \
		cat > Apptainer-base.def <<'EOF'
Bootstrap: docker
From: python:3.10-slim-bookworm

%post
    set -e
    apt-get update && apt-get install -y --no-install-recommends \
        git curl openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*
    SPARK_VERSION=4.1.1
    curl -sL "https://archive.apache.org/dist/spark/spark-$${SPARK_VERSION}/spark-$${SPARK_VERSION}-bin-hadoop3.tgz" | tar -xz -C /opt
    ln -s /opt/spark-$${SPARK_VERSION}-bin-hadoop3 /opt/spark
    python3 -m venv /opt/venv
    /opt/venv/bin/pip install --upgrade pip setuptools wheel

%environment
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export SPARK_HOME=/opt/spark
    export VIRTUAL_ENV=/opt/venv
    export PATH="/opt/venv/bin:$$PATH:/opt/spark/bin"
EOF
	fi
	apptainer build --force fairway-base.sif Apptainer-base.def

build-thin: fairway-base.sif ## Build thin layer on top of base (fast, ~1-2 min)
	@echo "Building thin layer from branch: $(BRANCH)"
	@cat > Apptainer-thin.def <<'EOF'
Bootstrap: localimage
From: fairway-base.sif

%post
    /opt/venv/bin/pip install --no-cache-dir "fairway[all] @ git+https://github.com/DISSC-yale/fairway.git@$(BRANCH)"

%environment
    export PYSPARK_PYTHON=/opt/venv/bin/python
    export PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

%runscript
    exec fairway "$$@"
EOF
	apptainer build --force $(FAIRWAY_SIF) Apptainer-thin.def
