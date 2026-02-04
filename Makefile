# Fairway Root Makefile
# Convenience targets for development

.PHONY: test test-unit test-nextflow install clean help

# Nextflow binary (local install takes precedence)
NF_BIN := $(shell command -v nextflow 2>/dev/null || [ -x "./nextflow" ] && echo "./nextflow")

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ==============================================================================
# Installation
# ==============================================================================

install: ## Install fairway in development mode
	pip install -e .

install-nextflow: ## Install Nextflow locally if not present
	@if command -v nextflow >/dev/null 2>&1 || [ -x "./nextflow" ]; then \
		echo "Nextflow already installed"; \
	else \
		echo "Installing Nextflow..."; \
		curl -s https://get.nextflow.io | bash; \
	fi

# ==============================================================================
# Testing
# ==============================================================================

test: install-nextflow ## Run all tests (unit + Nextflow integration)
	@echo "=== Running All Tests ==="
	PATH="$$(pwd):$$PATH" python -m pytest tests/test_cli_batch.py tests/test_nextflow_integration.py -v

test-unit: ## Run unit tests only (no Nextflow required)
	python -m pytest tests/test_cli_batch.py -v

test-nextflow: install-nextflow ## Run Nextflow integration tests only
	@echo "=== Running Nextflow Integration Tests ==="
	PATH="$$(pwd):$$PATH" python -m pytest tests/test_nextflow_integration.py -v

test-all: install-nextflow ## Run complete test suite
	PATH="$$(pwd):$$PATH" python -m pytest tests/ -v

# ==============================================================================
# Cleanup
# ==============================================================================

clean: ## Clean build artifacts and caches
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .nextflow* work/

clean-all: clean ## Clean everything including Nextflow
	rm -f nextflow
	rm -rf .nextflow/
