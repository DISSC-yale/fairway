# Contributing Guide

Development workflow, environment setup, and testing procedures for Fairway.

## Prerequisites

- Python 3.10+
- Java 8+ (for PySpark/Spark features)
- Apptainer/Singularity (for container builds, optional)

## Environment Setup

### 1. Clone the Repository

```bash
git clone https://github.com/DISSC-yale/fairway.git
cd fairway
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or: .venv\Scripts\activate  # Windows
```

### 3. Install Dependencies

```bash
# Core installation (lightweight)
pip install -e .

# With DuckDB support (recommended for local dev)
pip install -e ".[duckdb]"

# With PySpark support
pip install -e ".[spark]"

# With all dependencies
pip install -e ".[all]"

# For documentation
pip install -e ".[docs]"
```

## Project Structure

```
fairway/
├── src/fairway/           # Main package
│   ├── cli.py             # CLI entry points
│   ├── data/              # Container definitions, Makefile
│   └── ...
├── tests/                 # Test suite
│   └── fixtures/          # Test data
├── docs/                  # Documentation (MkDocs)
├── config/                # Example configurations
└── pyproject.toml         # Project metadata and dependencies
```

## Available Scripts

From `pyproject.toml`:

| Command | Description |
|---------|-------------|
| `fairway` | Main CLI entry point (`fairway.cli:main`) |

From `src/fairway/data/Makefile` (run from project root after `fairway init`):

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make run` | Run full pipeline locally (ingest + summary) |
| `make ingest` | Run ingestion only (skip summary) |
| `make summary` | Generate summary stats locally (requires JAVA_HOME) |
| `make submit` | Submit ingestion to Slurm with Spark cluster |
| `make summary-hpc` | Submit summary generation to Slurm |
| `make shell` | Open shell in container |
| `make build` | Build container image |
| `make schema` | Generate schema from raw data |
| `make schema-hpc` | Submit schema generation to Slurm |
| `make clean` | Remove logs and cache |
| `make shell-dev` | Open shell with local src/ mounted (no rebuild) |
| `make run-dev` | Run pipeline with local src/ mounted (no rebuild) |

## Dependencies

### Core Dependencies
- `click` - CLI framework
- `pyyaml` - YAML configuration parsing
- `pandas` - Data manipulation
- `tabulate` - Table formatting

### Optional Dependencies

| Extra | Packages | Use Case |
|-------|----------|----------|
| `spark` | pyspark, delta-spark, pyarrow | Distributed processing on Slurm |
| `duckdb` | duckdb, pyarrow | Local development, smaller datasets |
| `redivis` | redivis, pyarrow | Redivis data export |
| `test-data-gen` | numpy | Generating test datasets |
| `docs` | mkdocs-material | Building documentation |
| `all` | All of the above | Full installation |

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run only local tests (no Spark required)
pytest -m local

# Run Spark tests (requires Java)
pytest -m spark

# Run with coverage
pytest --cov=fairway --cov-report=html
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `local` | Runs without Spark (DuckDB only) |
| `spark` | Requires PySpark + Java |
| `hpc` | Requires SLURM cluster |

### Test Data

Generate test datasets for development:

```bash
# Small partitioned CSV dataset
fairway generate-data --size small --partitioned

# Large Parquet dataset
fairway generate-data --size large --no-partitioned --format parquet
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/my-feature
```

### 2. Make Changes

- Write tests first (TDD encouraged)
- Follow existing code patterns
- Update documentation if adding features

### 3. Run Tests

```bash
pytest -m local  # Quick feedback
pytest           # Full test suite before PR
```

### 4. Build Documentation

```bash
mkdocs serve  # Preview at http://localhost:8000
mkdocs build  # Build static site
```

### 5. Submit Pull Request

- Ensure all tests pass
- Update CHANGELOG if applicable
- Request review

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FAIRWAY_BINDS` | Additional Apptainer bind paths (comma-separated) | Auto-detected from config |
| `FAIRWAY_TEMP` | Temporary directory for large operations | System temp |
| `REDIVIS_API_TOKEN` | API token for Redivis data export | None |
| `SPARK_LOCAL_IP` | Spark driver bind address | Auto-detect |
| `PYSPARK_SUBMIT_ARGS` | Additional Spark submit arguments | Auto-configured |

### HPC Bind Paths

When running on HPC clusters, you may need to set `FAIRWAY_BINDS` to include your cluster's shared storage:

```bash
# For clusters using /scratch
export FAIRWAY_BINDS="/scratch/$USER"

# For clusters with multiple storage paths
export FAIRWAY_BINDS="/gpfs/data,/project/mygroup"
```

Alternatively, set `apptainer_binds` in your `spark.yaml`:

```yaml
apptainer_binds: "/scratch,/gpfs"
```

## Code Style

- Follow PEP 8 guidelines
- Use type hints where practical
- Docstrings for public APIs
- Keep functions focused and testable

## Troubleshooting

### Java Not Found (Spark tests)

```bash
# macOS
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Linux
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### DuckDB Import Errors

```bash
pip install --upgrade duckdb pyarrow
```

### Container Build Issues

Ensure Apptainer is installed:
```bash
apptainer --version
# or build with Docker fallback:
fairway build --docker
```
