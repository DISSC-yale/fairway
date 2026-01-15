"""Template files for fairway init command.

These are embedded directly in the package to ensure they're available
when fairway is installed via pip.
"""

HPC_SCRIPT = r'''#!/bin/bash
# =============================================================================
# fairway-hpc.sh - HPC Module Management and Job Submission for Fairway
# =============================================================================
# 
# This script helps manage module loading and job submission for Fairway
# on HPC clusters running Slurm. Supports both module-based and Apptainer
# container-based execution.
#
# Usage:
#   source fairway-hpc.sh setup           # Load required modules
#   source fairway-hpc.sh setup-spark     # Load modules including Spark
#   fairway-hpc.sh run [options]          # Submit a fairway job
#   fairway-hpc.sh run --apptainer        # Submit using Apptainer container
#   fairway-hpc.sh build-container        # Build/pull Apptainer image
#   fairway-hpc.sh status                 # Check running fairway jobs
#   fairway-hpc.sh cancel [job_id]        # Cancel a fairway job
#
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Configuration - Customize these for your HPC environment
# -----------------------------------------------------------------------------
DEFAULT_ACCOUNT="borzekowski"
DEFAULT_PARTITION="day"
DEFAULT_TIME="24:00:00"
DEFAULT_NODES=2
DEFAULT_CPUS=32
DEFAULT_MEM="200G"

# Container configuration
CONTAINER_IMAGE="docker://ghcr.io/dissc-yale/fairway:latest"
CONTAINER_LOCAL="fairway.sif"

# Module names - adjust these to match your HPC's module system
MODULES_BASE=(
    "Nextflow/25.04.6"
    "Python/3.10.8-GCCcore-12.2.0"
)

MODULES_SPARK=(
    "Java/11.0.20"
    "Spark/3.5.0"
)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

print_header() {
    echo ""
    echo "============================================================"
    echo "  Fairway HPC Helper"
    echo "============================================================"
}

print_usage() {
    cat << EOF

Usage: fairway-hpc.sh <command> [options]

Commands:
  setup             Load base modules (Nextflow, Python)
  setup-spark       Load all modules including Spark
  run               Submit a fairway pipeline job
  build-container   Build/pull the Fairway Apptainer image
  status            Show status of fairway jobs
  cancel <id>       Cancel a fairway job

Run Options:
  --config <file>     Config file path (default: auto-discover)
  --profile <name>    Nextflow profile (default: slurm)
  --nodes <n>         Number of nodes (default: $DEFAULT_NODES)
  --cpus <n>          CPUs per node (default: $DEFAULT_CPUS)
  --mem <size>        Memory per node (default: $DEFAULT_MEM)
  --time <hh:mm:ss>   Time limit (default: $DEFAULT_TIME)
  --account <name>    Slurm account (default: $DEFAULT_ACCOUNT)
  --partition <name>  Slurm partition (default: $DEFAULT_PARTITION)
  --with-spark        Enable Spark cluster provisioning
  --apptainer         Use Apptainer container instead of modules

Examples:
  # Load modules interactively
  source fairway-hpc.sh setup

  # Submit a job with modules (traditional)
  fairway-hpc.sh run --config config/mydata.yaml

  # Submit using Apptainer container (recommended - reproducible)
  fairway-hpc.sh run --config config/mydata.yaml --apptainer

  # Build/pull the container first
  fairway-hpc.sh build-container

EOF
}

load_modules() {
    echo "Loading HPC modules..."
    
    # Clear any conflicting modules first
    module purge 2>/dev/null || true
    
    for mod in "${MODULES_BASE[@]}"; do
        echo "  Loading: $mod"
        module load "$mod" 2>/dev/null || {
            echo "  Warning: Could not load module '$mod'"
            echo "  Available modules can be listed with: module avail"
        }
    done
    
    echo ""
    echo "Modules loaded successfully."
    echo "Python: $(which python)"
    echo ""
    echo "To install fairway in your environment:"
    echo "  pip install --user -r requirements.txt"
    echo "  # Or manually: pip install --user git+https://github.com/DISSC-yale/fairway.git#egg=fairway[all]"
}

load_spark_modules() {
    load_modules
    
    echo ""
    echo "Loading Spark modules..."
    
    for mod in "${MODULES_SPARK[@]}"; do
        echo "  Loading: $mod"
        module load "$mod" 2>/dev/null || {
            echo "  Warning: Could not load module '$mod'"
        }
    done
    
    echo ""
    echo "Spark modules loaded."
}

check_fairway() {
    if ! command -v fairway &> /dev/null; then
        echo "Error: fairway command not found"
        echo "Please ensure fairway is installed in your environment:"
        echo "  pip install git+https://github.com/DISSC-yale/fairway.git"
        exit 1
    fi
}

build_container() {
    echo "Building/pulling Fairway Apptainer image..."
    echo ""
    
    if [[ -f "$CONTAINER_LOCAL" ]]; then
        echo "Container already exists: $CONTAINER_LOCAL"
        echo "To rebuild, delete it first: rm $CONTAINER_LOCAL"
        return 0
    fi
    
    # Check if we have a Apptainer.def in current directory
    if [[ -f "Apptainer.def" ]]; then
        echo "Building from local Apptainer.def..."
        apptainer build "$CONTAINER_LOCAL" Apptainer.def
    else
        echo "Pulling from container registry..."
        echo "  Source: $CONTAINER_IMAGE"
        apptainer pull "$CONTAINER_LOCAL" "$CONTAINER_IMAGE"
    fi
    
    echo ""
    echo "Container built successfully: $CONTAINER_LOCAL"
    echo ""
    echo "Test with: apptainer exec $CONTAINER_LOCAL fairway --help"
}

submit_job() {
    # Parse arguments
    local CONFIG=""
    local PROFILE="slurm"
    local NODES=$DEFAULT_NODES
    local CPUS=$DEFAULT_CPUS
    local MEM=$DEFAULT_MEM
    local TIME=$DEFAULT_TIME
    local ACCOUNT=$DEFAULT_ACCOUNT
    local PARTITION=$DEFAULT_PARTITION
    local WITH_SPARK=""
    local USE_APPTAINER=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                CONFIG="$2"
                shift 2
                ;;
            --profile)
                PROFILE="$2"
                shift 2
                ;;
            --nodes)
                NODES="$2"
                shift 2
                ;;
            --cpus)
                CPUS="$2"
                shift 2
                ;;
            --mem)
                MEM="$2"
                shift 2
                ;;
            --time)
                TIME="$2"
                shift 2
                ;;
            --account)
                ACCOUNT="$2"
                shift 2
                ;;
            --partition)
                PARTITION="$2"
                shift 2
                ;;
            --with-spark)
                WITH_SPARK="--with-spark"
                shift
                ;;
            --apptainer)
                USE_APPTAINER=true
                shift
                ;;
            *)
                echo "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
    
    # Ensure logs directory exists
    mkdir -p logs/slurm
    
    # Determine config option
    local CONFIG_OPT=""
    if [[ -n "$CONFIG" ]]; then
        CONFIG_OPT="--config $CONFIG"
    fi
    
    # Build the sbatch script
    local JOB_NAME="fairway_$(date +%Y%m%d_%H%M%S)"
    local SBATCH_SCRIPT="logs/slurm/${JOB_NAME}.sh"
    
    cat > "$SBATCH_SCRIPT" << SBATCH_EOF
#!/bin/bash
#SBATCH --job-name=$JOB_NAME
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --time=$TIME
#SBATCH --account=$ACCOUNT
#SBATCH --partition=$PARTITION
#SBATCH --output=logs/slurm/${JOB_NAME}_%j.log
#SBATCH --error=logs/slurm/${JOB_NAME}_%j.err

# =============================================================================
# Fairway HPC Job - Auto-generated by fairway-hpc.sh
# =============================================================================

echo "========================================"
echo "Fairway Job Starting"
echo "Job ID: \$SLURM_JOB_ID"
echo "Node: \$SLURMD_NODENAME"
echo "Time: \$(date)"
echo "========================================"

SBATCH_EOF

    if [[ "$USE_APPTAINER" == true ]]; then
        # Apptainer-based execution - no modules needed
        cat >> "$SBATCH_SCRIPT" << SBATCH_EOF
# Using Apptainer container - no module loading required
CONTAINER="$CONTAINER_LOCAL"

if [[ ! -f "\$CONTAINER" ]]; then
    echo "Error: Container not found: \$CONTAINER"
    echo "Build it first with: fairway-hpc.sh build-container"
    exit 1
fi

echo "Using Apptainer container: \$CONTAINER"
echo ""

# Bind current directory and common HPC paths
APPTAINER_OPTS="--bind \$(pwd):/work --bind /vast --pwd /work"

# Run fairway inside container
apptainer exec \$APPTAINER_OPTS "\$CONTAINER" fairway run $CONFIG_OPT \\
    --profile $PROFILE \\
    --slurm-nodes $NODES \\
    --slurm-cpus $CPUS \\
    --slurm-mem $MEM \\
    --slurm-time $TIME \\
    --account $ACCOUNT \\
    --partition $PARTITION $WITH_SPARK
SBATCH_EOF
    else
        # Module-based execution
        cat >> "$SBATCH_SCRIPT" << SBATCH_EOF
# Load required modules
module purge
SBATCH_EOF

        # Add module loads
        for mod in "${MODULES_BASE[@]}"; do
            echo "module load $mod" >> "$SBATCH_SCRIPT"
        done
        
        # Add Spark modules if needed
        if [[ -n "$WITH_SPARK" ]]; then
            for mod in "${MODULES_SPARK[@]}"; do
                echo "module load $mod" >> "$SBATCH_SCRIPT"
            done
        fi
        
        cat >> "$SBATCH_SCRIPT" << SBATCH_EOF

echo ""
echo "Loaded modules:"
module list

echo ""
echo "Python location: \$(which python)"
echo "Fairway location: \$(which fairway)"
echo ""

# Run the fairway pipeline
fairway run $CONFIG_OPT \\
    --profile $PROFILE \\
    --slurm-nodes $NODES \\
    --slurm-cpus $CPUS \\
    --slurm-mem $MEM \\
    --slurm-time $TIME \\
    --account $ACCOUNT \\
    --partition $PARTITION $WITH_SPARK
SBATCH_EOF
    fi

    cat >> "$SBATCH_SCRIPT" << SBATCH_EOF

echo ""
echo "========================================"
echo "Fairway Job Completed"
echo "Time: \$(date)"
echo "========================================"
SBATCH_EOF

    chmod +x "$SBATCH_SCRIPT"
    
    echo ""
    if [[ "$USE_APPTAINER" == true ]]; then
        echo "Submitting job using Apptainer container..."
    else
        echo "Submitting job using HPC modules..."
    fi
    echo "Job name: $JOB_NAME"
    echo "Script: $SBATCH_SCRIPT"
    echo ""
    
    sbatch "$SBATCH_SCRIPT"
}

show_status() {
    echo ""
    echo "Fairway jobs for user: $USER"
    echo ""
    squeue -u "$USER" --name="fairway*" -o "%.10i %.20j %.8T %.10M %.6D %R" || {
        echo "No jobs found or squeue not available"
    }
    echo ""
}

cancel_job() {
    local JOB_ID="$1"
    
    if [[ -z "$JOB_ID" ]]; then
        echo "Error: Please specify a job ID to cancel"
        echo "Usage: fairway-hpc.sh cancel <job_id>"
        exit 1
    fi
    
    echo "Cancelling job: $JOB_ID"
    scancel "$JOB_ID"
}

# -----------------------------------------------------------------------------
# Main Command Handler
# -----------------------------------------------------------------------------

main() {
    local COMMAND="${1:-}"
    shift || true
    
    case "$COMMAND" in
        setup)
            print_header
            load_modules
            ;;
        setup-spark)
            print_header
            load_spark_modules
            ;;
        build-container)
            print_header
            build_container
            ;;
        run)
            print_header
            if [[ ! " $* " =~ " --apptainer " ]]; then
                check_fairway
            fi
            submit_job "$@"
            ;;
        status)
            print_header
            show_status
            ;;
        cancel)
            cancel_job "$@"
            ;;
        -h|--help|help|"")
            print_header
            print_usage
            ;;
        *)
            echo "Unknown command: $COMMAND"
            print_usage
            exit 1
            ;;
    esac
}

# Only run main if script is executed (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
else
    # Script was sourced - just run the command if provided
    if [[ $# -gt 0 ]]; then
        main "$@"
    fi
fi
'''

APPTAINER_DEF = '''Bootstrap: docker
From: python:3.10-slim-bookworm

%labels
    Author DISSC-Yale
    Version 1.0
    Description Fairway - Portable Data Ingestion Framework

%files
    requirements.txt /opt/requirements.txt

%post
    # Fail fast on any error
    set -e

    apt-get update && apt-get install -y --no-install-recommends \\
        git \\
        curl \\
        openjdk-17-jre-headless \\
        procps \\
        && rm -rf /var/lib/apt/lists/*
    
    # Install Nextflow
    curl -s https://get.nextflow.io | bash
    mv nextflow /usr/local/bin/
    chmod +x /usr/local/bin/nextflow
    
    # Install Spark
    SPARK_VERSION=3.5.1
    HADOOP_VERSION=3
    curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
    
    # Install project dependencies
    if [ -f /opt/requirements.txt ]; then
        pip install --no-cache-dir -r /opt/requirements.txt
    else
        # Fallback: install fairway[all]
        pip install --no-cache-dir "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[all]"
    fi

%environment
    export LC_ALL=C
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:/opt/spark/bin:/usr/local/bin

%runscript
    exec fairway "$@"

%help
    Fairway - A portable data ingestion framework for research data.
    
    Usage:
      apptainer run fairway.sif --help
      apptainer run fairway.sif run --config config/mydata.yaml
      apptainer exec fairway.sif fairway generate-schema data/raw/file.csv
    
    Building:
      apptainer build fairway.sif Apptainer.def
'''

NEXTFLOW_CONFIG = '''params {
    config = "config/fairway.yaml"
    outdir = "results"
}

profiles {
    standard {
        process.executor = 'local'
    }
    
    slurm {
        process.executor = 'slurm'
        process.queue = { params.slurm_partition ?: 'day' }
        process.memory = { params.slurm_mem ?: '16 GB' }
        process.cpus = { params.slurm_cpus_per_task ?: 4 }
        process.time = { params.slurm_time ?: '24:00:00' }
        process.clusterOptions = { "--account=${params.account ?: 'borzekowski'} --nodes=1" }
    }

    apptainer {
        apptainer.enabled = true
        apptainer.autoMounts = true
        apptainer.runOptions = '--bind /vast'
        process.container = 'docker://ghcr.io/dissc-yale/fairway:latest'
    }
}
'''

MAIN_NF = '''#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.config = "config/fairway.yaml"
params.batch_size = 30
params.spark_master = null

import org.yaml.snakeyaml.Yaml

def configPath = params.config
def configFile = new File(configPath)
def appConfig = new Yaml().load(configFile.text)

// Default to 'data/intermediate' and 'data/final' if not specified, but prefer config
def intermediateDir = appConfig.storage?.intermediate_dir ?: 'data/intermediate'
def finalDir = appConfig.storage?.final_dir ?: 'data/final'

process run_fairway {
    publishDir "${params.outdir}", mode: 'copy'
    
    input:
    path config_file

    output:
    path "${intermediateDir}/*"
    path "${finalDir}/*"

    script:
    def master_arg = params.spark_master ? "--spark_master ${params.spark_master}" : ""
    """
    fairway run --config ${config_file} ${master_arg}
    """
}

workflow {
    config_ch = Channel.fromPath(params.config)
    run_fairway(config_ch)
}
'''

DOCKERFILE_TEMPLATE = r'''# Dockerfile for Fairway Project
# Generated by fairway init

# Build stage
FROM python:3.10-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install project in editable mode
COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

# Final stage
FROM python:3.10-slim-bookworm

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Nextflow
RUN curl -s https://get.nextflow.io | bash \
    && mv nextflow /usr/local/bin/ \
    && chmod +x /usr/local/bin/nextflow

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV LC_ALL=C

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy project files
COPY . /app/

# Set Python path to include src
ENV PYTHONPATH=/app/src

# Default command
CMD ["fairway", "--help"]
'''

MAKEFILE_TEMPLATE = r'''# Fairway Project Makefile
.PHONY: all run run-hpc clean test shell

# Default: Run locally
all: run

# Run the pipeline (wraps nextflow)
run: ## Run the pipeline locally
	fairway run

# Run on HPC (wraps sbatch submission)
run-hpc: ## Run on Slurm with Spark
	fairway run --profile slurm --with-spark

# Enter a shell inside the container (useful for debugging)
shell: ## Enter a shell inside the ecosystem
	fairway shell

# Build the container (Apptainer/Docker)
build: ## Build the container image
	fairway build

# Clean up artifacts
clean: ## Clean logs and temp data
	rm -rf logs/ .nextflow* work/
'''

CONFIG_TEMPLATE = """dataset_name: "{name}"
engine: "{engine_type}"
storage:
  raw_dir: "data/raw"
  intermediate_dir: "data/intermediate"
  final_dir: "data/final"

sources:
  - name: "example_source"
    path: "data/raw/example.csv"
    format: "csv"
    schema:
      id: "BIGINT"
      value: "DOUBLE"

validations:
  level1:
    min_rows: 1

enrichment:
  geocode: false
"""

SPARK_YAML_TEMPLATE = """# Spark cluster configuration
# Override these values for your HPC environment

nodes: 2
cpus_per_node: 32
mem_per_node: "200G"

# Slurm-specific
account: "borzekowski"
partition: "day"
time: "24:00:00"

# Spark dynamic allocation
dynamic_allocation:
  enabled: true
  min_executors: 5
  max_executors: 150
  initial_executors: 15
"""

REQS_TEMPLATE = "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[{extra}]\n"

TRANSFORM_TEMPLATE = """
def example_transform(df):
    \"\"\"
    An example transformation function.
    Args:
        df: Input DataFrame (pandas or spark)
    Returns:
        Transformed DataFrame
    \"\"\"
    # Example logic
    return df
"""

README_TEMPLATE = """# {name}

Initialized by fairway on {timestamp}

**Engine**: {engine}

## Quick Start

### 1. Generate Test Data

```bash
fairway generate-data --size small --partitioned
```

### 2. Generate Schema (optional)

```bash
fairway generate-schema data/raw/your_data.csv
```

### 3. Update Configuration

Edit `config/fairway.yaml` to define your data sources, validations, and enrichments.

### 4. Run the Pipeline

**Local execution:**
```bash
make run
```

**Slurm cluster:**
```bash
make run-hpc
```

**Containerized execution:**
```bash
fairway run --config config/fairway.yaml --profile apptainer
```
*Note: This pulls the default container. To customize the environment, run `fairway eject`.*

## Extending the Pipeline

To add a post-processing step (e.g., reshaping), edit `main.nf`. For example:

```groovy
process RESHAPE {{
    input:
    path "data/final/*"
 
    output:
    path "data/reshaped/*"
 
    script:
    \"\"\"
    python3 src/reshape.py ...
    \"\"\"
}}

workflow {{
    // ... existing ...
    RESHAPE(run_fairway.out)
}}
```

## Customization

To customize the Apptainer container or Dockerfile:
1. Run `fairway eject` to generate `Apptainer.def` and `Dockerfile`.
2. Edit the generated files.
3. Build locally (fairway will automatically favor local definitions).

## Project Structure

- `config/` - Pipeline configuration files
- `data/raw/` - Input data files
- `data/intermediate/` - Intermediate processing outputs
- `data/final/` - Final processed data
- `src/transformations/` - Custom transformation scripts
- `docs/` - Project documentation
- `logs/` - Execution logs
- `Makefile` - Convenience commands

## Documentation

See the [fairway documentation](https://github.com/DISSC-yale/fairway) for more details.
"""

DOCS_TEMPLATE = """# Getting Started with {name}

## Prerequisites

- Python 3.9+
- fairway installed (`pip install git+https://github.com/DISSC-yale/fairway.git`)

## Generating Test Data

Create sample data to test your pipeline:

```bash
# Small partitioned CSV dataset
fairway generate-data --size small --partitioned

# Large Parquet dataset
fairway generate-data --size large --format parquet
```

## Schema Inference

Auto-generate schema from your data:

```bash
# From a single file
fairway generate-schema data/raw/example.csv

# From a partitioned directory
fairway generate-schema data/raw/partitioned_data/
```

## Configuration

Your pipeline is configured in `config/fairway.yaml`:

```yaml
dataset_name: "{name}"
engine: "{engine_type}"

sources:
  - name: "my_source"
    path: "data/raw/my_data.csv"
    format: "csv"
    schema:
      id: "int"
      value: "double"

validations:
  level1:
    min_rows: 100
    check_column_count: true

enrichment:
  geocode: true
```

## Running Your Pipeline

### Local (DuckDB)
```bash
make run
```

### Slurm Cluster (PySpark)
```bash
make run-hpc
```

### Custom Slurm Resources
```bash
fairway run --config config/fairway.yaml --slurm \\
  --cpus 8 --mem 32G --time 04:00:00 --nodes 4
```
"""

SBATCH_TEMPLATE = """#!/bin/bash
#SBATCH --job-name=fairway_{job_name_suffix}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --time={time}
#SBATCH --account={account}
#SBATCH --partition={partition}
#SBATCH --output=logs/slurm/fairway_%j.log

module load Nextflow
# Pass the spark master if we have one
SPARK_URL_ARG=""
if [ ! -z "$SPARK_MASTER_URL" ]; then
    SPARK_URL_ARG="--spark_master $SPARK_MASTER_URL"
fi

# Pass through Apptainer binds calculated by the CLI
export APPTAINER_BIND="{apptainer_bind}"

nextflow run main.nf -profile {profile} \\
    --config {config} \\
    --batch_size {batch_size} \\
    --slurm_nodes {nodes} \\
    --slurm_cpus_per_task {cpus} \\
    --slurm_mem {mem} \\
    --slurm_time {time} \\
    --slurm_partition {partition} \\
    --account {account} \\
    $SPARK_URL_ARG
"""
