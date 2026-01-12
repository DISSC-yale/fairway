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

process run_fairway {
    publishDir "${params.outdir}", mode: 'copy'
    
    input:
    path config_file

    output:
    path "data/final/*"

    script:
    """
    fairway run --config ${config_file}
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
COPY pyproject.toml requirements.txt* ./
COPY src/ src/
# Create a minimal requirements.txt if it doesn't exist
RUN if [ ! -f requirements.txt ]; then echo "fairway" > requirements.txt; fi
RUN pip install --no-cache-dir -r requirements.txt

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
ENV PYTHONPATH=$PYTHONPATH:/app/src

# Default command
CMD ["fairway", "--help"]
'''
