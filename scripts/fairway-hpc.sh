#!/bin/bash
# =============================================================================
# fairway-hpc.sh - HPC Module Management and Job Submission for Fairway
# =============================================================================
# 
# This script helps manage module loading and job submission for Fairway
# on HPC clusters running Slurm.
#
# Usage:
#   source fairway-hpc.sh setup         # Load required modules
#   source fairway-hpc.sh setup-spark   # Load modules including Spark
#   fairway-hpc.sh run [options]        # Submit a fairway job
#   fairway-hpc.sh status               # Check running fairway jobs
#   fairway-hpc.sh cancel [job_id]      # Cancel a fairway job
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

# Module names - adjust these to match your HPC's module system
MODULES_BASE=(
    "Nextflow"           # Workflow orchestration
    "miniconda"          # Python environment
)

MODULES_SPARK=(
    "Java/11.0.20"       # Required for Spark
    "Spark/3.5.0"        # Apache Spark
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
  setup           Load base modules (Nextflow, Python)
  setup-spark     Load all modules including Spark
  run             Submit a fairway pipeline job
  status          Show status of fairway jobs
  cancel <id>     Cancel a fairway job

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

Examples:
  # Load modules interactively
  source fairway-hpc.sh setup

  # Submit a job with defaults
  fairway-hpc.sh run --config config/mydata.yaml

  # Submit with custom resources
  fairway-hpc.sh run --nodes 4 --cpus 16 --mem 128G --with-spark

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
    echo "Base modules loaded. Activate your Python environment:"
    echo "  conda activate fairway"
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

# Activate conda environment (adjust name as needed)
source \$(conda info --base)/etc/profile.d/conda.sh
conda activate fairway || {
    echo "Warning: Could not activate conda environment 'fairway'"
    echo "Attempting to continue with base environment..."
}

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

echo ""
echo "========================================"
echo "Fairway Job Completed"
echo "Time: \$(date)"
echo "========================================"
SBATCH_EOF

    chmod +x "$SBATCH_SCRIPT"
    
    echo ""
    echo "Submitting job: $JOB_NAME"
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
        run)
            print_header
            check_fairway
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
