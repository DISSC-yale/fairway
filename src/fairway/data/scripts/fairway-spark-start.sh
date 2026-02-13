#!/bin/bash
# =============================================================================
# fairway-spark-start.sh - Spark Cluster Startup for Fairway
# =============================================================================
#
# This script provisions a standalone Spark cluster within Slurm jobs.
# It supports two modes:
#   1. Apptainer mode: Workers run inside containers (FAIRWAY_SIF is set)
#   2. Bare-metal mode: Workers run directly on nodes (FAIRWAY_SIF not set)
#
# Based on YCRC's spark-start script but adapted for fairway's container model.
#
# Environment Variables:
#   FAIRWAY_SIF       - Path to Apptainer .sif file (enables container mode)
#   FAIRWAY_BINDS     - Comma-separated bind paths for Apptainer (default: /vast)
#   SPARK_HOME        - Path to Spark installation (set by container or module)
#
# Usage:
#   # Bare-metal mode (requires module load Spark)
#   ./fairway-spark-start.sh
#
#   # Apptainer mode (runs inside container)
#   FAIRWAY_SIF=/path/to/fairway.sif ./fairway-spark-start.sh
#
# =============================================================================

set -e

# -----------------------------------------------------------------------------
# Validate Slurm Environment
# -----------------------------------------------------------------------------
if [[ -z "${SLURM_JOB_ID}" || -z "${SLURM_CPUS_PER_TASK}" || -z "${SLURM_MEM_PER_NODE}" || -z "${SLURM_JOB_NUM_NODES}" ]]; then
    echo "ERROR: Missing Slurm environment variables."
    echo "This script must run inside a Slurm job."
    echo ""
    echo "  SLURM_JOB_ID=${SLURM_JOB_ID}"
    echo "  SLURM_CPUS_PER_TASK=${SLURM_CPUS_PER_TASK}"
    echo "  SLURM_MEM_PER_NODE=${SLURM_MEM_PER_NODE}"
    echo "  SLURM_JOB_NUM_NODES=${SLURM_JOB_NUM_NODES}"
    exit 1
fi

# -----------------------------------------------------------------------------
# Determine Container Mode (must happen BEFORE SPARK_HOME validation)
# -----------------------------------------------------------------------------
USE_CONTAINER="no"
if [[ -n "${FAIRWAY_SIF}" ]]; then
    if [[ -f "${FAIRWAY_SIF}" ]]; then
        USE_CONTAINER="yes"
        echo "Container mode enabled: ${FAIRWAY_SIF}"
    else
        echo "WARNING: FAIRWAY_SIF set but file not found: ${FAIRWAY_SIF}"
        echo "Falling back to bare-metal mode."
    fi
else
    echo "Bare-metal mode (no FAIRWAY_SIF set)"
fi

# Default bind paths for Apptainer
FAIRWAY_BINDS="${FAIRWAY_BINDS:-/vast}"

# -----------------------------------------------------------------------------
# Validate Spark Installation
# -----------------------------------------------------------------------------
if [[ "${USE_CONTAINER}" == "yes" ]]; then
    # In container mode, Spark is inside the container at /opt/spark
    # We set SPARK_HOME here so commands reference the container path
    export SPARK_HOME=/opt/spark
    echo "Using SPARK_HOME: ${SPARK_HOME} (container path)"

    # Use --no-home to prevent classpath pollution from user's home directory
    # This avoids ClassFormatError caused by corrupted files in ~/.ivy2 or similar
    APPTAINER_BASE_OPTS="--no-home --bind ${FAIRWAY_BINDS},${PWD},/tmp"

    # Validate inside container
    if ! apptainer exec ${APPTAINER_BASE_OPTS} "${FAIRWAY_SIF}" test -x "${SPARK_HOME}/sbin/start-master.sh"; then
        echo "ERROR: Spark installation invalid inside container. ${SPARK_HOME}/sbin/start-master.sh not found."
        exit 1
    fi
    echo "Spark version: $(apptainer exec ${APPTAINER_BASE_OPTS} "${FAIRWAY_SIF}" ${SPARK_HOME}/bin/spark-submit --version 2>&1 | head -1)"
else
    # Bare-metal mode: SPARK_HOME must be set by module or environment
    if [[ -z "${SPARK_HOME}" ]]; then
        # Try common container location (in case running inside container directly)
        if [[ -d "/opt/spark" ]]; then
            export SPARK_HOME=/opt/spark
        else
            echo "ERROR: SPARK_HOME not set and /opt/spark not found."
            echo "Either set SPARK_HOME or run inside the fairway container."
            exit 1
        fi
    fi

    if [[ ! -x "${SPARK_HOME}/sbin/start-master.sh" ]]; then
        echo "ERROR: Spark installation invalid. ${SPARK_HOME}/sbin/start-master.sh not found."
        exit 1
    fi

    echo "Using SPARK_HOME: ${SPARK_HOME}"
    echo "Spark version: $(${SPARK_HOME}/bin/spark-submit --version 2>&1 | head -1)"
fi

# -----------------------------------------------------------------------------
# Resource Configuration
# -----------------------------------------------------------------------------
export SPARK_DAEMON_CORES=1
export SPARK_DAEMON_MEMORY=1
export SPARK_DRIVER_CORES=2
export SPARK_DRIVER_MEMORY=5
export SPARK_OVERHEAD_CORES=${SPARK_DRIVER_CORES}
export SPARK_OVERHEAD_MEMORY=${SPARK_DRIVER_MEMORY}

# -----------------------------------------------------------------------------
# Set Up Scratch and Config Directories
# -----------------------------------------------------------------------------
export SCRATCH="$(mktemp -d ${USER}.XXXXXXXXXX -p /tmp)"
echo "Spark scratch directory: ${SCRATCH}"

# Create scratch dirs on all nodes
srun --label --export=ALL mkdir -p -m 700 \
    "${SCRATCH}/local" \
    "${SCRATCH}/tmp" \
    "${SCRATCH}/work" \
    || { echo "ERROR: Could not create scratch directories"; exit 1; }

export OOD_JOB_HOME="${HOME}/.spark-local/${SLURM_JOB_ID}"
echo "Spark job home: ${OOD_JOB_HOME}"
mkdir -p "${OOD_JOB_HOME}/spark/conf" \
         "${OOD_JOB_HOME}/spark/pid" \
         "${OOD_JOB_HOME}/spark/logs" \
    || { echo "ERROR: Could not create Spark directories"; exit 1; }

export SPARK_CONF_DIR="${OOD_JOB_HOME}/spark/conf"
export SPARK_LOG_DIR="${OOD_JOB_HOME}/spark/logs"
export SPARK_WORKER_DIR="${OOD_JOB_HOME}/spark/logs"
export SPARK_PID_DIR="${OOD_JOB_HOME}/spark/pid"
export SPARK_LOCAL_DIRS="${SCRATCH}/local"

# -----------------------------------------------------------------------------
# Calculate Worker Resources
# -----------------------------------------------------------------------------
SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK}
SPARK_WORKER_MEMORY=$(( SLURM_MEM_PER_NODE / 1024 ))  # Convert MB to GB

if [[ ${SPARK_WORKER_CORES} -lt ${SPARK_OVERHEAD_CORES} ]] || [[ ${SPARK_WORKER_MEMORY} -lt ${SPARK_OVERHEAD_MEMORY} ]]; then
    echo "ERROR: Insufficient resources for Spark overhead."
    echo "  Required: ${SPARK_OVERHEAD_CORES} cores, ${SPARK_OVERHEAD_MEMORY}GB memory"
    echo "  Available: ${SPARK_WORKER_CORES} cores, ${SPARK_WORKER_MEMORY}GB memory"
    exit 1
fi

SPARK_WORKER_CORES_REMAINING=$(( SPARK_WORKER_CORES - SPARK_OVERHEAD_CORES ))
SPARK_WORKER_MEMORY_REMAINING=$(( SPARK_WORKER_MEMORY - SPARK_OVERHEAD_MEMORY ))

SPARK_CLUSTER_CORES=$(( SPARK_WORKER_CORES_REMAINING + (SLURM_JOB_NUM_NODES - 1) * SPARK_WORKER_CORES ))
SPARK_CLUSTER_MEMORY=$(( SPARK_WORKER_MEMORY_REMAINING + (SLURM_JOB_NUM_NODES - 1) * SPARK_WORKER_MEMORY ))

echo "Spark cluster capacity:"
echo "  - ${SPARK_CLUSTER_CORES} executor cores"
echo "  - ${SPARK_CLUSTER_MEMORY}GB executor memory"

# -----------------------------------------------------------------------------
# Generate Spark Configuration
# -----------------------------------------------------------------------------
SPARK_SECRET=$(openssl rand -base64 32)

touch "${SPARK_CONF_DIR}/spark-defaults.conf" "${SPARK_CONF_DIR}/spark-env.sh"
chmod 700 "${SPARK_CONF_DIR}/spark-defaults.conf" "${SPARK_CONF_DIR}/spark-env.sh"

cat > "${SPARK_CONF_DIR}/spark-defaults.conf" <<EOF
# Fairway Spark Configuration
# Generated by fairway-spark-start.sh

# Authentication
spark.authenticate                 true
spark.authenticate.secret          ${SPARK_SECRET}

# Disable REST submission server (incompatible with authentication in Spark 4.x)
spark.master.rest.enabled          false

# UI Settings
spark.ui.killEnabled               false
spark.ui.enabled                   true
spark.ui.showConsoleProgress       false
EOF

cat > "${SPARK_CONF_DIR}/spark-env.sh" <<EOF
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS}
export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY}g
export SPARK_LOG_DIR=${SPARK_LOG_DIR}
export SPARK_PID_DIR=${SPARK_PID_DIR}
export SPARK_CONF_DIR=${SPARK_CONF_DIR}
export SCRATCH=${SCRATCH}
export SPARK_CLUSTER_CORES=${SPARK_CLUSTER_CORES}
export SPARK_CLUSTER_MEMORY=${SPARK_CLUSTER_MEMORY}
EOF

# -----------------------------------------------------------------------------
# Start Spark Master
# -----------------------------------------------------------------------------
echo "Starting Spark Master..."

# In container mode, Spark is installed inside the container, so we must
# run start-master.sh inside apptainer. In bare-metal mode, run directly.
if [[ "${USE_CONTAINER}" == "yes" ]]; then
    echo "Starting master inside container..."
    # Use --no-home to prevent classpath pollution
    START_OUTPUT=$(apptainer exec --no-home \
        --bind "${FAIRWAY_BINDS},${HOME}/.spark-local,${SCRATCH},${PWD},/tmp" \
        --env SPARK_CONF_DIR="${SPARK_CONF_DIR}" \
        --env SPARK_LOG_DIR="${SPARK_LOG_DIR}" \
        --env SPARK_PID_DIR="${SPARK_PID_DIR}" \
        "${FAIRWAY_SIF}" \
        ${SPARK_HOME}/sbin/start-master.sh 2>&1)
else
    START_OUTPUT=$(${SPARK_HOME}/sbin/start-master.sh 2>&1)
fi

if [[ $? -ne 0 ]]; then
    echo "ERROR: Spark master failed to start."
    echo "${START_OUTPUT}"
    exit 1
fi

SPARK_MASTER_LOG=$(ls -1 ${SPARK_LOG_DIR}/*master*.out 2>/dev/null | head -n 1)
if [[ -z "${SPARK_MASTER_LOG}" ]]; then
    echo "ERROR: Could not find Spark master log file."
    exit 1
fi
echo "Spark master log: ${SPARK_MASTER_LOG}"

# Wait for master to start
echo -n "Waiting for master"
LOOP_COUNT=0
while ! grep -q "spark://" "${SPARK_MASTER_LOG}" 2>/dev/null; do
    echo -n "."
    sleep 2
    LOOP_COUNT=$(( LOOP_COUNT + 1 ))
    if [[ ${LOOP_COUNT} -gt 150 ]]; then
        echo ""
        echo "ERROR: Spark Master did not start within timeout."
        cat "${SPARK_MASTER_LOG}"
        exit 1
    fi
done
echo " ready!"

# Extract master URL
SPARK_MASTER_URL=$(grep -m 1 "spark://" "${SPARK_MASTER_LOG}" | sed "s/^.*spark:\/\//spark:\/\//")
SPARK_MASTER_WEBUI=$(grep -i -m 1 "started at http://" "${SPARK_MASTER_LOG}" | sed "s/^.*http:\/\//http:\/\//" || echo "")

echo "SPARK_MASTER_URL: ${SPARK_MASTER_URL}"
echo "SPARK_MASTER_WEBUI: ${SPARK_MASTER_WEBUI}"

echo "export SPARK_MASTER_URL=${SPARK_MASTER_URL}" >> "${SPARK_CONF_DIR}/spark-env.sh"
echo "export SPARK_MASTER_WEBUI=${SPARK_MASTER_WEBUI}" >> "${SPARK_CONF_DIR}/spark-env.sh"

# -----------------------------------------------------------------------------
# Generate Worker Script
# -----------------------------------------------------------------------------
# The worker script needs to run on each Slurm node.
# In container mode, we wrap the spark-class command in apptainer exec.

if [[ "${USE_CONTAINER}" == "yes" ]]; then
    # Container mode: workers run inside Apptainer
    # Use --no-home to prevent classpath pollution from user's home directory
    cat > "${SPARK_CONF_DIR}/sparkworker.sh" <<EOF
#!/bin/bash
ulimit -u 16384 -n 16384
export SPARK_CONF_DIR=${SPARK_CONF_DIR}
export SPARK_WORKER_CORES=\${SPARK_WORKER_CORES:-${SPARK_WORKER_CORES}}
export SPARK_WORKER_MEMORY=\${SPARK_WORKER_MEMORY:-${SPARK_WORKER_MEMORY}g}
logf="${SPARK_LOG_DIR}/spark-worker-\$(hostname).out"

# Run worker inside Apptainer container (--no-home prevents classpath pollution)
exec apptainer exec --no-home \
    --bind ${FAIRWAY_BINDS},${HOME}/.spark-local,${SCRATCH},/tmp \
    --env SPARK_CONF_DIR=${SPARK_CONF_DIR} \
    ${FAIRWAY_SIF} \
    spark-class org.apache.spark.deploy.worker.Worker "${SPARK_MASTER_URL}" &> "\${logf}"
EOF
else
    # Bare-metal mode: workers run directly
    cat > "${SPARK_CONF_DIR}/sparkworker.sh" <<EOF
#!/bin/bash
ulimit -u 16384 -n 16384
export SPARK_CONF_DIR=${SPARK_CONF_DIR}
export SPARK_WORKER_CORES=\${SPARK_WORKER_CORES:-${SPARK_WORKER_CORES}}
export SPARK_WORKER_MEMORY=\${SPARK_WORKER_MEMORY:-${SPARK_WORKER_MEMORY}g}
logf="${SPARK_LOG_DIR}/spark-worker-\$(hostname).out"
exec spark-class org.apache.spark.deploy.worker.Worker "${SPARK_MASTER_URL}" &> "\${logf}"
EOF
fi

chmod +x "${SPARK_CONF_DIR}/sparkworker.sh"

# -----------------------------------------------------------------------------
# Broadcast and Start Workers
# -----------------------------------------------------------------------------
echo "Broadcasting worker script to nodes..."
srun /usr/bin/cp "${SPARK_CONF_DIR}/sparkworker.sh" "${SCRATCH}/sparkworker.sh" \
    || { echo "ERROR: Could not broadcast worker script"; exit 1; }

# Adjust first node (driver node) to have reduced resources
sed -i "s/SPARK_WORKER_CORES=\${SPARK_WORKER_CORES:-${SPARK_WORKER_CORES}}/SPARK_WORKER_CORES=\${SPARK_WORKER_CORES:-${SPARK_WORKER_CORES_REMAINING}}/;
    s/SPARK_WORKER_MEMORY=\${SPARK_WORKER_MEMORY:-${SPARK_WORKER_MEMORY}g}/SPARK_WORKER_MEMORY=\${SPARK_WORKER_MEMORY:-${SPARK_WORKER_MEMORY_REMAINING}g}/" \
    "${SCRATCH}/sparkworker.sh"

echo "Starting workers on ${SLURM_JOB_NUM_NODES} nodes..."
srun --label --export=ALL --wait=0 --cpus-per-task=${SLURM_CPUS_PER_TASK} "${SCRATCH}/sparkworker.sh" &
WORKERS_PID=$!
echo "WORKERS_PID=${WORKERS_PID}"
echo "export WORKERS_PID=${WORKERS_PID}" >> "${SPARK_CONF_DIR}/spark-env.sh"

# Give workers time to register
sleep 5

echo ""
echo "============================================================"
echo "Spark cluster is ready!"
echo "  Master URL: ${SPARK_MASTER_URL}"
echo "  Web UI: ${SPARK_MASTER_WEBUI}"
echo "  Config Dir: ${SPARK_CONF_DIR}"
echo "  Container Mode: ${USE_CONTAINER}"
echo "============================================================"
