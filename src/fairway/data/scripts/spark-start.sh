#!/bin/bash
# =============================================================================
# Reference copy of YCRC's spark-start script
# Source: /vast/palmer/apps/avx2/software/Spark/3.5.4-foss-2022b-Scala-2.13/bin/spark-start
# =============================================================================
# This script is called inside Slurm jobs to provision a standalone Spark cluster.
# Key behavior:
#   - Creates SPARK_CONF_DIR at ${HOME}/.spark-local/${SLURM_JOB_ID}/spark/conf
#   - Overwrites spark-defaults.conf with spark.authenticate=true and a random secret
#   - Starts master and workers using that config
# =============================================================================

################################################################################
# Test for Slurm environment variables
################################################################################

if [[ -z "${SLURM_JOB_ID}" || -z "${SLURM_CPUS_PER_TASK}" || -z "${SLURM_MEM_PER_NODE}" || -z "${SLURM_JOB_NUM_NODES}" ]]; then
  echo "ERROR: Some expected slurm environment variables are missing."
  echo "Note this script should only be called from a slurm script. Perhaps you are trying to run the spark-start script outside of a slurm job."
  echo ""
  echo "Env var values:"
  echo "  SLURM_JOB_ID=${SLURM_JOB_ID}"
  echo "  SLURM_CPUS_PER_TASK=${SLURM_CPUS_PER_TASK}"
  echo "  SLURM_MEM_PER_NODE=${SLURM_MEM_PER_NODE}"
  echo "  SLURM_JOB_NUM_NODES=${SLURM_JOB_NUM_NODES}"
  exit 1
fi

################################################################################
# Set up and launch standalone Spark cluster
################################################################################

export SPARK_DAEMON_CORES=1
export SPARK_DAEMON_MEMORY=1
export SPARK_DRIVER_CORES=2
export SPARK_DRIVER_MEMORY=5
export SPARK_OVERHEAD_CORES=${SPARK_DRIVER_CORES}
export SPARK_OVERHEAD_MEMORY=${SPARK_DRIVER_MEMORY}

export SCRATCH="$(mktemp -d ${USER}.XXXXXXXXXX -p /tmp)"
echo "Writing spark tmp files to slurm compute nodes: ${SCRATCH}"
srun --label --export=ALL mkdir -p -m 700 \
    "${SCRATCH}/local" \
    "${SCRATCH}/tmp"   \
    "${SCRATCH}/work"  \
    || fail "Could not set up node local directories"

export OOD_JOB_HOME="${HOME}/.spark-local/${SLURM_JOB_ID}"
echo "Writing spark job files to user's HOME: ${HOME}/.spark-local/${SLURM_JOB_ID}/spark"
mkdir -p "${OOD_JOB_HOME}/spark/conf" \
         "${OOD_JOB_HOME}/spark/pid" \
         "${OOD_JOB_HOME}/spark/logs" \
         || fail "Could not set up spark directories"

export SPARK_CONF_DIR="${OOD_JOB_HOME}/spark/conf"
export SPARK_LOG_DIR="${OOD_JOB_HOME}/spark/logs"
export SPARK_WORKER_DIR="${OOD_JOB_HOME}/spark/logs"
export SPARK_PID_DIR="${OOD_JOB_HOME}/spark/pid"
export SPARK_LOCAL_DIRS="${SCRATCH}/local"

SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK}
SPARK_WORKER_MEMORY=$(( SLURM_MEM_PER_NODE / 1024 ))

SPARK_SECRET=$(openssl rand -base64 32)
SPARK_WEBUI_SECRET=$(openssl rand -hex 32)
SPARK_UI_AUTH_TOKEN=${SPARK_WEBUI_SECRET}

touch ${SPARK_CONF_DIR}/spark-defaults.conf ${SPARK_CONF_DIR}/spark-env.sh
chmod 700 ${SPARK_CONF_DIR}/spark-defaults.conf ${SPARK_CONF_DIR}/spark-env.sh

# NOTE: This overwrites any existing spark-defaults.conf (cat >, not >>)
cat > "${SPARK_CONF_DIR}/spark-defaults.conf" <<EOF
# Enable RPC Authentication
spark.authenticate                 true
spark.authenticate.secret          ${SPARK_SECRET}

spark.ui.killEnabled               false
spark.edu.osc.spark.AuthFilter
spark.ui.enabled                   true
spark.ui.showConsoleProgress       false

spark.driver.extraJavaOptions     -Dhttp.proxyHost=proxy1.arc-ts.umich.edu -Dhttp.proxyPort=3128 -Dhttps.proxyHost=proxy1.arc-ts.umich.edu -Dhttps.proxyPort=3128
spark.executor.extraJavaOptions   -Dhttp.proxyHost=proxy1.arc-ts.umich.edu -Dhttp.proxyPort=3128 -Dhttps.proxyHost=proxy1.arc-ts.umich.edu -Dhttps.proxyPort=3128
EOF

cat > "${SPARK_CONF_DIR}/spark-env.sh" <<EOF
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS}
export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY}g
export SPARK_LOG_DIR=${SPARK_LOG_DIR}
export SPARK_PID_DIR=${SPARK_PID_DIR}
export SPARK_CONF_DIR=${SPARK_CONF_DIR}
export SCRATCH=${SCRATCH}
EOF

if [ ${SPARK_WORKER_CORES} -ge ${SPARK_OVERHEAD_CORES} ] && [ ${SPARK_WORKER_MEMORY} -ge ${SPARK_OVERHEAD_MEMORY} ]; then
    SPARK_WORKER_CORES_REMAINING=$(( SPARK_WORKER_CORES - SPARK_OVERHEAD_CORES ))
    SPARK_WORKER_MEMORY_REMAINING=$(( SPARK_WORKER_MEMORY - SPARK_OVERHEAD_MEMORY ))
else
    echo "Error: The slurm node running the spark driver does not have enough resources."
    exit 1
fi

SPARK_CLUSTER_CORES=$(( SPARK_WORKER_CORES_REMAINING + (SLURM_JOB_NUM_NODES - 1) * SPARK_WORKER_CORES ))
SPARK_CLUSTER_MEMORY=$(( SPARK_WORKER_MEMORY_REMAINING + (SLURM_JOB_NUM_NODES - 1) * SPARK_WORKER_MEMORY ))
echo "Spark cluster total capacity available for executors:"
echo "  - ${SPARK_CLUSTER_CORES} cores"
echo "  - ${SPARK_CLUSTER_MEMORY}G memory"
echo "export SPARK_CLUSTER_CORES=${SPARK_CLUSTER_CORES}" >> ${SPARK_CONF_DIR}/spark-env.sh
echo "export SPARK_CLUSTER_MEMORY=${SPARK_CLUSTER_MEMORY}" >> ${SPARK_CONF_DIR}/spark-env.sh

START_OUTPUT=$(${SPARK_HOME}/sbin/start-master.sh)
if [ $? -ne 0 ]; then
    echo "Error: Spark master did not start."
    echo "Output of the start script was: ${START_OUTPUT}"
    exit 1
fi
echo "Spark master start script finished."
SPARK_MASTER_LOG=$(ls -1 ${SPARK_LOG_DIR}/*master*.out | head -n 1)
echo "SPARK_MASTER_LOG: ${SPARK_MASTER_LOG}"

LOOP_COUNT=0
while ! grep -q "started at http://" ${SPARK_MASTER_LOG}; do
    echo -n ".."
    sleep 2
    if [ ${LOOP_COUNT} -gt 300 ]; then
        echo "Error: Spark Master did not start. Web UI address not found in master log."
        exit 1
    fi
    LOOP_COUNT=$(( LOOP_COUNT + 1 ))
done

LOOP_COUNT=0
while ! grep -q "spark://" ${SPARK_MASTER_LOG}; do
    echo -n ".."
    sleep 2
    if [ ${LOOP_COUNT} -gt 300 ]; then
        echo "Error: Spark Master did not start. Spark URL not found in master log."
        exit 1
    fi
    LOOP_COUNT=$(( LOOP_COUNT + 1 ))
done

SPARK_MASTER_URL=$(grep -m 1 "spark://" ${SPARK_MASTER_LOG} | sed "s/^.*spark:\/\//spark:\/\//")
SPARK_MASTER_WEBUI=$(grep -i -m 1 "started at http://" ${SPARK_MASTER_LOG} | sed "s/^.*http:\/\//http:\/\//")
echo "export SPARK_MASTER_URL=${SPARK_MASTER_URL}" >> ${SPARK_CONF_DIR}/spark-env.sh
echo "export SPARK_MASTER_WEBUI=${SPARK_MASTER_WEBUI}" >> ${SPARK_CONF_DIR}/spark-env.sh
echo "SPARK_MASTER_URL: ${SPARK_MASTER_URL}"
echo "SPARK_MASTER_WEBUI: ${SPARK_MASTER_WEBUI}"

cat > ${SPARK_CONF_DIR}/sparkworker.sh <<EOF
#!/bin/bash
ulimit -u 16384 -n 16384
export SPARK_CONF_DIR=${SPARK_CONF_DIR}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}g
logf="${SPARK_LOG_DIR}/spark-worker-\$(hostname).out"
exec spark-class org.apache.spark.deploy.worker.Worker "${SPARK_MASTER_URL}" &> "\${logf}"
EOF

chmod +x ${SPARK_CONF_DIR}/sparkworker.sh
srun /usr/bin/cp ${SPARK_CONF_DIR}/sparkworker.sh "${SCRATCH}/sparkworker.sh" \
    || fail "Could not broadcast worker start script to nodes"
rm -f ${SPARK_CONF_DIR}/sparkworker.sh

sed -i "s/SPARK_WORKER_CORES=${SPARK_WORKER_CORES}/SPARK_WORKER_CORES=${SPARK_WORKER_CORES_REMAINING}/;
    s/SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}g/SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY_REMAINING}g/" \
    "${SCRATCH}/sparkworker.sh"

srun --label --export=ALL --wait=0 --cpus-per-task=${SLURM_CPUS_PER_TASK} "${SCRATCH}/sparkworker.sh" &
WORKERS_PID=$!
echo "WORKERS_PID=${WORKERS_PID}"
echo "export WORKERS_PID=${WORKERS_PID}" >> ${SPARK_CONF_DIR}/spark-env.sh
