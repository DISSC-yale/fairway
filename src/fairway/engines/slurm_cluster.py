import subprocess
import os
import time
import click

class SlurmSparkManager:
    """Manages Spark clusters on Slurm with support for Apptainer containers."""

    def __init__(self, config, driver_job_id=None):
        self.config = config
        self.driver_job_id = driver_job_id

        if driver_job_id:
            # Use job-specific directory to prevent race conditions between concurrent jobs
            state_dir = os.path.expanduser(f"~/.fairway-spark/{driver_job_id}")
            self.master_url_file = os.path.join(state_dir, "master_url.txt")
            self.job_id_file = os.path.join(state_dir, "cluster_job_id.txt")
            self.conf_dir_file = os.path.join(state_dir, "conf_dir.txt")
            self.cores_file = os.path.join(state_dir, "total_cores.txt")
        else:
            # Legacy flat paths for backwards compatibility
            self.master_url_file = os.path.expanduser("~/spark_master_url.txt")
            self.job_id_file = os.path.expanduser("~/cluster_job_id.txt")
            self.conf_dir_file = os.path.expanduser("~/spark_conf_dir.txt")
            self.cores_file = os.path.expanduser("~/totalexecutorcores.txt")

    def _detect_apptainer_mode(self):
        """Detect if we should use Apptainer mode.

        Returns tuple of (use_apptainer: bool, sif_path: str or None)
        """
        # Check config first
        use_apptainer = self.config.get('use_apptainer', None)
        sif_path = self.config.get('apptainer_sif', None)

        # If not specified in config, auto-detect
        if use_apptainer is None:
            # Check for local fairway.sif
            if os.path.exists("fairway.sif"):
                return True, os.path.abspath("fairway.sif")
            # Check for FAIRWAY_SIF env var
            env_sif = os.environ.get('FAIRWAY_SIF')
            if env_sif and os.path.exists(env_sif):
                return True, env_sif
            return False, None

        # Config specified explicitly
        if use_apptainer:
            if sif_path and os.path.exists(sif_path):
                return True, os.path.abspath(sif_path)
            elif os.path.exists("fairway.sif"):
                return True, os.path.abspath("fairway.sif")
            else:
                click.echo("WARNING: use_apptainer=true but no .sif file found. Falling back to bare-metal.")
                return False, None
        return False, None

    def _get_spark_start_script_path(self):
        """Get path to fairway-spark-start.sh, writing it if needed."""
        script_name = "fairway-spark-start.sh"
        script_path = os.path.join("scripts", script_name)

        if os.path.exists(script_path):
            return script_path

        # Write from template
        os.makedirs("scripts", exist_ok=True)
        from ..templates import SPARK_START_TEMPLATE
        with open(script_path, 'w') as f:
            f.write(SPARK_START_TEMPLATE)
        os.chmod(script_path, 0o755)
        return script_path

    def start_cluster(self):
        """Submit a Slurm job to start a Spark cluster.

        Supports two modes:
        - Apptainer mode: Spark runs inside containers (auto-detected or config)
        - Bare-metal mode: Spark runs via host modules (fallback)
        """
        click.echo("Provisioning Spark cluster on Slurm...")

        # Create job-specific state directory if using driver_job_id
        if self.driver_job_id:
            state_dir = os.path.dirname(self.master_url_file)
            os.makedirs(state_dir, exist_ok=True)

        # Remove stale files from any previous cluster (in this job's directory)
        for stale in [self.master_url_file, self.job_id_file, self.conf_dir_file, self.cores_file]:
            if os.path.exists(stale):
                os.remove(stale)

        # Detect mode
        use_apptainer, sif_path = self._detect_apptainer_mode()
        if use_apptainer:
            click.echo(f"Using Apptainer mode with: {sif_path}")
        else:
            click.echo("Using bare-metal mode (host Spark module)")

        # Resource configuration from spark.yaml
        nodes = self.config.get('slurm_nodes')
        cpus = self.config.get('slurm_cpus_per_node')
        mem = self.config.get('slurm_mem_per_node')
        account = self.config.get('slurm_account')
        time_limit = self.config.get('slurm_time')
        partition = self.config.get('slurm_partition')

        # Build dynamic allocation settings
        dynamic_alloc = self.config.get('dynamic_allocation', {})
        da_lines = []
        if dynamic_alloc.get('enabled') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.enabled {dynamic_alloc["enabled"]}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('min_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.minExecutors {dynamic_alloc["min_executors"]}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('max_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.maxExecutors {dynamic_alloc["max_executors"]}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('initial_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.initialExecutors {dynamic_alloc["initial_executors"]}" >> $DEFAULTS_FILE')
        dynamic_alloc_script = "\n".join(da_lines)

        # Build spark_conf settings
        spark_conf = self.config.get('spark_conf', {})
        spark_conf_lines = "\n".join(
            f'echo "{key} {value}" >> $DEFAULTS_FILE'
            for key, value in spark_conf.items()
        )

        # Get bind paths for Apptainer (empty default - auto-detected from storage config)
        bind_paths = self.config.get('apptainer_binds', '')

        if use_apptainer:
            sbatch_script = self._generate_apptainer_script(
                nodes, cpus, mem, account, time_limit, partition,
                sif_path, bind_paths, dynamic_alloc_script, spark_conf_lines
            )
        else:
            sbatch_script = self._generate_baremetal_script(
                nodes, cpus, mem, account, time_limit, partition,
                dynamic_alloc_script, spark_conf_lines
            )

        # Write and submit script
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        os.makedirs("logs/slurm", exist_ok=True)

        mode_suffix = "apptainer" if use_apptainer else "baremetal"
        script_path = f"logs/slurm/start_spark_cluster_{mode_suffix}_{timestamp}.sh"
        with open(script_path, "w") as f:
            f.write(sbatch_script)
        os.chmod(script_path, 0o755)

        click.echo(f"Generated Spark cluster script: {script_path}")

        result = subprocess.run(["sbatch", script_path], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to submit Spark cluster job: {result.stderr}")

        click.echo("Waiting for Spark Master URL...")
        while not os.path.exists(self.master_url_file):
            time.sleep(5)

        with open(self.master_url_file, "r") as f:
            master_url = f.read().strip()

        click.echo(f"Spark cluster ready at: {master_url}")
        return master_url

    def _generate_apptainer_script(self, nodes, cpus, mem, account, time_limit, partition,
                                    sif_path, bind_paths, dynamic_alloc_script, spark_conf_lines):
        """Generate sbatch script for Apptainer mode."""

        # Ensure fairway-spark-start.sh exists
        spark_start_script = self._get_spark_start_script_path()

        return f"""#!/bin/bash
#SBATCH --job-name=fairway-spark
#SBATCH --output=logs/slurm/spark_cluster_%j.log
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --account={account}
#SBATCH --time={time_limit}
#SBATCH --partition={partition}

# =============================================================================
# Fairway Spark Cluster - Apptainer Mode
# =============================================================================

set -e
mkdir -p logs/slurm

# Apptainer container configuration
# These are exported so fairway-spark-start.sh can detect container mode
# and wrap Spark components (master, workers) in apptainer exec
export FAIRWAY_SIF="{sif_path}"
export FAIRWAY_BINDS="{bind_paths}"

echo "Starting Spark cluster in Apptainer mode"
echo "  Container: $FAIRWAY_SIF"
echo "  Bind paths: $FAIRWAY_BINDS"

# Run fairway-spark-start.sh on the HOST (not inside container)
# This is critical: the script uses srun which is only available on the host.
# The script itself handles containerization of Spark components (master, workers).
SPARK_START_LOG="spark_start_${{SLURM_JOB_ID}}.log"
bash {spark_start_script} > "${{SPARK_START_LOG}}" 2>&1

# Display log for debugging
cat "${{SPARK_START_LOG}}"

# spark-start sets SPARK_CONF_DIR to ~/.spark-local/$SLURM_JOB_ID/spark/conf
export SPARK_CONF_DIR="${{HOME}}/.spark-local/${{SLURM_JOB_ID}}/spark/conf"
DEFAULTS_FILE="${{SPARK_CONF_DIR}}/spark-defaults.conf"

# Append dynamic allocation settings from config/spark.yaml
{dynamic_alloc_script}
echo "spark.port.maxRetries 40" >> $DEFAULTS_FILE

# Append spark_conf settings from config/spark.yaml
{spark_conf_lines}

echo "DEBUG: SPARK_CONF_DIR=$SPARK_CONF_DIR"
echo "DEBUG: spark-defaults.conf contents:"
cat $DEFAULTS_FILE

# Extract SPARK_MASTER_URL from the log
SPARK_MASTER_URL=$(grep "SPARK_MASTER_URL:" "${{SPARK_START_LOG}}" | awk '{{print $2}}')

if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERROR: Could not find SPARK_MASTER_URL in output"
    exit 1
fi

echo "Detected Spark Master: $SPARK_MASTER_URL"

# Write state files (directory created by driver or here)
mkdir -p "$(dirname '{self.master_url_file}')"
echo "${{SPARK_MASTER_URL}}" > {self.master_url_file}
echo "${{SLURM_JOB_ID}}" > {self.job_id_file}
echo "${{SPARK_CONF_DIR}}" > {self.conf_dir_file}
echo "$((SLURM_CPUS_ON_NODE - 1))" > {self.cores_file}

sleep infinity
"""

    def _generate_baremetal_script(self, nodes, cpus, mem, account, time_limit, partition,
                                    dynamic_alloc_script, spark_conf_lines):
        """Generate sbatch script for bare-metal mode (host Spark module)."""
        return f"""#!/bin/bash
#SBATCH --job-name=fairway-spark
#SBATCH --output=logs/slurm/spark_cluster_%j.log
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --account={account}
#SBATCH --time={time_limit}
#SBATCH --partition={partition}

# =============================================================================
# Fairway Spark Cluster - Bare-Metal Mode
# =============================================================================

set -e
mkdir -p logs/slurm

echo "Starting Spark cluster in bare-metal mode (host module)"

# Load Spark module from host
module load Spark/3.5.1-foss-2022b-Scala-2.13

# Let spark-start handle cluster setup (creates spark-defaults.conf with auth secret)
SPARK_START_LOG="spark_start_${{SLURM_JOB_ID}}.log"
spark-start > "${{SPARK_START_LOG}}" 2>&1

# Display log for debugging
cat "${{SPARK_START_LOG}}"

# spark-start sets SPARK_CONF_DIR to ~/.spark-local/$SLURM_JOB_ID/spark/conf
export SPARK_CONF_DIR="${{HOME}}/.spark-local/${{SLURM_JOB_ID}}/spark/conf"
DEFAULTS_FILE="${{SPARK_CONF_DIR}}/spark-defaults.conf"

# Append dynamic allocation settings from config/spark.yaml
{dynamic_alloc_script}
echo "spark.port.maxRetries 40" >> $DEFAULTS_FILE

# Append spark_conf settings from config/spark.yaml
{spark_conf_lines}

echo "DEBUG: SPARK_CONF_DIR=$SPARK_CONF_DIR"
echo "DEBUG: spark-defaults.conf contents:"
cat $DEFAULTS_FILE

# Extract SPARK_MASTER_URL from the log
SPARK_MASTER_URL=$(grep "SPARK_MASTER_URL:" "${{SPARK_START_LOG}}" | awk '{{print $2}}')

if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERROR: Could not find SPARK_MASTER_URL in output"
    exit 1
fi

echo "Detected Spark Master: $SPARK_MASTER_URL"

# Write state files (directory created by driver or here)
mkdir -p "$(dirname '{self.master_url_file}')"
echo "${{SPARK_MASTER_URL}}" > {self.master_url_file}
echo "${{SLURM_JOB_ID}}" > {self.job_id_file}
echo "${{SPARK_CONF_DIR}}" > {self.conf_dir_file}
echo "$((SLURM_CPUS_ON_NODE - 1))" > {self.cores_file}

sleep infinity
"""

    def stop_cluster(self):
        """Cancel the Slurm job for the Spark cluster."""
        if os.path.exists(self.job_id_file):
            with open(self.job_id_file, "r") as f:
                job_id = f.read().strip()
            click.echo(f"Stopping Spark cluster (Slurm Job ID: {job_id})...")
            subprocess.run(["scancel", job_id])
            for stale in [self.master_url_file, self.job_id_file, self.conf_dir_file]:
                if os.path.exists(stale):
                    os.remove(stale)
        else:
            click.echo("No active Spark cluster found to stop.")
