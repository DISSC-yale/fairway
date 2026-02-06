import subprocess
import os
import time
import click

class SlurmSparkManager:
    def __init__(self, config):
        self.config = config
        self.master_url_file = os.path.expanduser("~/spark_master_url.txt")
        self.job_id_file = os.path.expanduser("~/cluster_job_id.txt")
        self.conf_dir_file = os.path.expanduser("~/spark_conf_dir.txt")
        self.cores_file = os.path.expanduser("~/totalexecutorcores.txt")

    def start_cluster(self):
        """Submit a Slurm job to start a Spark cluster."""
        click.echo("Provisioning Spark cluster on Slurm...")

        # Remove stale files from any previous cluster so we don't read old values
        for stale in [self.master_url_file, self.job_id_file, self.conf_dir_file, self.cores_file]:
            if os.path.exists(stale):
                os.remove(stale)
        
        # Determine resource requirements from config (from spark.yaml)
        nodes = self.config.get('slurm_nodes')
        cpus = self.config.get('slurm_cpus_per_node')
        mem = self.config.get('slurm_mem_per_node')
        account = self.config.get('slurm_account')
        time_limit = self.config.get('slurm_time')
        partition = self.config.get('slurm_partition')

        # Build dynamic allocation settings from config/spark.yaml
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

        # Build spark_conf settings from config/spark.yaml
        spark_conf = self.config.get('spark_conf', {})
        spark_conf_lines = "\n".join(
            f'echo "{key} {value}" >> $DEFAULTS_FILE'
            for key, value in spark_conf.items()
        )

        sbatch_script = f"""#!/bin/bash
#SBATCH --job-name=fairway-spark
#SBATCH --output=logs/slurm/spark_cluster_%j.log
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --account={account}
#SBATCH --time={time_limit}
#SBATCH --partition={partition}

# Ensure log directory exists
mkdir -p logs/slurm

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
# Expecting line: SPARK_MASTER_URL: spark://...
SPARK_MASTER_URL=$(grep "SPARK_MASTER_URL:" "${{SPARK_START_LOG}}" | awk '{{print $2}}')

if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERROR: Could not find SPARK_MASTER_URL in output"
    exit 1
fi

echo "Detected Spark Master: $SPARK_MASTER_URL"
echo "${{SPARK_MASTER_URL}}" > {self.master_url_file}
echo "${{SLURM_JOB_ID}}" > {self.job_id_file}
echo "${{SPARK_CONF_DIR}}" > {self.conf_dir_file}
echo "$((SLURM_CPUS_ON_NODE - 1))" > {self.cores_file}

sleep infinity
"""
        # Use timestamped script path for visibility
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Ensure log dir exists
        os.makedirs("logs/slurm", exist_ok=True)
        
        script_path = f"logs/slurm/start_spark_cluster_{timestamp}.sh"
        with open(script_path, "w") as f:
            f.write(sbatch_script)
            
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

    def stop_cluster(self):
        """Cancel the Slurm job for the Spark cluster."""
        if os.path.exists(self.job_id_file):
            with open(self.job_id_file, "r") as f:
                job_id = f.read().strip()
            click.echo(f"Stopping Spark cluster (Slurm Job ID: {job_id})...")
            subprocess.run(["scancel", job_id])
            for f in [self.master_url_file, self.job_id_file, self.conf_dir_file]:
                if os.path.exists(f):
                    os.remove(f)
        else:
            click.echo("No active Spark cluster found to stop.")
