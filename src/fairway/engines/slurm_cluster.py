import subprocess
import os
import time
import click

class SlurmSparkManager:
    def __init__(self, config):
        self.config = config
        self.master_url_file = os.path.expanduser("~/spark_master_url.txt")
        self.job_id_file = os.path.expanduser("~/cluster_job_id.txt")
        self.cores_file = os.path.expanduser("~/totalexecutorcores.txt")

    def start_cluster(self):
        """Submit a Slurm job to start a Spark cluster."""
        click.echo("Provisioning Spark cluster on Slurm...")
        
        # Determine resource requirements from config or defaults
        nodes = self.config.get('slurm_nodes', 2)
        cpus = self.config.get('slurm_cpus_per_node', 32)
        mem = self.config.get('slurm_mem_per_node', '200G')
        account = self.config.get('slurm_account', 'borzekowski')
        time_limit = self.config.get('slurm_time', '24:00:00')
        partition = self.config.get('slurm_partition', 'day')
        
        sbatch_script = f"""#!/bin/bash
#SBATCH --job-name=fairway-spark
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --account={account}
#SBATCH --time={time_limit}
#SBATCH --partition={partition}

module load Spark/3.5.1-foss-2022b-Scala-2.13


# Capture spark-start output to find the Master URL
SPARK_START_LOG="spark_start_${{SLURM_JOB_ID}}.log"
spark-start > "${{SPARK_START_LOG}}" 2>&1

# Display log for debugging
cat "${{SPARK_START_LOG}}"

# Extract SPARK_MASTER_URL from the log
# Expecting line: SPARK_MASTER_URL: spark://...
SPARK_MASTER_URL=$(grep "SPARK_MASTER_URL:" "${{SPARK_START_LOG}}" | awk '{{print $2}}')

if [ -z "$SPARK_MASTER_URL" ]; then
    echo "ERROR: Could not find SPARK_MASTER_URL in output"
    exit 1
fi

echo "Detected Spark Master: $SPARK_MASTER_URL"

        # --- Spark Tuning Parity (data_l2) ---
        # Discover the ephemeral config path
        SPARK_CONF_DIR="${{HOME}}/.spark-local/${{SLURM_JOB_ID}}/spark/conf"
        DEFAULTS_FILE="${{SPARK_CONF_DIR}}/spark-defaults.conf"

        # Inject tuning parameters
        echo "spark.dynamicAllocation.enabled True" >> $DEFAULTS_FILE
        echo "spark.dynamicAllocation.minExecutors 5" >> $DEFAULTS_FILE
        echo "spark.dynamicAllocation.maxExecutors 150" >> $DEFAULTS_FILE
        echo "spark.dynamicAllocation.initialExecutors 15" >> $DEFAULTS_FILE
        echo "spark.port.maxRetries 40" >> $DEFAULTS_FILE
        # --- End Tuning ---
echo "${{SPARK_MASTER_URL}}" > {self.master_url_file}
echo "${{SLURM_JOB_ID}}" > {self.job_id_file}
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
            os.remove(self.master_url_file)
            os.remove(self.job_id_file)
        else:
            click.echo("No active Spark cluster found to stop.")
