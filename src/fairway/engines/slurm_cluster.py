import subprocess
import os
import re
import time
import click


def _sanitize_job_id(job_id: str) -> str:
    """Validate a SLURM job ID for safe use in file paths and shell scripts."""
    if not re.match(r'^[a-zA-Z0-9_\-]+$', str(job_id)):
        raise ValueError(f"Unsafe job ID rejected: {job_id!r}")
    return str(job_id)


def _sanitize_spark_conf_key(key: str) -> str:
    """Validate a spark conf key for safe embedding in a shell script.

    Spark conf keys are dot-separated identifiers like spark.executor.memory.
    Only allow [a-zA-Z0-9._-] to prevent command injection via key names.

    Raises ValueError for keys containing characters outside the allowed set.
    """
    if not re.match(r'^[a-zA-Z0-9._\-]+$', str(key)):
        raise ValueError(f"Unsafe spark_conf key rejected: {key!r}")
    return str(key)


def _sanitize_sbatch_value(value) -> str:
    """Sanitize a value for safe use in an #SBATCH directive."""
    s = str(value)
    if '\n' in s or '\r' in s:
        raise ValueError(f"Newline injection in SBATCH value rejected: {value!r}")
    # Also block shell-active chars that appear in SBATCH directives
    if re.search(r'[`$"\\;|&<>]', s):
        raise ValueError(f"Unsafe SBATCH value rejected: {value!r}")
    return s


def _sanitize_spark_conf_value(value: str) -> str:
    """Validate a spark conf value for safe embedding in a shell script.

    The value is embedded inside a double-quoted bash string, e.g.:
        echo "spark.key <value>" >> $DEFAULTS_FILE

    Inside double quotes, the shell only interprets: $, `, ", and \\.
    We also reject newlines and other control characters to prevent
    multi-line injection.  Everything else (*, #, ;, &, |, >, <, (, ),
    [, ]) is inert inside double quotes.

    Raises ValueError for values containing shell-unsafe characters.
    """
    s = str(value)
    # Reject any character that is dangerous inside a double-quoted shell string,
    # or any control character (including newlines and null bytes).
    if re.search(r'["\$`\\\x00-\x1f\x7f]', s):
        raise ValueError(
            f"Unsafe spark_conf value rejected (contains shell-unsafe characters): {value!r}"
        )
    return s


def _parse_mem_to_gb(mem_str):
    """Parse a SLURM memory string (e.g. '200G', '1024M') to gigabytes (int)."""
    if not mem_str:
        return None
    m = re.match(r'^(\d+)\s*([KMGT]?)$', str(mem_str).strip(), re.IGNORECASE)
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2).upper()
    if unit == 'T':
        return value * 1024
    if unit == 'G' or unit == '':
        return value
    if unit == 'M':
        return max(1, value // 1024)
    if unit == 'K':
        return max(1, value // (1024 * 1024))
    return value


def compute_executor_defaults(nodes, cpus_per_node, mem_per_node_str):
    """Derive Spark executor settings from SLURM resource allocation.

    Formula:
        - Reserve 1 CPU per node for OS/Spark daemons
        - executor.cores = 4 (balances parallelism vs JVM overhead)
        - executors_per_node = floor(usable_cpus / cores_per_executor)
        - Reserve 10% memory per node for OS overhead
        - mem_per_executor = floor(usable_mem / executors_per_node)
        - Split: ~83% executor.memory, ~17% memoryOverhead

    Returns:
        tuple of (spark_conf dict, max_executors int, detail_lines list[str])
        detail_lines are human-readable strings explaining the calculation.
    """
    mem_gb = _parse_mem_to_gb(mem_per_node_str)
    if not all([nodes, cpus_per_node, mem_gb]):
        return {}, None, ["Auto-compute skipped: missing SLURM resource values"]

    cores_per_executor = 4
    reserved_cpus = 1
    usable_cpus = cpus_per_node - reserved_cpus
    executors_per_node = usable_cpus // cores_per_executor

    if executors_per_node < 1:
        return {}, None, [f"Auto-compute skipped: only {usable_cpus} usable CPUs, need at least {cores_per_executor}"]

    os_reserve_fraction = 0.10
    usable_mem_gb = int(mem_gb * (1 - os_reserve_fraction))
    mem_per_executor_gb = usable_mem_gb // executors_per_node

    # Split: ~83% heap, ~17% overhead (safe for wide schemas)
    overhead_gb = max(1, mem_per_executor_gb // 6)
    heap_gb = mem_per_executor_gb - overhead_gb

    max_executors = executors_per_node * nodes

    conf = {
        'spark.executor.cores': str(cores_per_executor),
        'spark.executor.memory': f'{heap_gb}g',
        'spark.executor.memoryOverhead': f'{overhead_gb}g',
    }

    details = [
        f"SLURM allocation: {nodes} nodes x {cpus_per_node} CPUs x {mem_per_node_str}",
        f"  Reserved per node: {reserved_cpus} CPU for daemons, {os_reserve_fraction:.0%} memory for OS",
        f"  Usable per node: {usable_cpus} CPUs, {usable_mem_gb}G memory",
        f"  executor.cores={cores_per_executor} -> {executors_per_node} executors/node, {max_executors} max total",
        f"  {mem_per_executor_gb}G per executor -> {heap_gb}g heap + {overhead_gb}g overhead",
    ]

    return conf, max_executors, details


class SlurmSparkManager:
    """Manages Spark clusters on Slurm with support for Apptainer containers."""

    def __init__(self, config, driver_job_id=None):
        self.config = config
        if driver_job_id is not None:
            driver_job_id = _sanitize_job_id(driver_job_id)
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

        # Sanitize all SBATCH header values before interpolating into shell scripts
        if nodes is not None:
            nodes = _sanitize_sbatch_value(nodes)
        if cpus is not None:
            cpus = _sanitize_sbatch_value(cpus)
        if mem is not None:
            mem = _sanitize_sbatch_value(mem)
        if account is not None:
            account = _sanitize_sbatch_value(account)
        if time_limit is not None:
            time_limit = _sanitize_sbatch_value(time_limit)
        if partition is not None:
            partition = _sanitize_sbatch_value(partition)

        # Auto-compute executor defaults from SLURM resources
        computed_conf, computed_max, detail_lines = compute_executor_defaults(nodes, cpus, mem)
        if detail_lines:
            click.echo("Spark executor auto-compute:")
            for line in detail_lines:
                click.echo(f"  {line}")

        # Merge: user-specified spark_conf wins over computed defaults
        spark_conf = dict(computed_conf)
        user_conf = self.config.get('spark_conf', {})
        if user_conf:
            overridden = [k for k in user_conf if k in computed_conf]
            if overridden:
                click.echo(f"  User overrides: {', '.join(overridden)}")
            spark_conf.update(user_conf)

        # Build dynamic allocation settings (after auto-compute so max_executors can be filled)
        dynamic_alloc = self.config.get('dynamic_allocation', {})
        if computed_max and dynamic_alloc.get('max_executors') is None:
            dynamic_alloc = dict(dynamic_alloc)
            dynamic_alloc['max_executors'] = computed_max
            click.echo(f"  Auto-set dynamic_allocation.max_executors={computed_max}")

        da_lines = []
        if dynamic_alloc.get('enabled') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.enabled {bool(dynamic_alloc["enabled"])}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('min_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.minExecutors {int(dynamic_alloc["min_executors"])}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('max_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.maxExecutors {int(dynamic_alloc["max_executors"])}" >> $DEFAULTS_FILE')
        if dynamic_alloc.get('initial_executors') is not None:
            da_lines.append(f'echo "spark.dynamicAllocation.initialExecutors {int(dynamic_alloc["initial_executors"])}" >> $DEFAULTS_FILE')
        dynamic_alloc_script = "\n".join(da_lines)

        click.echo("Effective spark_conf:")
        for k, v in spark_conf.items():
            click.echo(f"  {k} = {v}")

        spark_conf_lines = "\n".join(
            f'echo "{_sanitize_spark_conf_key(key)} {_sanitize_spark_conf_value(value)}" >> $DEFAULTS_FILE'
            for key, value in spark_conf.items()
        )

        # Get bind paths for Apptainer (empty default - auto-detected from storage config)
        bind_paths = self.config.get('apptainer_binds', '')

        # Sanitize container path values used in the script body
        if sif_path is not None:
            sif_path = _sanitize_sbatch_value(sif_path)
        if bind_paths:
            bind_paths = _sanitize_sbatch_value(bind_paths)

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
