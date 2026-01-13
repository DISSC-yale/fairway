import click
import os
import shutil
import subprocess
import sys
from datetime import datetime
from .generate_test_data import generate_test_data


def discover_config():
    """Auto-discover config file in config/ folder.
    
    Returns the path to the config file if exactly one is found.
    Raises ClickException if zero or multiple configs exist.
    """
    config_dir = 'config'
    if not os.path.isdir(config_dir):
        raise click.ClickException("No config/ directory found. Run 'fairway init' first.")
    
    # Exclude schema files and spark.yaml
    configs = [f for f in os.listdir(config_dir) 
               if f.endswith(('.yaml', '.yml')) 
               and not f.endswith('_schema.yaml')
               and f != 'spark.yaml']
    
    if len(configs) == 0:
        raise click.ClickException("No config files found in config/")
    elif len(configs) == 1:
        return os.path.join(config_dir, configs[0])
    else:
        raise click.ClickException(
            f"Multiple config files found: {configs}. Use --config to specify one.")


def _get_apptainer_binds(cfg):
    """Calculate Apptainer bind paths from config."""
    bind_paths = set()
    
    # 1. Check storage directories
    if cfg.storage:
        for key in ['raw_dir', 'intermediate_dir', 'final_dir']:
            path = cfg.storage.get(key)
            if path:
                abs_path = os.path.abspath(path)
                if os.path.exists(abs_path):
                    bind_paths.add(abs_path)
    
    # 2. Check source paths
    if cfg.sources:
        for src in cfg.sources:
            path = src.get('path')
            if path:
                abs_path = os.path.abspath(path)
                if os.path.exists(abs_path):
                    bind_paths.add(abs_path)
            # Handle unexpanded path patterns if any (though config loader might have expanded them)
            # The config loader expands sources, so 'path' should be concrete file paths
            # But we might want to bind the parent directory of files to be safe/cleaner
            if os.path.isfile(abs_path):
                bind_paths.add(os.path.dirname(abs_path))
    
    return bind_paths


@click.group()
def main():
    """fairway: A portable data ingestion framework."""
    pass

@main.command()
@click.argument('name')
@click.option('--engine', type=click.Choice(['duckdb', 'spark']), required=True, help='Compute engine to use (duckdb or spark).')
def init(name, engine):
    """Initialize a new fairway project."""
    click.echo(f"Initializing new fairway project: {name} with engine: {engine}")
    
    directories = [
        'config',
        'data/raw',
        'data/intermediate',
        'data/final',
        'src/transformations',
        'docs',
        'logs/slurm',
        'logs/nextflow',
        'scripts'
    ]
    
    for d in directories:
        os.makedirs(os.path.join(name, d), exist_ok=True)
        click.echo(f"  Created directory: {d}")

    # Create config.yaml
    config_content = f"""dataset_name: "{name}"
engine: "{'pyspark' if engine == 'spark' else 'duckdb'}"
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
    with open(os.path.join(name, 'config', 'fairway.yaml'), 'w') as f:
        f.write(config_content)
    click.echo("  Created file: config/fairway.yaml")

    # Create spark.yaml with defaults
    spark_config_content = """# Spark cluster configuration
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
    with open(os.path.join(name, 'config', 'spark.yaml'), 'w') as f:
        f.write(spark_config_content)
    click.echo("  Created file: config/spark.yaml")

    # Create requirements.txt
    # Assuming installation from git source for now, as reflected in other parts of the CLI
    reqs_content = f"git+https://github.com/DISSC-yale/fairway.git#egg=fairway[{'spark' if engine == 'spark' else 'duckdb'}]\n"
    with open(os.path.join(name, 'requirements.txt'), 'w') as f:
        f.write(reqs_content)
    click.echo("  Created file: requirements.txt")

    # Write nextflow.config from template
    from .templates import NEXTFLOW_CONFIG, MAIN_NF, APPTAINER_DEF, DOCKERFILE_TEMPLATE, MAKEFILE_TEMPLATE
    with open(os.path.join(name, 'nextflow.config'), 'w') as f:
        f.write(NEXTFLOW_CONFIG)
    click.echo("  Created file: nextflow.config (customize profiles here)")
    
    # Write main.nf pipeline file from template
    with open(os.path.join(name, 'main.nf'), 'w') as f:
        f.write(MAIN_NF)
    click.echo("  Created file: main.nf (Nextflow pipeline)")

    # Write Makefile
    with open(os.path.join(name, 'Makefile'), 'w') as f:
        f.write(MAKEFILE_TEMPLATE)
    click.echo("  Created file: Makefile")
    
    from .templates import FAIRWAY_HPC_SH_TEMPLATE
    with open(os.path.join(name, 'scripts', 'fairway-hpc.sh'), 'w') as f:
        f.write(FAIRWAY_HPC_SH_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'fairway-hpc.sh'), 0o755)
    click.echo("  Created file: scripts/fairway-hpc.sh")

    # Create example transformation
    transform_content = """
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
    with open(os.path.join(name, 'src', 'transformations', 'example_transform.py'), 'w') as f:
        f.write(transform_content.strip())
    click.echo("  Created file: src/transformations/example_transform.py")

    # Create README.md with usage examples
    readme_content = f"""# {name}

Initialized by fairway on {datetime.now().isoformat()}

**Engine**: {engine}

## Quick Start
    
### 0. Load Environment (HPC Only)

On an HPC system, load the required modules first:
```bash
source scripts/fairway-hpc.sh setup
```

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
    with open(os.path.join(name, 'README.md'), 'w') as f:
        f.write(readme_content)
    click.echo("  Created file: README.md")

    # Create docs/getting-started.md
    docs_content = f"""# Getting Started with {name}

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
engine: "{'pyspark' if engine == 'spark' else 'duckdb'}"

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
    with open(os.path.join(name, 'docs', 'getting-started.md'), 'w') as f:
        f.write(docs_content)
    click.echo("  Created file: docs/getting-started.md")

    click.echo(f"Project {name} initialized successfully.")

@main.command()
@click.option('--size', type=click.Choice(['small', 'large']), default='small', help='Size of dataset to generate.')
@click.option('--partitioned/--no-partitioned', default=True, help='Generate partitioned data (year/month).')
@click.option('--format', type=click.Choice(['csv', 'parquet']), default='csv', help='Output format (csv or parquet).')
def generate_data(size, partitioned, format):
    """Generate mock test data."""
    click.echo(f"Generating {size} test data (partitioned={partitioned}, format={format})...")
    generate_test_data(size=size, partitioned=partitioned, file_format=format)

@main.command()
@click.argument('file_path')
@click.option('--output', help='Output file path for the schema (YAML).')
def generate_schema(file_path, output):
    """Generate a schema skeleton from a data file or partitioned directory."""
    import re
    import glob
    import duckdb
    import yaml
    
    if not os.path.exists(file_path):
        click.echo(f"Error: Path not found: {file_path}", err=True)
        return

    partition_columns = []
    sample_file = None
    dataset_name = os.path.basename(file_path.rstrip('/'))
    
    # Check if path is a directory (partitioned data)
    if os.path.isdir(file_path):
        click.echo(f"Detected partitioned directory: {file_path}")
        
        # Walk directory to extract partition columns and find a data file
        current_path = file_path
        while os.path.isdir(current_path):
            entries = os.listdir(current_path)
            dirs = [e for e in entries if os.path.isdir(os.path.join(current_path, e))]
            files = [e for e in entries if os.path.isfile(os.path.join(current_path, e)) 
                     and (e.endswith('.csv') or e.endswith('.parquet') or e.endswith('.json'))]
            
            # Check for Hive-style partition directories (key=value)
            partition_dirs = [d for d in dirs if '=' in d]
            
            if partition_dirs:
                # Extract partition column name from first partition dir
                partition_col = partition_dirs[0].split('=')[0]
                if partition_col not in partition_columns:
                    partition_columns.append(partition_col)
                # Navigate into first partition
                current_path = os.path.join(current_path, partition_dirs[0])
            elif files:
                # Found data files, pick first one as sample
                sample_file = os.path.join(current_path, files[0])
                break
            elif dirs:
                # Non-partition subdirectory, navigate in
                current_path = os.path.join(current_path, dirs[0])
            else:
                click.echo("Error: No data files found in partitioned directory", err=True)
                return
        
        if not sample_file:
            click.echo("Error: Could not find a sample data file", err=True)
            return
            
        click.echo(f"Detected partition columns: {partition_columns}")
        click.echo(f"Using sample file: {sample_file}")
    else:
        sample_file = file_path
    
    click.echo(f"Inferring schema from {sample_file}...")
    
    try:
        # Detect file format and read accordingly
        if sample_file.endswith('.parquet'):
            rel = duckdb.read_parquet(sample_file)
        elif sample_file.endswith('.json'):
            rel = duckdb.read_json(sample_file)
        else:
            rel = duckdb.read_csv(sample_file)
        
        columns = {}
        for col_name, col_type in zip(rel.columns, rel.types):
            # Skip partition columns since they're derived from directory structure
            if col_name not in partition_columns:
                columns[col_name] = str(col_type)

        # Build output schema
        schema_output = {'name': dataset_name}
        if partition_columns:
            schema_output['partition_by'] = partition_columns
        schema_output['columns'] = columns

        schema_yaml = yaml.dump(schema_output, sort_keys=False, default_flow_style=False)
        
        # Default output path: config/{dataset_name}_schema.yaml
        if not output:
            os.makedirs('config', exist_ok=True)
            output = f"config/{dataset_name}_schema.yaml"
        
        with open(output, 'w') as f:
            f.write(schema_yaml)
        click.echo(f"Schema written to {output}")
            
    except Exception as e:
        click.echo(f"Error inferring schema: {e}", err=True)

@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--profile', default='standard', help='Nextflow profile to use.')
@click.option('--slurm', is_flag=True, help='Run as a Slurm batch job (allocates a controller node).')
@click.option('--with-spark', is_flag=True, help='Automatically provision a Spark-on-Slurm cluster.')
@click.option('--slurm-cpus', 'cpus', default=None, type=int, help='CPUs per task/node (overrides spark.yaml).')
@click.option('--slurm-mem', 'mem', default=None, help='Memory per node (overrides spark.yaml).')
@click.option('--slurm-time', 'time', default=None, help='Time limit (overrides spark.yaml).')
@click.option('--slurm-nodes', 'nodes', default=None, type=int, help='Number of nodes (overrides spark.yaml).')
@click.option('--account', default=None, help='Slurm account (overrides spark.yaml).')
@click.option('--partition', default=None, help='Slurm partition (overrides spark.yaml).')
@click.option('--batch-size', default=30, help='Max parallel jobs.')
@click.option('--dry-run', is_flag=True, help='Generate script but do not submit.')
def run(config, profile, slurm, with_spark, cpus, mem, time, account, partition, nodes, batch_size, dry_run):
    """Run the fairway ingestion pipeline."""
    import yaml
    
    # Auto-discover config if not specified
    if config is None:
        config = discover_config()
        click.echo(f"Auto-discovered config: {config}")
    
    # Load spark.yaml for defaults
    spark_yaml_path = 'config/spark.yaml'
    spark_defaults = {}
    if os.path.exists(spark_yaml_path):
        with open(spark_yaml_path, 'r') as f:
            spark_defaults = yaml.safe_load(f) or {}
    
    # Merge: CLI args > spark.yaml > hardcoded defaults
    nodes = nodes or spark_defaults.get('nodes', 2)
    cpus = cpus or spark_defaults.get('cpus_per_node', 32)
    mem = mem or spark_defaults.get('mem_per_node', '200G')
    account = account or spark_defaults.get('account', 'borzekowski')
    partition = partition or spark_defaults.get('partition', 'day')
    time = time or spark_defaults.get('time', '24:00:00')
    
    # Load main config to check engine
    from .config_loader import Config
    cfg = Config(config)
    
    spark_manager = None
    master_url = None
    
    if profile == 'slurm' and (with_spark or cfg.engine == 'pyspark'):
        from .engines.slurm_cluster import SlurmSparkManager
        # Pass the CLI-provided resources to the spark manager
        spark_cfg = {
            'slurm_nodes': nodes,
            'slurm_cpus_per_node': cpus,
            'slurm_mem_per_node': mem,
            'slurm_account': account,
            'slurm_time': time,
            'slurm_partition': partition
        }
        spark_manager = SlurmSparkManager(spark_cfg)
        master_url = spark_manager.start_cluster()
        # Set environment variable for the pipeline to discover the master
        os.environ['SPARK_MASTER_URL'] = master_url

    # Calculate Apptainer bind paths from config
    bind_paths = _get_apptainer_binds(cfg)
    
    if bind_paths:
        bind_str = ','.join(sorted(list(bind_paths)))
        click.echo(f"Auto-binding paths for Apptainer: {bind_str}")
        
        # Merge with existing binds if any
        existing_binds = os.environ.get('APPTAINER_BIND', '')
        if existing_binds:
            os.environ['APPTAINER_BIND'] = f"{existing_binds},{bind_str}"
        else:
            os.environ['APPTAINER_BIND'] = bind_str

    try:
        if slurm:
            if not dry_run:
                click.echo("Submitting fairway pipeline as a Slurm batch job...")
            else:
                click.echo("Generating Slurm batch job script (Dry Run)...")
                
            # Pass dry_run to kwargs for use in the block below if needed, or just use local var
            kwargs = {'dry_run': dry_run}
            # Template for sbatch script (Controller node)
    # Template for sbatch script (Controller node)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_name_safe = os.path.basename(config).replace('.', '_')
            
            # Ensure log dir exists
            os.makedirs("logs/slurm", exist_ok=True)
            
            script_path = f"logs/slurm/fairway_{job_name_safe}_{timestamp}.sh"
            
            # Spark shutdown logic
            spark_cleanup = ""
            if master_url: # If using existing master
               pass # We don't shut it down as we didn't start it here
            elif spark_manager:
               # We are starting a cluster inside the job (if we refactor slurm_cluster to be called from within the job)
               # OR, if we started it here in CLI (current logic), we can't easily trap it in the batch job unless we pass the ID.
               # Current logic: CLI starts cluster -> gets URL -> submits batch job -> waits (?) -> stops cluster.
               # BUT: if --slurm is used, the CLI exits. Who stops the cluster?
               # FIX: The Batch Job itself should start/stop the cluster OR we accept that the user must cancel it.
               # Better FIX (as per plan): The generated batch script should trap EXIT and cancel the spark job.
               # However, the Spark Cluster is currently started by Python `subprocess` in CLI *before* sbatch.
               # This means the spark cluster job ID is known here `spark_manager.job_id_file`?
               # We need to get the job ID from spark_manager to inject into the cleanup script.
               
               # Let's inspect spark_manager implementation. It writes job_id to a file.
               # We can read it back or make spark_manager return it.
               # For now, let's assume we can get it.
               
               # UPDATE: Since we can't easily change `slurm_cluster.py`'s return signature without checking,
               # let's read the job_id_file if it exists.
               job_id_file = os.path.expanduser("~/cluster_job_id.txt")
               if os.path.exists(job_id_file):
                   with open(job_id_file, 'r') as f:
                       spark_cluster_id = f.read().strip()
                   spark_cleanup = f"""
# Clean up Spark cluster on exit
cleanup_spark() {{
    echo "Stopping Spark cluster (Job ID: {spark_cluster_id})..."
    scancel {spark_cluster_id}
}}
trap cleanup_spark EXIT
"""

            sbatch_content = f"""#!/bin/bash
#SBATCH --job-name=fairway_{job_name_safe}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --time={time}
#SBATCH --account={account}
#SBATCH --partition={partition}
#SBATCH --output=logs/slurm/fairway_{job_name_safe}_%j.log

{spark_cleanup}

module load Nextflow
# Pass the spark master if we have one
SPARK_URL_ARG=""
if [ ! -z "$SPARK_MASTER_URL" ]; then
    SPARK_URL_ARG="--spark_master $SPARK_MASTER_URL"
fi

# Pass through Apptainer binds calculated by the CLI
export APPTAINER_BIND="{os.environ.get('APPTAINER_BIND', '')}"

echo "Starting Fairway Pipeline..."
echo "Config: {config}"
echo "Profile: {profile}"
echo "Spark Master: $SPARK_MASTER_URL"

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

echo "Fairway Pipeline Completed."
"""
            with open(script_path, 'w') as f:
                f.write(sbatch_content)
            
            click.echo(f"Generated submission script: {script_path}")
            
            if not kwargs.get('dry_run'):
                subprocess.run(['sbatch', script_path], check=True)
            else:
                click.echo("Dry run: Skipping submission.")
        else:
            click.echo(f"Running fairway pipeline with config: {config}, profile: {profile}")
            
            # Check for nextflow
            nextflow_path = shutil.which('nextflow')
            
            cmd = [
                'run', 'main.nf', 
                '-profile', profile, 
                '--config', config,
                '--batch_size', str(batch_size),
                '--slurm_nodes', str(nodes),
                '--slurm_cpus_per_task', str(cpus),
                '--slurm_mem', mem,
                '--slurm_time', time,
                '--slurm_partition', partition
            ]
            if master_url:
                cmd.extend(['--spark_master', master_url])

            if nextflow_path:
                cmd.insert(0, 'nextflow')
                subprocess.run(cmd)
            else:
                # Nextflow not found, check for Apptainer
                apptainer_path = shutil.which('apptainer')
                if not apptainer_path:
                    raise click.ClickException(
                        "Nextflow not found on PATH. Apptainer also not found. "
                        "Please install Nextflow or Apptainer to run the pipeline."
                    )
                
                click.echo("Nextflow not found locally. Falling back to Apptainer execution...")
                
                # Determine image
                container_image = "fairway.sif" if os.path.exists("fairway.sif") else "docker://ghcr.io/dissc-yale/fairway:latest"
                
                # Construct Apptainer command
                apptainer_cmd = ['apptainer', 'exec']
                
                # Ensure we bind the current directory and any auto-discovered paths
                # Note: os.environ['APPTAINER_BIND'] might have been set above, but apptainer exec 
                # respects the env var, so we don't strictly need to pass --bind if the env var is set.
                # However, we MUST ensure the current working directory is bound so main.nf is visible.
                # Apptainer usually binds $PWD by default, but let's be safe if we want robustness.
                # Implicit binding of $PWD is standard in Apptainer.
                
                # If the env var was set by us earlier (line 531), it will be inherited by subprocess.run
                
                apptainer_cmd.append(container_image)
                apptainer_cmd.append('nextflow')
                apptainer_cmd.extend(cmd)
                
                result = subprocess.run(apptainer_cmd)
                if result.returncode != 0:
                    click.echo("", err=True)
                    click.echo("Error: Apptainer execution failed.", err=True)
                    click.echo("If the container image could not be pulled, try building it locally:", err=True)
                    click.echo("  1. fairway eject", err=True)
                    click.echo("  2. fairway build", err=True)
                    click.echo("  3. Run this command again.", err=True)
                    sys.exit(result.returncode)
    finally:
        if spark_manager and not slurm:
            # Only stop if we are running in the current terminal session
            # If we submitted via sbatch, the controller script should handle cleanup
            spark_manager.stop_cluster()

@main.command()
def eject():
    """Eject container definitions (Apptainer.def, Dockerfile) to the current directory."""
    if os.path.exists('Apptainer.def') or os.path.exists('Dockerfile'):
        if not click.confirm('Container files already exist. Overwrite?'):
            return

    from .templates import APPTAINER_DEF, DOCKERFILE_TEMPLATE
    
    # Write Apptainer.def from template
    with open('Apptainer.def', 'w') as f:
        f.write(APPTAINER_DEF)
    click.echo("  Created file: Apptainer.def (Apptainer container definition)")

    # Write Dockerfile from template
    with open('Dockerfile', 'w') as f:
        f.write(DOCKERFILE_TEMPLATE)
    click.echo("  Created file: Dockerfile (Docker container definition)")


@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--image', default=None, help='Path to Apptainer image (default: checks local fairway.sif then pulls from registry).')
@click.option('--bind', multiple=True, help='Additional bind paths.')
def shell(config, image, bind):
    """Enter an interactive shell inside the fairway container."""
    from .config_loader import Config

    # Auto-discover config if not specified
    if config is None:
        try:
            config = discover_config()
            click.echo(f"Auto-discovered config: {config}")
        except Exception:
            # It's okay if we don't find config for shell, but we won't auto-bind project paths
            config = None
            click.echo("No config file found. Proceeding without auto-binding project paths.")

    bind_paths = set(bind)
    
    if config:
        cfg = Config(config)
        auto_binds = _get_apptainer_binds(cfg)
        bind_paths.update(auto_binds)

    # Determine image
    if image:
        container_image = image
    elif os.path.exists("fairway.sif"):
        container_image = "fairway.sif"
    else:
        # Default to latest from registry
        # We need to import this constant or hardcode it
        # For now, let's look at the implementation plan
        container_image = "docker://ghcr.io/dissc-yale/fairway:latest"

    cmd = ["apptainer", "shell"]
    
    if bind_paths:
        bind_str = ','.join(sorted(list(bind_paths)))
        cmd.extend(["--bind", bind_str])
        click.echo(f"Binding paths: {bind_str}")
    
    cmd.append(container_image)
    
    click.echo(f"Launching shell in container: {container_image}")
    try:
        subprocess.run(cmd)
    except FileNotFoundError:
        click.echo("Error: 'apptainer' command not found. Is Apptainer installed?", err=True)


@main.command()
def status():
    """Show status of fairway jobs (wraps squeue)."""
    click.echo(f"Fairway jobs for user: {os.environ.get('USER', 'unknown')}")
    try:
        user = os.environ.get('USER')
        if not user:
            click.echo("Error: USER environment variable not set.", err=True)
            return

        # Using -o to format output similar to the script
        # %.10i %.20j %.8T %.10M %.6D %R
        subprocess.run([
            "squeue", 
            "-u", user, 
            "--name=fairway*", 
            "-o", "%.10i %.20j %.8T %.10M %.6D %R"
        ], check=True)
    except subprocess.CalledProcessError:
        click.echo("Error running squeue. Is Slurm available?", err=True)
    except FileNotFoundError:
        click.echo("squeue command not found.", err=True)

@main.command()
@click.argument('job_id')
def cancel(job_id):
    """Cancel a fairway job (wraps scancel)."""
    click.echo(f"Cancelling job: {job_id}")
    try:
        subprocess.run(["scancel", job_id], check=True)
        click.echo("Job cancelled.")
    except subprocess.CalledProcessError:
        click.echo(f"Error cancelling job {job_id}.", err=True)
    except FileNotFoundError:
        click.echo("scancel command not found.", err=True)

@main.command()
def build():
    """Build or pull the Apptainer container (fairway.sif)."""
    container_local = "fairway.sif"
    
    if os.path.exists(container_local):
        click.echo(f"Container already exists: {container_local}")
        click.echo("To rebuild, delete it first: rm fairway.sif")
        return

    click.echo("Building/pulling Fairway Apptainer image...")
    
    # Check for local definition first
    if os.path.exists("Apptainer.def"):
        click.echo("Building from local Apptainer.def...")
        cmd = ["apptainer", "build", container_local, "Apptainer.def"]
    else:
        # Pull from registry
        # Hardcoding registry URL for now (same as in script)
        container_image = "docker://ghcr.io/dissc-yale/fairway:latest"
        click.echo(f"Pulling from registry: {container_image}")
        cmd = ["apptainer", "pull", container_local, container_image]
    
    try:
        subprocess.run(cmd, check=True)
        click.echo(f"\nContainer built successfully: {container_local}")
        click.echo(f"Test with: apptainer exec {container_local} fairway --help")
    except subprocess.CalledProcessError:
        click.echo("\nError: Container build/pull failed.", err=True)
        click.echo("Possible reasons: Network issues, registry outage, or invalid definition.", err=True)
    except FileNotFoundError:
        click.echo("apptainer command not found. Is Apptainer installed?", err=True)


if __name__ == '__main__':
    main()

