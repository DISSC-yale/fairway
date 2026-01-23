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
        'scripts',
        'logs/slurm',
        'logs/nextflow',
        'scripts'
    ]
    
    for d in directories:
        os.makedirs(os.path.join(name, d), exist_ok=True)
        click.echo(f"  Created directory: {d}")

    # Create config.yaml
    engine_type = 'pyspark' if engine == 'spark' else 'duckdb'
    from .templates import NEXTFLOW_CONFIG, MAIN_NF, APPTAINER_DEF, DOCKERFILE_TEMPLATE, MAKEFILE_TEMPLATE, CONFIG_TEMPLATE, SPARK_YAML_TEMPLATE, TRANSFORM_TEMPLATE, README_TEMPLATE, DOCS_TEMPLATE
    config_content = CONFIG_TEMPLATE.format(name=name, engine_type=engine_type)
    
    with open(os.path.join(name, 'config', 'fairway.yaml'), 'w') as f:
        f.write(config_content)
    click.echo("  Created file: config/fairway.yaml")
    
    # Write .dockerignore
    from .templates import DOCKERIGNORE
    with open(os.path.join(name, '.dockerignore'), 'w') as f:
        f.write(DOCKERIGNORE)
    click.echo("  Created file: .dockerignore")

    # Create spark.yaml with defaults
    with open(os.path.join(name, 'config', 'spark.yaml'), 'w') as f:
        f.write(SPARK_YAML_TEMPLATE)
    click.echo("  Created file: config/spark.yaml")



    # Write nextflow.config from template
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
    


    # Create example transformation
    with open(os.path.join(name, 'src', 'transformations', 'example_transform.py'), 'w') as f:
        f.write(TRANSFORM_TEMPLATE.strip())
    click.echo("  Created file: src/transformations/example_transform.py")

    # Create README.md with usage examples

    readme_content = README_TEMPLATE.format(
        name=name,
        timestamp=datetime.now().isoformat(),
        engine=engine
    )
    with open(os.path.join(name, 'README.md'), 'w') as f:
        f.write(readme_content)
    click.echo("  Created file: README.md")

    # Create scripts/driver.sh and scripts/fairway-hpc.sh
    from .templates import DRIVER_TEMPLATE, HPC_SCRIPT, RUN_PIPELINE_SCRIPT
    with open(os.path.join(name, 'scripts', 'driver.sh'), 'w') as f:
        f.write(DRIVER_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'driver.sh'), 0o755)
    click.echo("  Created file: scripts/driver.sh")

    with open(os.path.join(name, 'scripts', 'run_pipeline.sh'), 'w') as f:
        f.write(RUN_PIPELINE_SCRIPT)
    os.chmod(os.path.join(name, 'scripts', 'run_pipeline.sh'), 0o755)
    click.echo("  Created file: scripts/run_pipeline.sh")

    with open(os.path.join(name, 'scripts', 'fairway-hpc.sh'), 'w') as f:
        f.write(HPC_SCRIPT)
    os.chmod(os.path.join(name, 'scripts', 'fairway-hpc.sh'), 0o755)
    click.echo("  Created file: scripts/fairway-hpc.sh")

    # Create docs/getting-started.md
    docs_content = DOCS_TEMPLATE.format(
        name=name,
        engine_type=engine_type
    )
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
@click.argument('file_path', required=False)
@click.option('--config', help='Path to fairway.yaml config (enables Pipeline Mode).')
@click.option('--output', help='Output file path for the schema (YAML).')
@click.option('--engine', type=click.Choice(['duckdb', 'pyspark']), default='duckdb', help='Engine (legacy mode only).')
@click.option('--sampling-ratio', type=float, default=1.0, help='Ratio of data to read (Spark only).')
# Slurm Options
@click.option('--slurm', is_flag=True, help='Submit as a Slurm job.')
@click.option('--account', help='Slurm account.')
@click.option('--time', help='Slurm time limit.')
@click.option('--cpus', type=int, help='Slurm CPUs.')
@click.option('--mem', help='Slurm Memory.')
@click.option('--internal-run', is_flag=True, hidden=True, help='Internal flag for Slurm execution.')
@click.option('--spark-master', hidden=True, help='Spark master URL (for internal run).')
def generate_schema(file_path, config, output, engine, sampling_ratio, slurm, account, time, cpus, mem, internal_run, spark_master):
    """Generate schema from data.
    
    Modes:
    1. Pipeline Mode: Provide --config. Uses full pipeline (preprocessing/unzipping) to find data.
    2. Legacy Mode: Provide FILE_PATH. Scans that specific file/dir.
    """
    import yaml
    import os
    import sys
    
    # ---------------------------------------------------------
    # SLURM SUBMISSION (Wrapper)
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # SLURM SUBMISSION (Distributed Cluster Mode)
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # SLURM SUBMISSION (Distributed Cluster Mode)
    # ---------------------------------------------------------
    if slurm and not internal_run:
        script_path = "scripts/driver-schema.sh"
        if not os.path.exists(script_path):
            click.echo(f"Error: {script_path} not found. Run 'fairway init' to regenerate scripts.", err=True)
            sys.exit(1)
            
        click.echo(f"Submitting Schema Generation Driver Job ({script_path})...")
        try:
            # We assume the user has configured spark.yaml or fairway-hpc.sh variables if needed
            # The driver script handles cluster provisioning.
            subprocess.run(["sbatch", script_path], check=True)
            click.echo("Job submitted. Check status with 'fairway status'.")
            return
        except subprocess.CalledProcessError as e:
             click.echo(f"Error submitting job: {e}", err=True)
             sys.exit(1)

    # ---------------------------------------------------------
    # PIPELINE MODE (Config Driven)
    # ---------------------------------------------------------
    if config:
        from .config_loader import Config
        from .schema_pipeline import SchemaDiscoveryPipeline
        
        click.echo(f"Starting Schema Discovery (Pipeline Mode) using {config}...")
        cfg = Config(config)
        
        # Initialize Pipeline
        pipeline = SchemaDiscoveryPipeline(config, spark_master=spark_master or "local[*]")
        # Note: If running inside Slurm (internal-run), spark_master should arguably be 
        # determined by environment, but for schema inference 'local[*]' on the 
        # compute node is usually sufficient and simpler than spinning up a full cluster context 
        # just for this. If full distribution is needed, we'd need the SlurmSparkManager logic here.
        
        pipeline.run_inference(output_path=output, sampling_ratio=sampling_ratio)
        return

    # ---------------------------------------------------------
    # LEGACY MODE (Single File)
    # ---------------------------------------------------------
    if not file_path:
        click.echo("Error: Must provide either FILE_PATH or --config.", err=True)
        sys.exit(1)

    # ... [Existing Legacy Logic for PySpark/DuckDB] ...
    # (Rest of the function follows, but pasted to ensure replacement)
    
    if engine == 'pyspark':
        click.echo(f"Initializing PySpark to infer schema from: {file_path}")
        try:
             from .engines.pyspark_engine import PySparkEngine
             spark_engine = PySparkEngine()
             
             fmt = 'parquet'
             if file_path.endswith('.csv') or '.csv' in file_path: fmt = 'csv'
             elif file_path.endswith('.json') or '.json' in file_path: fmt = 'json'
             
             if os.path.isdir(file_path):
                  entries = os.listdir(file_path)
                  if any(e.endswith('.csv') for e in entries): fmt = 'csv'
                  elif any(e.endswith('.json') for e in entries): fmt = 'json'
             
             schema_dict = spark_engine.infer_schema(
                 path=file_path, 
                 format=fmt,
                 sampling_ratio=sampling_ratio
             )
             
             dataset_name = os.path.basename(file_path.rstrip('/'))
             schema_output = {'name': dataset_name, 'columns': schema_dict}
             
             schema_yaml = yaml.dump(schema_output, sort_keys=False, default_flow_style=False)
             if not output:
                os.makedirs('config', exist_ok=True)
                output = f"config/{dataset_name}_schema.yaml"
             
             with open(output, 'w') as f:
                f.write(schema_yaml)
             click.echo(f"Schema written to {output}")
             return

        except Exception as e:
             click.echo(f"Error inferring schema with Spark: {e}", err=True)
             sys.exit(1)

    # DUCKDB PATH
    import duckdb

    if not os.path.exists(file_path):
        click.echo(f"Error: Path not found: {file_path}", err=True)
        return

    partition_columns = []
    sample_file = None
    dataset_name = os.path.basename(file_path.rstrip('/'))
    
    # Check if path is a directory (partitioned data)
    if os.path.isdir(file_path):
        click.echo(f"Detected partitioned directory: {file_path}")
        
        current_path = file_path
        while os.path.isdir(current_path):
            entries = os.listdir(current_path)
            dirs = [e for e in entries if os.path.isdir(os.path.join(current_path, e))]
            files = [e for e in entries if os.path.isfile(os.path.join(current_path, e)) 
                     and (e.endswith('.csv') or e.endswith('.parquet') or e.endswith('.json'))]
            
            partition_dirs = [d for d in dirs if '=' in d]
            
            if partition_dirs:
                partition_col = partition_dirs[0].split('=')[0]
                if partition_col not in partition_columns:
                    partition_columns.append(partition_col)
                current_path = os.path.join(current_path, partition_dirs[0])
            elif files:
                sample_file = os.path.join(current_path, files[0])
                break
            elif dirs:
                current_path = os.path.join(current_path, dirs[0])
            else:
                return
        
        if not sample_file:
            return
            
        click.echo(f"Detected partition columns: {partition_columns}")
        click.echo(f"Using sample file: {sample_file}")
    else:
        sample_file = file_path
    
    click.echo(f"Inferring schema from {sample_file}...")
    
    try:
        if sample_file.endswith('.parquet'):
            rel = duckdb.read_parquet(sample_file)
        elif sample_file.endswith('.json'):
            rel = duckdb.read_json(sample_file)
        else:
            rel = duckdb.read_csv(sample_file)
        
        columns = {}
        for col_name, col_type in zip(rel.columns, rel.types):
            if col_name not in partition_columns:
                columns[col_name] = str(col_type)

        schema_output = {'name': dataset_name}
        if partition_columns:
            schema_output['partition_by'] = partition_columns
        schema_output['columns'] = columns

        schema_yaml = yaml.dump(schema_output, sort_keys=False, default_flow_style=False)
        
        if not output:
            os.makedirs('config', exist_ok=True)
            output = f"config/{dataset_name}_schema.yaml"
        
        with open(output, 'w') as f:
            f.write(schema_yaml)
        click.echo(f"Schema written to {output}")
            
    except Exception as e:
        click.echo(f"Error inferring schema: {e}", err=True)

@main.group()
def spark():
    """Manage Spark clusters."""
    pass

@spark.command()
@click.option('--slurm-nodes', 'nodes', default=None, type=int, help='Number of worker nodes.')
@click.option('--slurm-cpus', 'cpus', default=None, type=int, help='CPUs per node.')
@click.option('--slurm-mem', 'mem', default=None, help='Memory per node.')
@click.option('--slurm-time', 'time', default=None, help='Time limit.')
@click.option('--account', default=None, help='Slurm account.')
@click.option('--partition', default=None, help='Slurm partition.')
def start(nodes, cpus, mem, time, account, partition):
    """Start a Spark cluster on Slurm."""
    import yaml
    
    # Load defaults from spark.yaml
    spark_yaml_path = 'config/spark.yaml'
    spark_defaults = {}
    if os.path.exists(spark_yaml_path):
        with open(spark_yaml_path, 'r') as f:
            spark_defaults = yaml.safe_load(f) or {}

    # effective resources
    nodes = nodes or spark_defaults.get('nodes', 2)
    cpus = cpus or spark_defaults.get('cpus_per_node', 32)
    mem = mem or spark_defaults.get('mem_per_node', '200G')
    account = account or spark_defaults.get('account', 'borzekowski')
    partition = partition or spark_defaults.get('partition', 'day')
    time = time or spark_defaults.get('time', '24:00:00')

    from .engines.slurm_cluster import SlurmSparkManager
    spark_cfg = {
        'slurm_nodes': nodes,
        'slurm_cpus_per_node': cpus,
        'slurm_mem_per_node': mem,
        'slurm_account': account,
        'slurm_time': time,
        'slurm_partition': partition
    }
    
    spark_manager = SlurmSparkManager(spark_cfg)
    spark_master = spark_manager.start_cluster()
    click.echo(f"Spark cluster started. Master URL: {spark_master}")

@spark.command()
def stop():
    """Stop the running Spark cluster."""
    from .engines.slurm_cluster import SlurmSparkManager
    spark_manager = SlurmSparkManager({})
    spark_manager.stop_cluster()

@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--spark-master', default=None, help='Spark master URL (e.g., spark://host:port or local[*]).')
def run(config, spark_master):
    """Run the ingestion pipeline (Worker Mode).
    
    This command executes the pipeline directly on the current machine.
    It does NOT launch Nextflow or submit Slurm jobs.
    """
    from .config_loader import Config
    from .pipeline import IngestionPipeline
    
    # Auto-discover config
    if config is None:
        config = discover_config()
        click.echo(f"Auto-discovered config: {config}")

    cfg = Config(config)
    
    click.echo(f"Starting pipeline execution using config: {config}")
    pipeline = IngestionPipeline(config, spark_master=spark_master)
    pipeline.run()
    click.echo("Pipeline execution completed successfully.")

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

    # Write .dockerignore from template
    from .templates import DOCKERIGNORE
    if not os.path.exists('.dockerignore') or click.confirm('.dockerignore already exists. Overwrite?'):
        with open('.dockerignore', 'w') as f:
            f.write(DOCKERIGNORE)
        click.echo("  Created file: .dockerignore")


@main.command()
@click.option('--force', is_flag=True, help='Force rebuild (overwrite existing image).')
def build(force):
    """Build the container image (Apptainer preferred, falls back to Docker)."""
    
    # Check for Apptainer.def
    if os.path.exists('Apptainer.def'):
        click.echo("Found Apptainer.def. Building Apptainer image...")
        
        if os.path.exists("fairway.sif"):
            if force:
                click.echo("Overwriting existing fairway.sif...")
                os.remove("fairway.sif")
            else:
                if not click.confirm("fairway.sif already exists. Overwrite?"):
                    return

        cmd = ["apptainer", "build", "fairway.sif", "Apptainer.def"]
        try:
            subprocess.run(cmd, check=True)
            click.echo("\nBuild complete: fairway.sif")
            click.echo("You can now run tasks with: fairway run --profile apptainer")
        except subprocess.CalledProcessError as e:
            raise click.ClickException(f"Apptainer build failed with exit code {e.returncode}")
        except FileNotFoundError:
             raise click.ClickException("Apptainer command not found.")
             
    elif os.path.exists("Dockerfile"):
        click.echo("Found Dockerfile. Building Docker image...")
        
        cmd = ["docker", "build", "-t", "fairway", "."]
        try:
            subprocess.run(cmd, check=True)
            click.echo("\nBuild complete: fairway:latest")
        except subprocess.CalledProcessError as e:
             raise click.ClickException(f"Docker build failed with exit code {e.returncode}")
        except FileNotFoundError:
             raise click.ClickException("Docker command not found.")
    else:
        raise click.ClickException(
            "No container definition found. "
            "Run 'fairway eject' to generate Apptainer.def and Dockerfile."
        )


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
@click.option('--user', default=None, help='Filter by user (default: current user).')
@click.option('--job-id', help='Filter by job ID.')
def status(user, job_id):
    """Show the status of submitted Slurm jobs (wrapper around squeue)."""
    # Check for active Spark cluster info
    master_path = os.path.expanduser("~/spark_master_url.txt")
    job_id_path = os.path.expanduser("~/cluster_job_id.txt")
    
    if os.path.exists(master_path) and os.path.exists(job_id_path):
        with open(master_path, 'r') as f:
            master_url = f.read().strip()
        with open(job_id_path, 'r') as f:
            cluster_job_id = f.read().strip()
        
        click.echo("Found active Spark cluster:")
        click.echo(f"  Slurm Job ID: {cluster_job_id}")
        click.echo(f"  Master URL:   {master_url}")
        click.echo("")

    cmd = ['squeue']
    
    if job_id:
        cmd.extend(['--jobs', job_id])
    else:
        # Default to current user if no user specified
        if not user:
            import getpass
            user = getpass.getuser()
        cmd.extend(['--user', user])
        
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        click.echo("Error: 'squeue' command not found. Are you on a system with Slurm?", err=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error checking status: {e}", err=True)


@main.command()
@click.argument('job_id', required=False)
@click.option('--all', 'kill_all', is_flag=True, help='Cancel all your running jobs.')
def cancel(job_id, kill_all):
    """Cancel a Slurm job (wrapper around scancel)."""
    if kill_all:
        if not click.confirm("Are you sure you want to cancel ALL your running jobs?"):
            return
        
        import getpass
        user = getpass.getuser()
        cmd = ['scancel', '--user', user]
        click.echo(f"Cancelling all jobs for user {user}...")
        
    elif job_id:
        cmd = ['scancel', job_id]
        click.echo(f"Cancelling job {job_id}...")
    else:
        raise click.ClickException("Must specify JOB_ID or --all.")

    try:
        subprocess.run(cmd, check=True)
        click.echo("Done.")
    except FileNotFoundError:
        click.echo("Error: 'scancel' command not found. Are you on a system with Slurm?", err=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error cancelling job: {e}", err=True)



@main.command()
def pull():
    """Pull (mirror) the Apptainer container from the registry."""
    container_local = "fairway.sif"
    container_image = "docker://ghcr.io/dissc-yale/fairway:latest"
    
    if os.path.exists(container_local):
        if not click.confirm(f"Container {container_local} already exists. Overwrite?"):
            return

    click.echo(f"Pulling from registry: {container_image}...")
    cmd = ["apptainer", "pull", "--force", container_local, container_image]
    
    try:
        subprocess.run(cmd, check=True)
        click.echo(f"\nContainer pulled successfully: {container_local}")
    except subprocess.CalledProcessError:
        click.echo("\nError: Container pull failed.", err=True)
        click.echo("If you see an auth error, run: source scripts/fairway-hpc.sh registry-login", err=True)
    except FileNotFoundError:
        click.echo("apptainer command not found. Is Apptainer installed?", err=True)


if __name__ == '__main__':
    main()

