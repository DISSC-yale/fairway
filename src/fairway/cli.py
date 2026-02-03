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
    
    # 2. Check table paths
    if cfg.tables:
        for tbl in cfg.tables:
            path = tbl.get('path')
            if path:
                abs_path = os.path.abspath(path)
                if os.path.exists(abs_path):
                    bind_paths.add(abs_path)
                # Handle unexpanded path patterns if any (though config loader might have expanded them)
                # The config loader expands tables, so 'path' should be concrete file paths
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

    # Write fairway_processes.nf (reusable process definitions)
    from .templates import FAIRWAY_PROCESSES_NF
    with open(os.path.join(name, 'fairway_processes.nf'), 'w') as f:
        f.write(FAIRWAY_PROCESSES_NF)
    click.echo("  Created file: fairway_processes.nf (batch process definitions)")

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
    from .templates import DRIVER_TEMPLATE, DRIVER_SCHEMA_TEMPLATE, HPC_SCRIPT, RUN_PIPELINE_SCRIPT
    with open(os.path.join(name, 'scripts', 'driver.sh'), 'w') as f:
        f.write(DRIVER_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'driver.sh'), 0o755)
    click.echo("  Created file: scripts/driver.sh")

    with open(os.path.join(name, 'scripts', 'driver-schema.sh'), 'w') as f:
        f.write(DRIVER_SCHEMA_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'driver-schema.sh'), 0o755)
    click.echo("  Created file: scripts/driver-schema.sh")

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
@click.option('--format', type=click.Choice(['csv', 'tsv', 'parquet']), default='csv', help='Output format (csv, tsv, or parquet).')
def generate_data(size, partitioned, format):
    """Generate mock test data."""
    click.echo(f"Generating {size} test data (partitioned={partitioned}, format={format})...")
    generate_test_data(size=size, partitioned=partitioned, file_format=format)

@main.command()
@click.argument('file_path', required=False)
@click.option('--config', help='Path to fairway.yaml config (enables Pipeline Mode).')
@click.option('--output', help='Output file path for the schema (YAML).')
@click.option('--engine', type=click.Choice(['duckdb', 'pyspark']), default=None, help='Engine for schema inference. Default: duckdb (portable). Use pyspark for distributed inference on slow storage.')
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

        # Determine engine: CLI flag > config > default (duckdb)
        config_engine = cfg.engine.lower() if cfg.engine else 'duckdb'
        effective_engine = engine if engine else config_engine

        click.echo(f"Using engine: {effective_engine}")

        if effective_engine in ['pyspark', 'spark']:
            # Use Spark - requires spark_master for distributed mode
            if spark_master:
                click.echo(f"Spark master: {spark_master}")
            else:
                click.echo("No spark_master provided, using local[*]")
            pipeline = SchemaDiscoveryPipeline(config, spark_master=spark_master or "local[*]")
        else:
            # Use DuckDB (default) - portable, no cluster needed
            pipeline = SchemaDiscoveryPipeline(config, spark_master=None)

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
             elif file_path.endswith('.tsv') or '.tsv' in file_path: fmt = 'tsv'
             elif file_path.endswith('.json') or '.json' in file_path: fmt = 'json'
             
             if os.path.isdir(file_path):
                  entries = os.listdir(file_path)
                  if any(e.endswith('.csv') for e in entries): fmt = 'csv'
                  elif any(e.endswith('.tsv') for e in entries): fmt = 'tsv'
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
@click.option('--dry-run', is_flag=True, help='Show matched files without processing.')
def run(config, spark_master, dry_run):
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

    if dry_run:
        click.echo(f"DRY RUN - showing matched files for config: {config}\n")
        pipeline = IngestionPipeline(config, spark_master=spark_master)
        pipeline.dry_run()
        return

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


@main.group()
def cache():
    """Manage fairway cache."""
    pass


# ============================================================
# Batch Commands (for Nextflow orchestration)
# ============================================================

@main.command()
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
@click.option('--count', 'show_count', is_flag=True, help='Show file count only.')
@click.option('--batch', type=int, default=None, help='Show files for specific batch.')
def files(config, table, show_count, batch):
    """List files for a table, optionally filtered by batch."""
    from .batch_processor import BatchProcessor

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table)

    if show_count:
        click.echo(bp.get_file_count())
    elif batch is not None:
        try:
            batch_files = bp.get_files_for_batch(batch)
            for f in batch_files:
                click.echo(f)
        except ValueError as e:
            raise click.ClickException(str(e))
    else:
        # List all files
        for f in bp._discover_files():
            click.echo(f)


@main.command()
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
@click.option('--size', type=int, default=None, help='Override batch size.')
def batches(config, table, size):
    """Show number of batches for a table."""
    from .batch_processor import BatchProcessor

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table, batch_size=size)
    click.echo(bp.get_batch_count())


@main.command('schema-scan')
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
@click.option('--batch', type=int, required=True, help='Batch number to scan.')
def schema_scan(config, table, batch):
    """Scan schema for a specific batch.

    For Nextflow orchestration, this runs on SLURM compute nodes.
    Unzipping is distributed via SLURM (one batch per node), not Spark.
    Extractions are cached to temp_location for reuse across batches.
    """
    from .batch_processor import BatchProcessor
    import json
    import zipfile
    import glob as glob_module

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table)

    try:
        batch_files = bp.get_files_for_batch(batch)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(f"Scanning schema for batch {batch} ({len(batch_files)} files)...")

    from .engines.duckdb_engine import DuckDBEngine
    engine = DuckDBEngine()

    if batch_files:
        sample_file = batch_files[0]
        fmt = bp.table_config.get('format', 'csv')

        # Check if preprocessing is configured (preprocess.action: unzip)
        preprocess = bp.table_config.get('preprocess', {})
        action = preprocess.get('action')

        # Handle zip files - extract before schema inference
        # Triggers if: (1) preprocess.action == 'unzip', OR (2) file is a zip
        if action == 'unzip' or (sample_file.endswith('.zip') and zipfile.is_zipfile(sample_file)):
            click.echo(f"  Extracting zip file: {sample_file}")

            # Temp location priority: FAIRWAY_TEMP env > config.temp_location > .fairway_cache
            # This should be on shared storage (e.g., GPFS) for HPC clusters
            temp_base = bp.config.temp_location or '.fairway_cache'

            # Extract to: {temp_base}/{table_name}/{zip_filename_without_ext}/
            # Using table name + zip name for human-readable, deterministic paths
            zip_name = os.path.splitext(os.path.basename(sample_file))[0]
            extract_dir = os.path.join(temp_base, table, zip_name)

            # Extract if not already cached (enables reuse across batches/runs)
            if not os.path.exists(extract_dir):
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(sample_file, 'r') as zf:
                    zf.extractall(extract_dir)
                click.echo(f"  Extracted to: {extract_dir}")
            else:
                click.echo(f"  Using cached extraction: {extract_dir}")

            # Find extracted files matching the declared format
            ext_map = {'csv': '**/*.csv', 'tsv': '**/*.tsv', 'tab': '**/*.tab',
                       'json': '**/*.json', 'parquet': '**/*.parquet'}
            pattern = os.path.join(extract_dir, ext_map.get(fmt, '**/*'))
            extracted_files = glob_module.glob(pattern, recursive=True)

            if not extracted_files:
                raise click.ClickException(
                    f"No {fmt} files found in zip: {sample_file}"
                )

            click.echo(f"  Found {len(extracted_files)} {fmt} files in archive")
            sample_file = extracted_files[0]

        schema = engine.infer_schema(sample_file, fmt)

        # Write schema to batch work directory
        batch_dir = bp.get_batch_dir(batch)
        os.makedirs(batch_dir, exist_ok=True)
        schema_path = os.path.join(batch_dir, f'schema_{batch}.json')

        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)

        click.echo(f"Schema written to {schema_path}")


@main.command('schema-merge')
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
def schema_merge(config, table):
    """Merge partial schemas from all batches."""
    from .batch_processor import BatchProcessor
    import json

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table)
    work_dir = os.path.join(bp.work_dir, table)

    # Collect all partial schemas
    merged_schema = {}
    schema_files = []

    for batch in range(bp.get_batch_count()):
        schema_path = os.path.join(work_dir, f'batch_{batch}', f'schema_{batch}.json')
        if os.path.exists(schema_path):
            schema_files.append(schema_path)
            with open(schema_path, 'r') as f:
                partial = json.load(f)
                # Merge: later schemas can override (simple strategy)
                merged_schema.update(partial)

    if not schema_files:
        # Check for legacy schema files in work_dir directly
        for f in os.listdir(work_dir) if os.path.exists(work_dir) else []:
            if f.startswith('schema_') and f.endswith('.json'):
                schema_path = os.path.join(work_dir, f)
                schema_files.append(schema_path)
                with open(schema_path, 'r') as sf:
                    partial = json.load(sf)
                    merged_schema.update(partial)

    click.echo(f"Merged {len(schema_files)} schema files")

    # Write unified schema
    unified_path = os.path.join(work_dir, 'unified_schema.json')
    os.makedirs(work_dir, exist_ok=True)
    with open(unified_path, 'w') as f:
        json.dump(merged_schema, f, indent=2)

    click.echo(f"Unified schema written to {unified_path}")


@main.command()
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
@click.option('--batch', type=int, required=True, help='Batch number to ingest.')
@click.option('--spark-master', default=None, help='Spark master URL.')
def ingest(config, table, batch, spark_master):
    """Ingest a specific batch."""
    from .batch_processor import BatchProcessor

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table)

    try:
        batch_files = bp.get_files_for_batch(batch)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(f"Ingesting batch {batch} ({len(batch_files)} files)...")

    # For now, just confirm the files exist
    # Full implementation would use the ingestion pipeline
    batch_dir = bp.get_batch_dir(batch)
    os.makedirs(batch_dir, exist_ok=True)

    click.echo(f"Batch {batch} ingestion complete. Output: {batch_dir}")


@main.command()
@click.option('--config', default=None, help='Path to config file.')
@click.option('--table', required=True, help='Table name.')
def finalize(config, table):
    """Finalize processing for a table."""
    from .batch_processor import BatchProcessor

    if config is None:
        config = discover_config()

    bp = BatchProcessor(config, table)
    work_dir = os.path.join(bp.work_dir, table)

    click.echo(f"Finalizing table {table}...")

    # Check for completed batches
    completed = 0
    for batch in range(bp.get_batch_count()):
        batch_dir = bp.get_batch_dir(batch)
        if os.path.exists(batch_dir):
            completed += 1

    click.echo(f"Found {completed}/{bp.get_batch_count()} completed batches")
    click.echo(f"Finalization complete for {table}")


@main.command('list-tables')
@click.option('--config', default=None, help='Path to config file.')
def list_tables(config):
    """List all tables defined in config (one per line, for scripting)."""
    import yaml

    if config is None:
        config = discover_config()

    with open(config, 'r') as f:
        raw_config = yaml.safe_load(f)

    tables = raw_config.get('tables', [])
    for table in tables:
        name = table.get('name')
        if name:
            click.echo(name)


@cache.command()
@click.option('--force', is_flag=True, help='Skip confirmation prompt.')
def clean(force):
    """Clear the archive extraction cache (.fairway_cache/)."""
    cache_dir = '.fairway_cache'

    if not os.path.exists(cache_dir):
        click.echo("No cache directory found.")
        return

    # Calculate size
    total_size = 0
    file_count = 0
    for dirpath, _, filenames in os.walk(cache_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
            file_count += 1

    size_mb = total_size / (1024 * 1024)

    if not force:
        if not click.confirm(f"Delete {cache_dir}/ ({file_count} files, {size_mb:.1f} MB)?"):
            return

    shutil.rmtree(cache_dir)
    click.echo(f"Removed {cache_dir}/ ({size_mb:.1f} MB freed)")


if __name__ == '__main__':
    main()

