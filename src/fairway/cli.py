import click
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from .generate_test_data import generate_test_data


def _validate_slurm_param(value, param_name, pattern, max_length=64):
    """Validate Slurm parameter against allowed pattern to prevent command injection."""
    if not value:
        return value
    value = str(value)
    if len(value) > max_length:
        raise click.ClickException(f"Parameter '{param_name}' exceeds maximum length ({max_length})")
    if not re.match(pattern, value):
        raise click.ClickException(f"Invalid {param_name}: '{value}' (contains disallowed characters)")
    return value


def _validate_slurm_time(time_str):
    """Validate Slurm time format (HH:MM:SS or D-HH:MM:SS)."""
    if not re.match(r'^(\d+-)?(\d{1,2}:)?\d{1,2}:\d{2}$', time_str):
        raise click.ClickException(f"Invalid time format: '{time_str}' (expected HH:MM:SS or D-HH:MM:SS)")
    return time_str


def _validate_slurm_mem(mem_str):
    """Validate Slurm memory format (e.g., 16G, 1024M)."""
    if not re.match(r'^\d+[KMGT]?$', mem_str, re.IGNORECASE):
        raise click.ClickException(f"Invalid memory format: '{mem_str}' (expected e.g., 16G, 1024M)")
    return mem_str


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
    for path in [cfg.raw_dir, cfg.processed_dir, cfg.curated_dir]:
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
        'data/processed',
        'data/curated',
        'src/transformations',
        'docs',
        'scripts',
        'logs/slurm',
    ]
    
    for d in directories:
        os.makedirs(os.path.join(name, d), exist_ok=True)
        click.echo(f"  Created directory: {d}")

    # Create config.yaml
    engine_type = 'pyspark' if engine == 'spark' else 'duckdb'
    from .templates import MAKEFILE_TEMPLATE, CONFIG_TEMPLATE, SPARK_YAML_TEMPLATE, TRANSFORM_TEMPLATE, README_TEMPLATE, DOCS_TEMPLATE
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
    from .templates import DRIVER_TEMPLATE, DRIVER_SCHEMA_TEMPLATE, HPC_SCRIPT
    with open(os.path.join(name, 'scripts', 'driver.sh'), 'w') as f:
        f.write(DRIVER_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'driver.sh'), 0o755)
    click.echo("  Created file: scripts/driver.sh")

    with open(os.path.join(name, 'scripts', 'driver-schema.sh'), 'w') as f:
        f.write(DRIVER_SCHEMA_TEMPLATE)
    os.chmod(os.path.join(name, 'scripts', 'driver-schema.sh'), 0o755)
    click.echo("  Created file: scripts/driver-schema.sh")

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
            pipeline = SchemaDiscoveryPipeline(config, spark_master=spark_master or "local[*]", engine_override='pyspark')
        else:
            # Use DuckDB (default) - portable, no cluster needed
            pipeline = SchemaDiscoveryPipeline(config, spark_master=None, engine_override='duckdb')

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
@click.option('--driver-job-id', default=None, help='Driver Slurm job ID for state file isolation.')
def start(nodes, cpus, mem, time, account, partition, driver_job_id):
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

    # Get dynamic allocation settings
    dynamic_alloc = spark_defaults.get('dynamic_allocation', {})

    # Get arbitrary spark_conf settings (e.g., spark.executor.memory)
    spark_conf = spark_defaults.get('spark_conf', {})

    spark_cfg = {
        'slurm_nodes': nodes,
        'slurm_cpus_per_node': cpus,
        'slurm_mem_per_node': mem,
        'slurm_account': account,
        'slurm_time': time,
        'slurm_partition': partition,
        'dynamic_allocation': dynamic_alloc,
        'spark_conf': spark_conf,
    }

    spark_manager = SlurmSparkManager(spark_cfg, driver_job_id=driver_job_id)
    spark_master = spark_manager.start_cluster()
    click.echo(f"Spark cluster started. Master URL: {spark_master}")

@spark.command()
@click.option('--driver-job-id', default=None, help='Driver Slurm job ID for state file isolation.')
def stop(driver_job_id):
    """Stop the running Spark cluster."""
    from .engines.slurm_cluster import SlurmSparkManager
    spark_manager = SlurmSparkManager({}, driver_job_id=driver_job_id)
    spark_manager.stop_cluster()

@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--spark-master', default=None, help='Spark master URL (e.g., spark://host:port or local[*]).')
@click.option('--dry-run', is_flag=True, help='Show matched files without processing.')
@click.option('--skip-summary', is_flag=True, default=False, help='Skip summary generation after ingestion. Run `fairway summarize` separately.')
@click.option('--log-file', default='logs/fairway.jsonl', help='Path to JSONL log file. Set to empty string to disable.')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), help='Log level.')
def run(config, spark_master, dry_run, skip_summary, log_file, log_level):
    """Run the ingestion pipeline.

    This command executes the pipeline directly on the current machine.
    For HPC submission, use `fairway submit` instead.
    """
    import yaml
    from .pipeline import IngestionPipeline
    from .logging_config import setup_logging

    # Initialize logging FIRST
    setup_logging(
        log_file=log_file if log_file else None,
        level=log_level.upper(),
        console=True
    )

    # Auto-discover config
    if config is None:
        config = discover_config()
        click.echo(f"Auto-discovered config: {config}")

    # Load spark_conf from spark.yaml for executor settings
    spark_conf = None
    spark_yaml_path = 'config/spark.yaml'
    if os.path.exists(spark_yaml_path):
        with open(spark_yaml_path, 'r') as f:
            spark_defaults = yaml.safe_load(f) or {}
            spark_conf = spark_defaults.get('spark_conf', {})

    if dry_run:
        click.echo(f"DRY RUN - showing matched files for config: {config}\n")
        pipeline = IngestionPipeline(config, spark_master=spark_master, spark_conf=spark_conf)
        pipeline.dry_run()
        return

    click.echo(f"Starting pipeline execution using config: {config}")
    if skip_summary:
        click.echo("Summary generation will be skipped. Run `fairway summarize` separately.")
    pipeline = IngestionPipeline(config, spark_master=spark_master, spark_conf=spark_conf)
    try:
        pipeline.run(skip_summary=skip_summary)
        click.echo("Pipeline execution completed successfully.")
    finally:
        # Explicitly stop the engine to release Spark resources
        if hasattr(pipeline, 'engine') and hasattr(pipeline.engine, 'stop'):
            pipeline.engine.stop()


@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--spark-master', default=None, help='Spark master URL (e.g., spark://host:port or local[*]).')
@click.option('--slurm', is_flag=True, help='Submit as a Slurm job (loads Spark/Java modules).')
@click.option('--account', default=None, help='Slurm account.')
@click.option('--partition', default='day', help='Slurm partition.')
@click.option('--time', 'slurm_time', default='04:00:00', help='Slurm time limit (HH:MM:SS).')
@click.option('--mem', default='32G', help='Slurm memory.')
@click.option('--cpus', default=4, type=int, help='Slurm CPUs per task.')
@click.option('--log-file', default='logs/fairway.jsonl', help='Path to JSONL log file. Set to empty string to disable.')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), help='Log level.')
def summarize(config, spark_master, slurm, account, partition, slurm_time, mem, cpus, log_file, log_level):
    """Generate summary stats and reports for already-ingested data.

    This command generates summary statistics, markdown reports, and exports
    to Redivis for data that has already been ingested to curated_dir.

    Use this after running `fairway run --skip-summary` to generate summaries
    in a separate step.

    Examples:

        # Run locally (requires JAVA_HOME to be set)
        fairway summarize

        # Submit as Slurm job (loads Spark/Java modules automatically)
        fairway summarize --slurm

        # Submit with custom resources
        fairway summarize --slurm --mem 64G --time 08:00:00
    """
    import yaml
    import tempfile
    from .logging_config import setup_logging

    # Auto-discover config first (needed for both paths)
    if config is None:
        config = discover_config()
        click.echo(f"Auto-discovered config: {config}")

    # ---------------------------------------------------------
    # SLURM SUBMISSION
    # ---------------------------------------------------------
    if slurm:
        # Load spark.yaml for account default
        spark_yaml_path = 'config/spark.yaml'
        if os.path.exists(spark_yaml_path):
            with open(spark_yaml_path, 'r') as f:
                spark_defaults = yaml.safe_load(f) or {}
                account = account or spark_defaults.get('account', 'borzekowski')

        # Validate parameters
        slurm_time = _validate_slurm_time(slurm_time)
        mem = _validate_slurm_mem(mem)
        partition = _validate_slurm_param(partition, 'partition', r'^[a-zA-Z0-9_-]+$')
        account = _validate_slurm_param(account, 'account', r'^[a-zA-Z0-9_-]+$')
        config = _validate_slurm_param(config, 'config', r'^[a-zA-Z0-9_./-]+$', max_length=256)
        if cpus < 1 or cpus > 256:
            raise click.ClickException(f"Invalid cpus: {cpus} (must be 1-256)")

        job_script = f'''#!/bin/bash
#SBATCH --job-name=fairway_summary
#SBATCH --output=logs/slurm/summary_%j.log
#SBATCH --time={slurm_time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --partition={partition}
#SBATCH --account={account}

set -e

# Ensure log directory exists
mkdir -p logs/slurm

# Load Spark module (includes Java)
echo "Loading Spark module..."
module load Spark/3.5.1-foss-2022b-Scala-2.13 2>/dev/null || echo "WARNING: Could not load Spark module"

# Run summarization
echo "Running summarization for config: {config}"
fairway summarize --config {config}

echo "Summary generation completed successfully."
'''

        # Ensure logs directory exists
        os.makedirs('logs/slurm', exist_ok=True)

        # Write to temp file and submit
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(job_script)
            script_path = f.name

        try:
            click.echo(f"Submitting summary job (config: {config})...")
            result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)
            if result.returncode == 0:
                click.echo(result.stdout.strip())
                click.echo("Job submitted. Check status with 'fairway status'.")
            else:
                click.echo(f"Error submitting job: {result.stderr}", err=True)
                sys.exit(1)
        except FileNotFoundError:
            click.echo("Error: 'sbatch' command not found. Are you on a system with Slurm?", err=True)
            sys.exit(1)
        finally:
            os.unlink(script_path)
        return

    # ---------------------------------------------------------
    # LOCAL EXECUTION
    # ---------------------------------------------------------
    from .pipeline import IngestionPipeline

    # Initialize logging
    setup_logging(
        log_file=log_file if log_file else None,
        level=log_level.upper(),
        console=True
    )

    # Load spark_conf from spark.yaml for executor settings
    spark_conf = None
    spark_yaml_path = 'config/spark.yaml'
    if os.path.exists(spark_yaml_path):
        with open(spark_yaml_path, 'r') as f:
            spark_defaults = yaml.safe_load(f) or {}
            spark_conf = spark_defaults.get('spark_conf', {})

    click.echo(f"Generating summaries for config: {config}")
    pipeline = IngestionPipeline(config, spark_master=spark_master, spark_conf=spark_conf)
    pipeline.summarize()
    click.echo("Summary generation completed successfully.")


@main.command()
@click.option('--file', '-f', 'log_file', default='logs/fairway.jsonl', help='Path to JSONL log file.')
@click.option('--level', '-l', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), help='Filter by log level.')
@click.option('--batch', '-b', 'batch_id', help='Filter by batch ID (supports partial match).')
@click.option('--last', '-n', 'last_n', type=int, default=0, help='Show only last N entries.')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON instead of formatted text.')
@click.option('--errors', is_flag=True, help='Shortcut for --level ERROR.')
def logs(log_file, level, batch_id, last_n, output_json, errors):
    """View and filter pipeline logs.

    Examples:

        fairway logs                     # Show all logs
        fairway logs --last 20           # Show last 20 entries
        fairway logs --level ERROR       # Show only errors
        fairway logs --errors            # Shortcut for --level ERROR
        fairway logs --batch batch_001   # Filter by batch ID
        fairway logs --json              # Raw JSON output (pipe to jq)
    """
    import json as json_module

    if errors:
        level = 'ERROR'

    if not os.path.exists(log_file):
        raise click.ClickException(f"Log file not found: {log_file}")

    # Read all entries
    entries = []
    with open(log_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json_module.loads(line)
                entries.append(entry)
            except json_module.JSONDecodeError:
                continue  # Skip malformed lines

    # Apply filters
    if level:
        entries = [e for e in entries if e.get('level', '').upper() == level.upper()]

    if batch_id:
        entries = [e for e in entries if batch_id in e.get('batch_id', '')]

    # Apply --last N
    if last_n > 0:
        entries = entries[-last_n:]

    # Output
    if not entries:
        click.echo("No matching log entries found.")
        return

    for entry in entries:
        if output_json:
            click.echo(json_module.dumps(entry))
        else:
            # Formatted output
            ts = entry.get('timestamp', '')[:19]  # Truncate microseconds
            lvl = entry.get('level', 'INFO')
            msg = entry.get('message', '')
            bid = entry.get('batch_id', '')

            # Color by level
            level_colors = {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
            }
            color = level_colors.get(lvl, 'white')

            if bid:
                click.echo(f"{ts} [{click.style(lvl, fg=color)}] [{bid}] {msg}")
            else:
                click.echo(f"{ts} [{click.style(lvl, fg=color)}] {msg}")


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
@click.option('--branch', default='main', help='Git branch/tag to install fairway from (default: main).')
def build(force, branch):
    """Build the container image (Apptainer preferred, falls back to Docker)."""

    # Check for Apptainer.def
    if os.path.exists('Apptainer.def'):
        click.echo(f"Found Apptainer.def. Building Apptainer image from branch: {branch}")

        if os.path.exists("fairway.sif"):
            if force:
                click.echo("Overwriting existing fairway.sif...")
                os.remove("fairway.sif")
            else:
                if not click.confirm("fairway.sif already exists. Overwrite?"):
                    return

        cmd = ["apptainer", "build", "--build-arg", f"FAIRWAY_GIT_REF={branch}", "fairway.sif", "Apptainer.def"]
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


def _get_dev_bind_path():
    """Get bind path for dev mode - mounts local src over container's installed package."""
    # Find local fairway source
    local_src = os.path.join(os.getcwd(), 'src', 'fairway')
    if not os.path.isdir(local_src):
        # Try relative to this file (for when running from fairway repo)
        local_src = os.path.dirname(os.path.abspath(__file__))

    if os.path.isdir(local_src):
        # Container's installed package location
        container_pkg = "/opt/venv/lib/python3.10/site-packages/fairway"
        return f"{local_src}:{container_pkg}"
    return None


@main.command()
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--image', default=None, help='Path to Apptainer image (default: checks local fairway.sif then pulls from registry).')
@click.option('--bind', multiple=True, help='Additional bind paths.')
@click.option('--dev', is_flag=True, help='Dev mode: bind-mount local src/fairway over container package for rapid iteration.')
def shell(config, image, bind, dev):
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

    # Dev mode: bind-mount local source over container's installed package
    dev_bind = None
    if dev:
        dev_bind = _get_dev_bind_path()
        if dev_bind:
            click.echo(f"Dev mode: mounting local source -> {dev_bind}")
        else:
            click.echo("Warning: --dev specified but src/fairway not found in current directory", err=True)

    # Determine image
    if image:
        container_image = image
    elif os.path.exists("fairway.sif"):
        container_image = "fairway.sif"
    else:
        # Default to latest from registry
        container_image = "docker://ghcr.io/dissc-yale/fairway:latest"

    cmd = ["apptainer", "shell"]

    # Add dev bind first (so it takes precedence)
    if dev_bind:
        cmd.extend(["--bind", dev_bind])

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
@click.option('--config', default=None, help='Path to config file. Auto-discovered from config/ if not specified.')
@click.option('--account', default=None, help='Slurm account.')
@click.option('--partition', default='day', help='Slurm partition.')
@click.option('--time', default='24:00:00', help='Time limit (HH:MM:SS).')
@click.option('--mem', default='16G', help='Memory per node.')
@click.option('--cpus', default=4, type=int, help='CPUs per task.')
@click.option('--with-spark', is_flag=True, help='Start Spark cluster before running pipeline.')
@click.option('--with-summary', is_flag=True, help='Include summary generation (default: skip summary, run separately with `fairway summarize`).')
@click.option('--dry-run', is_flag=True, help='Print job script without submitting.')
def submit(config, account, partition, time, mem, cpus, with_spark, with_summary, dry_run):
    """Submit pipeline as a Slurm job.

    The job will run `fairway run --skip-summary` by default (ingestion only).
    Use --with-summary to include summary generation in the same job.

    Examples:

        # Submit ingestion only (default)
        fairway submit --with-spark

        # Submit with summary generation included
        fairway submit --with-spark --with-summary

        # Submit with custom resources
        fairway submit --with-spark --mem 64G --cpus 8 --time 48:00:00

        # Preview the job script
        fairway submit --with-spark --dry-run
    """
    import yaml
    import tempfile

    # Auto-discover config
    if config is None:
        config = discover_config()
        click.echo(f"Auto-discovered config: {config}")

    # Load spark.yaml for defaults
    spark_yaml_path = 'config/spark.yaml'
    spark_defaults = {}
    if os.path.exists(spark_yaml_path):
        with open(spark_yaml_path, 'r') as f:
            spark_defaults = yaml.safe_load(f) or {}

    # Apply defaults from spark.yaml if not specified
    account = account or spark_defaults.get('account', 'borzekowski')

    # Validate all parameters to prevent command injection
    time = _validate_slurm_time(time)
    mem = _validate_slurm_mem(mem)
    partition = _validate_slurm_param(partition, 'partition', r'^[a-zA-Z0-9_-]+$')
    account = _validate_slurm_param(account, 'account', r'^[a-zA-Z0-9_-]+$')
    # Config path validation: only allow safe path characters
    config = _validate_slurm_param(config, 'config', r'^[a-zA-Z0-9_./-]+$', max_length=256)
    if cpus < 1 or cpus > 256:
        raise click.ClickException(f"Invalid cpus: {cpus} (must be 1-256)")

    # Generate job script
    if with_spark:
        job_script = f'''#!/bin/bash
#SBATCH --job-name=fairway_pipeline
#SBATCH --output=logs/slurm/fairway_%j.log
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --partition={partition}
#SBATCH --account={account}

set -e

# Ensure log directory exists
mkdir -p logs/slurm

# =============================================================================
# Apptainer Mode Detection
# =============================================================================
# Detect if we should use Apptainer mode based on:
# 1. FAIRWAY_SIF environment variable
# 2. Local fairway.sif file
USE_APPTAINER="no"
FAIRWAY_SIF="${{FAIRWAY_SIF:-fairway.sif}}"
FAIRWAY_BINDS="${{FAIRWAY_BINDS:-/vast}}"

if [ -f "$FAIRWAY_SIF" ]; then
    USE_APPTAINER="yes"
    echo "Apptainer mode detected: $FAIRWAY_SIF"
else
    echo "Bare-metal mode (no container found at $FAIRWAY_SIF)"
fi

# =============================================================================
# Environment Setup
# =============================================================================
if [ "$USE_APPTAINER" = "yes" ]; then
    echo "Using Apptainer container for Spark cluster and driver"
    # Export for SlurmSparkManager to detect
    export FAIRWAY_SIF
    export FAIRWAY_BINDS
else
    # Load Spark module only in bare-metal mode (container has its own Spark)
    echo "Loading Spark module..."
    module load Spark/3.5.1-foss-2022b-Scala-2.13 2>/dev/null || echo "WARNING: Could not load Spark module"
fi

# Use job-specific state directory to prevent race conditions with concurrent jobs
STATE_DIR="$HOME/.fairway-spark/$SLURM_JOB_ID"
mkdir -p "$STATE_DIR"

# Cleanup function to stop Spark cluster on exit
# NOTE: fairway spark stop runs on HOST (not in container) because it:
#   1. Reads state files from host filesystem
#   2. Runs scancel (Slurm command) to cancel the cluster job
cleanup() {{
    echo "Stopping Spark cluster..."
    fairway spark stop --driver-job-id $SLURM_JOB_ID || true
    # Clean up state directory
    rm -rf "$STATE_DIR"
}}
trap cleanup EXIT

# Start Spark cluster with job isolation
echo "Starting Spark cluster..."
fairway spark start --driver-job-id $SLURM_JOB_ID

# Wait for master URL and conf dir (job-specific paths)
MASTER_URL_FILE="$STATE_DIR/master_url.txt"
CONF_DIR_FILE="$STATE_DIR/conf_dir.txt"
SPARK_ARGS=""
if [ -f "$MASTER_URL_FILE" ]; then
    SPARK_MASTER=$(cat "$MASTER_URL_FILE")
    echo "Spark Master: $SPARK_MASTER"
    SPARK_ARGS="--spark-master $SPARK_MASTER"
else
    echo "WARNING: Spark master URL not found. Running in local mode."
fi

# Export SPARK_CONF_DIR so the driver picks up auth settings (SASL secret) from spark-start
if [ -f "$CONF_DIR_FILE" ]; then
    export SPARK_CONF_DIR=$(cat "$CONF_DIR_FILE")
    echo "Spark Conf Dir: $SPARK_CONF_DIR"
fi

# =============================================================================
# Run Pipeline
# =============================================================================
echo "Running pipeline..."
if [ "$USE_APPTAINER" = "yes" ]; then
    # Build bind paths for Apptainer
    BIND_PATHS="${{FAIRWAY_BINDS}}"
    BIND_PATHS="${{BIND_PATHS}},${{HOME}}/.spark-local"
    BIND_PATHS="${{BIND_PATHS}},${{PWD}}"
    BIND_PATHS="${{BIND_PATHS}},/tmp"
    if [ -n "$SPARK_CONF_DIR" ]; then
        BIND_PATHS="${{BIND_PATHS}},${{SPARK_CONF_DIR}}"
    fi
    echo "Running inside Apptainer with binds: $BIND_PATHS"
    apptainer exec \
        --bind "$BIND_PATHS" \
        --env SPARK_CONF_DIR="${{SPARK_CONF_DIR}}" \
        "$FAIRWAY_SIF" \
        fairway run --config {config} $SPARK_ARGS{'' if with_summary else ' --skip-summary'}
else
    fairway run --config {config} $SPARK_ARGS{'' if with_summary else ' --skip-summary'}
fi

echo "Pipeline completed successfully."
'''
    else:
        job_script = f'''#!/bin/bash
#SBATCH --job-name=fairway_pipeline
#SBATCH --output=logs/slurm/fairway_%j.log
#SBATCH --time={time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={cpus}
#SBATCH --partition={partition}
#SBATCH --account={account}

set -e

# Ensure log directory exists
mkdir -p logs/slurm

# Run pipeline (no Spark cluster)
echo "Running pipeline..."
fairway run --config {config}{'' if with_summary else ' --skip-summary'}

echo "Pipeline completed successfully."
'''

    if dry_run:
        click.echo("Generated job script:")
        click.echo("-" * 60)
        click.echo(job_script)
        click.echo("-" * 60)
        return

    # Ensure logs directory exists
    os.makedirs('logs/slurm', exist_ok=True)

    # Write to temp file and submit
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
        f.write(job_script)
        script_path = f.name

    try:
        click.echo(f"Submitting job (config: {config}, with-spark: {with_spark})...")
        result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)
        if result.returncode == 0:
            click.echo(result.stdout.strip())
            click.echo("Job submitted. Check status with 'fairway status'.")
        else:
            click.echo(f"Error submitting job: {result.stderr}", err=True)
            sys.exit(1)
    except FileNotFoundError:
        click.echo("Error: 'sbatch' command not found. Are you on a system with Slurm?", err=True)
        sys.exit(1)
    finally:
        os.unlink(script_path)


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


# =============================================================================
# MANIFEST COMMANDS
# =============================================================================

@main.group()
def manifest():
    """Inspect and query the file manifest."""
    pass


@manifest.command('list')
def manifest_list():
    """List all tables with manifests."""
    from .manifest import ManifestStore
    from tabulate import tabulate

    store = ManifestStore()
    tables = store.list_tables()

    if not tables:
        click.echo("No tables found in manifest. Run 'fairway run' first.")
        return

    # Gather stats for each table
    rows = []
    for table_name in tables:
        tm = store.get_table_manifest(table_name)
        files = tm.data.get("files", {})
        total = len(files)
        success = sum(1 for f in files.values() if f.get("status") == "success")
        failed = sum(1 for f in files.values() if f.get("status") == "failed")
        rows.append([table_name, total, success, failed])

    headers = ["Table", "Files", "Success", "Failed"]
    click.echo(tabulate(rows, headers=headers, tablefmt="simple"))


@manifest.command('query')
@click.option('--table', '-t', required=False, help='Table name to query.')
@click.option('--file', '-f', 'file_key', help='Query a specific file by key.')
@click.option('--status', '-s', type=click.Choice(['success', 'failed']), help='Filter by status.')
@click.option('--batch-id', '-b', help='Filter by batch ID.')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON.')
def manifest_query(table, file_key, status, batch_id, json_output):
    """Query files in the manifest.

    Examples:

        fairway manifest query --table claims

        fairway manifest query --table claims --status failed

        fairway manifest query --table claims --batch-id batch_001

        fairway manifest query --table claims --file CT_2023_01.csv

        fairway manifest query --table claims --json
    """
    import json as json_module
    from .manifest import ManifestStore
    from tabulate import tabulate

    store = ManifestStore()

    # If no table specified, show error
    if not table:
        click.echo("Error: --table is required. Use 'fairway manifest list' to see available tables.")
        raise SystemExit(1)

    # Check if table exists
    tables = store.list_tables()
    if table not in tables:
        click.echo(f"Error: Table '{table}' not found. Available tables: {', '.join(tables) if tables else 'none'}")
        raise SystemExit(1)

    tm = store.get_table_manifest(table)

    # Query single file
    if file_key:
        entry = tm.query_file(file_key)
        if entry is None:
            click.echo(f"File '{file_key}' not found in {table} manifest.")
            raise SystemExit(1)

        if json_output:
            result = entry.copy()
            result["file_key"] = file_key
            click.echo(json_module.dumps(result, indent=2, default=str))
        else:
            click.echo(f"File: {file_key}")
            click.echo(f"  Status: {entry.get('status', 'unknown')}")
            click.echo(f"  Last Processed: {entry.get('last_processed', 'unknown')}")
            click.echo(f"  Hash: {entry.get('hash', 'unknown')}")
            metadata = entry.get('metadata', {})
            if metadata:
                click.echo(f"  Metadata:")
                for k, v in metadata.items():
                    click.echo(f"    {k}: {v}")
        return

    # Query with filters
    results = tm.query_files(status=status, batch_id=batch_id)

    if not results:
        click.echo(f"No files found matching filters.")
        return

    if json_output:
        click.echo(json_module.dumps(results, indent=2, default=str))
    else:
        # Format as table
        rows = []
        for entry in results:
            metadata = entry.get('metadata', {})
            rows.append([
                entry.get('file_key', 'unknown'),
                entry.get('status', 'unknown'),
                metadata.get('batch_id', '-'),
                metadata.get('partition', '-'),
            ])

        headers = ["File", "Status", "Batch ID", "Partition"]
        click.echo(tabulate(rows, headers=headers, tablefmt="simple"))
        click.echo(f"\nTotal: {len(results)} files")


@main.group()
def cache():
    """Manage fairway cache."""
    pass


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
    for dirpath, dirnames, filenames in os.walk(cache_dir):
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

