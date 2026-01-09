import click
import os
import shutil
import subprocess
from datetime import datetime
from .generate_test_data import generate_test_data

@click.group()
def main():
    """fairway: A portable data ingestion framework."""
    pass

@main.command()
@click.argument('name')
def init(name):
    """Initialize a new fairway project."""
    click.echo(f"Initializing new fairway project: {name}")
    
    directories = [
        'config',
        'data/raw',
        'data/intermediate',
        'data/final',
        'src/transformations',
        'docs',
        'logs/slurm',
        'logs/nextflow'
    ]
    
    for d in directories:
        os.makedirs(os.path.join(name, d), exist_ok=True)
        click.echo(f"  Created directory: {d}")

    # Copy template files if they exist in the package
    # For now, we'll just create placeholder READMEs
    with open(os.path.join(name, 'README.md'), 'w') as f:
        f.write(f"# {name}\n\nInitialized by fairway on {datetime.now().isoformat()}\n")

    click.echo(f"Project {name} initialized successfully.")

@main.command()
@click.option('--size', type=click.Choice(['small', 'large']), default='small', help='Size of dataset to generate.')
@click.option('--partitioned/--no-partitioned', default=True, help='Generate partitioned Parquet data (year/month).')
def generate_data(size, partitioned):
    """Generate mock test data."""
    click.echo(f"Generating {size} test data (partitioned={partitioned})...")
    generate_test_data(size=size, partitioned=partitioned)

@main.command()
@click.option('--config', default='config/example_config.yaml', help='Path to the config file.')
@click.option('--profile', default='standard', help='Nextflow profile to use.')
@click.option('--slurm', is_flag=True, help='Run as a Slurm batch job (allocates a controller node).')
@click.option('--with-spark', is_flag=True, help='Automatically provision a Spark-on-Slurm cluster.')
@click.option('--slurm-cpus', 'cpus', default=4, help='CPUs per task/node.')
@click.option('--slurm-mem', 'mem', default='16G', help='Memory per node.')
@click.option('--slurm-time', 'time', default='24:00:00', help='Time limit.')
@click.option('--slurm-nodes', 'nodes', default=1, help='Number of nodes.')
@click.option('--account', default='borzekowski', help='Slurm account.')
@click.option('--partition', default='day', help='Slurm partition.')
@click.option('--batch-size', default=30, help='Max parallel jobs.')
def run(config, profile, slurm, with_spark, cpus, mem, time, account, partition, nodes, batch_size):
    """Run the fairway ingestion pipeline."""
    # Load config to check engine
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

    try:
        if slurm:
            click.echo("Submitting fairway pipeline as a Slurm batch job...")
            # Template for sbatch script (Controller node)
            sbatch_content = f"""#!/bin/bash
#SBATCH --job-name=fairway_{os.path.basename(config)}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --time={time}
#SBATCH --account={account}
#SBATCH --partition={partition}
#SBATCH --output=logs/slurm/fairway_%j.log

module load Nextflow
# Pass the spark master if we have one
SPARK_URL_ARG=""
if [ ! -z "$SPARK_MASTER_URL" ]; then
    SPARK_URL_ARG="--spark_master $SPARK_MASTER_URL"
fi

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
"""
            with open('run_fairway_slurm.sh', 'w') as f:
                f.write(sbatch_content)
            
            subprocess.run(['sbatch', 'run_fairway_slurm.sh'])
        else:
            click.echo(f"Running fairway pipeline with config: {config}, profile: {profile}")
            cmd = [
                'nextflow', 'run', 'main.nf', 
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
            subprocess.run(cmd)
    finally:
        if spark_manager and not slurm:
            # Only stop if we are running in the current terminal session
            # If we submitted via sbatch, the controller script should handle cleanup
            spark_manager.stop_cluster()

if __name__ == '__main__':
    main()
