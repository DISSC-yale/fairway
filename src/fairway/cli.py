# ruff: noqa: E701, E722
# Step 0 baseline suppression: pre-existing single-line if/for/with bodies and a
# bare `except:` in `logs` subcommand. The whole module is rewritten in Step 9
# (cli.py rebuild). Suppressing here keeps Step 0 bounded to baseline capture.
import click
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from string import Template
from .generate_test_data import generate_test_data
from .apptainer import DEFAULT_SIF_NAME
from .config_loader import Config
from .hpc import SlurmManager


def _normalize_engine_name(engine):
    """Inlined from engines/__init__.py for the v0.3 rewrite (Step 1).

    Returns the lowercase, stripped form. The engines package is now
    transitional (empty `__init__.py`) and goes away in Step 7.
    """
    if engine is None:
        return None
    if not isinstance(engine, str):
        return engine
    return engine.strip().lower()


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


def slurm_options(f):
    """Decorator for common Slurm/Resource options."""
    options = [
        click.option('--account', help='Slurm account.'),
        click.option('--partition', help='Slurm partition.'),
        click.option('--time', help='Slurm time limit (HH:MM:SS).'),
        click.option('--cpus', type=int, help='Slurm CPUs.'),
        click.option('--mem', help='Slurm Memory (e.g. 16G).'),
        click.option('--nodes', type=int, help='Number of Spark worker nodes.')
    ]
    for option in reversed(options):
        f = option(f)
    return f


def _validate_slurm_account(account):
    """Reject obvious placeholder account values."""
    _PLACEHOLDER_SLURM_ACCOUNTS = {"your-account", "your_account", "myaccount", "my-account", "account-name", "changeme", "todo"}
    if account and str(account).strip().casefold() in _PLACEHOLDER_SLURM_ACCOUNTS:
        raise click.ClickException(f"Slurm account {account!r} looks like a placeholder. Edit config/spark.yaml or pass --account.")


def _latest_log_file(log_dir):
    """Find the most recently modified run_*.jsonl under a sharded log dir."""
    from pathlib import Path
    root = Path(log_dir)
    if not root.exists():
        return None
    candidates = sorted(root.glob("*/run_*.jsonl"), key=lambda p: p.stat().st_mtime)
    return str(candidates[-1]) if candidates else None


def discover_config():
    """Auto-discover config file in config/ folder."""
    config_dir = 'config'
    if not os.path.isdir(config_dir):
        raise click.ClickException("No config/ directory found. Run 'fairway init' first.")
    configs = [f for f in os.listdir(config_dir) if f.endswith(('.yaml', '.yml')) and not f.endswith('_schema.yaml') and f != 'spark.yaml']
    if len(configs) == 0:
        raise click.ClickException("No config files found in config/")
    elif len(configs) == 1:
        return os.path.join(config_dir, configs[0])
    else:
        raise click.ClickException(f"Multiple config files found: {configs}. Use --config to specify one.")


@click.group()
def main():
    """fairway: A portable data ingestion framework."""
    pass

@main.command()
@click.argument('name')
@click.option('--engine', type=click.Choice(['duckdb']), required=True, help="Compute engine to use.")
@click.option('--force', is_flag=True, default=False, help='Overwrite an existing project directory.')
def init(name, engine, force):
    """Initialize a new fairway project."""
    if os.path.exists(name):
        if not force:
            raise click.ClickException(f"Path '{name}' already exists. Re-run with --force.")
        # Guard against --force blowing away cwd, its ancestors, $HOME, or /.
        target = os.path.realpath(name)
        cwd = os.path.realpath(os.getcwd())
        home = os.path.realpath(os.path.expanduser("~"))
        if (
            target in (os.path.realpath(os.sep), home)
            or target == cwd
            or cwd.startswith(target + os.sep)
        ):
            raise click.ClickException(
                f"Refusing to --force-init {name!r}: resolved path {target!r} is the "
                f"filesystem root, your home directory, your current working directory, "
                f"or an ancestor of it. Pick a new subdirectory name or cd elsewhere."
            )
        if os.path.isdir(name): shutil.rmtree(name)
        else: os.remove(name)
    
    directories = ['config', 'data/raw', 'data/processed', 'data/curated', 'src/transformations', 'docs', 'scripts', 'logs/slurm']
    for d in directories: os.makedirs(os.path.join(name, d), exist_ok=True)

    engine_type = _normalize_engine_name(engine)
    from .templates import MAKEFILE_TEMPLATE, CONFIG_TEMPLATE, SPARK_YAML_TEMPLATE, TRANSFORM_TEMPLATE, README_TEMPLATE, DOCS_TEMPLATE
    
    with open(os.path.join(name, "config/fairway.yaml"), 'w') as f:
        f.write(Template(CONFIG_TEMPLATE).safe_substitute(name=name, engine_type=engine_type))
    
    with open(os.path.join(name, 'config', 'spark.yaml'), 'w') as f:
        f.write(SPARK_YAML_TEMPLATE)

    with open(os.path.join(name, 'Makefile'), 'w') as f:
        f.write(MAKEFILE_TEMPLATE)

    with open(os.path.join(name, 'src', 'transformations', 'example_transform.py'), 'w') as f:
        f.write(TRANSFORM_TEMPLATE.strip())

    with open(os.path.join(name, 'README.md'), 'w') as f:
        f.write(Template(README_TEMPLATE).safe_substitute(name=name, timestamp=datetime.now().isoformat(), engine=engine))

    from .templates import DRIVER_TEMPLATE, DRIVER_SCHEMA_TEMPLATE, HPC_SCRIPT
    for script, content in [('driver.sh', DRIVER_TEMPLATE), ('driver-schema.sh', DRIVER_SCHEMA_TEMPLATE), ('fairway-hpc.sh', HPC_SCRIPT)]:
        path = os.path.join(name, 'scripts', script)
        with open(path, 'w') as f: f.write(content)
        os.chmod(path, 0o755)

    with open(os.path.join(name, 'docs', 'getting-started.md'), 'w') as f:
        f.write(Template(DOCS_TEMPLATE).safe_substitute(name=name, engine_type=engine_type))

    click.echo(f"Project {name} initialized successfully.")

@main.command()
@click.option('--size', type=click.Choice(['small', 'large']), default='small')
@click.option('--partitioned/--no-partitioned', default=True)
@click.option('--format', type=click.Choice(['csv', 'tsv', 'parquet']), default='csv')
def generate_data(size, partitioned, format):
    """Generate mock test data."""
    generate_test_data(size=size, partitioned=partitioned, file_format=format)

@main.command()
@click.argument('file_path', required=False)
@click.option('--config', help='Path to fairway.yaml config.')
@click.option('--output', help='Output file path for the schema.')
@click.option('--engine', type=click.Choice(['duckdb']), default=None)
@click.option('--sampling-ratio', type=float, default=1.0)
@click.option('--slurm', is_flag=True, help='Submit as a Slurm job.')
@slurm_options
def generate_schema(file_path, config, output, engine, sampling_ratio, slurm, **overrides):
    """Generate schema from data."""
    if slurm:
        script_path = "scripts/driver-schema.sh"
        if not os.path.exists(script_path):
            raise click.ClickException(f"{script_path} not found. Run 'fairway init'.")
        subprocess.run(["sbatch", script_path], check=True)
        return

    if config:
        from .schema_pipeline import SchemaDiscoveryPipeline
        cfg = Config(config, overrides=overrides)
        effective_engine = _normalize_engine_name(engine or cfg.engine)
        pipeline = SchemaDiscoveryPipeline(config, engine_override=effective_engine)
        pipeline.run_inference(output_path=output, sampling_ratio=sampling_ratio)
        return

    if not file_path: raise click.ClickException("Must provide FILE_PATH or --config.")
    
    # Legacy logic remains for simple direct file inference
    import yaml
    import duckdb
    if not os.path.exists(file_path):
        raise click.ClickException(f"Path not found: {file_path}")

    # Detect partitioned dir for legacy mode
    partition_columns = []
    sample_file = file_path
    if os.path.isdir(file_path):
        click.echo(f"Detected partitioned directory: {file_path}")
        # Simplistic discovery for legacy mode
        for root, dirs, files in os.walk(file_path):
            for d in dirs:
                if '=' in d:
                    col = d.split('=')[0]
                    if col not in partition_columns: partition_columns.append(col)
            for f in files:
                if f.endswith(('.csv', '.parquet', '.json')):
                    sample_file = os.path.join(root, f)
                    break
            if sample_file != file_path: break

    click.echo(f"Inferring schema from {sample_file}...")
    try:
        if sample_file.endswith('.parquet'):
            rel = duckdb.read_parquet(sample_file)
        elif sample_file.endswith('.json'):
            rel = duckdb.read_json(sample_file)
        else:
            rel = duckdb.read_csv(sample_file)
    except Exception as e:
        raise click.ClickException(f"Error reading {sample_file}: {e}")
    
    schema_output = {'name': os.path.basename(file_path.rstrip('/'))}
    if partition_columns: schema_output['partition_by'] = partition_columns
    schema_output['columns'] = {n: str(t) for n, t in zip(rel.columns, rel.types) if n not in partition_columns}
    
    out_path = output or f"config/{schema_output['name']}_schema.yaml"
    if os.path.dirname(out_path):
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f: yaml.dump(schema_output, f)
    click.echo(f"Schema written to {out_path}")

@main.command()
@click.option('--config', default=None)
@click.option('--spark-master', default=None)
@click.option('--dry-run', is_flag=True)
@click.option('--skip-summary', is_flag=True, default=False)
@click.option('--log-file', default=None,
              help='Override log file path. Defaults to PathResolver.log_file for the current run_id.')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
def run(config, spark_master, dry_run, skip_summary, log_file, log_level):
    """Run the ingestion pipeline."""
    from .pipeline import IngestionPipeline
    from .logging_config import setup_logging
    from .paths import generate_run_id

    cfg = Config(config or discover_config())

    if dry_run:
        setup_logging(log_file=log_file, level=log_level.upper(), console=True)
        click.echo(f"DRY RUN - showing matched files for config: {cfg.config_path}\n")
        pipeline = IngestionPipeline(cfg.config_path, spark_master=spark_master, spark_conf=cfg.hpc.spark_conf)
        pipeline.dry_run()
        return

    # Pre-calculate run_id so we can setup logging before pipeline init
    run_id = generate_run_id()
    resolved_paths = cfg.paths.with_run_id(run_id)
    resolved_log = log_file or str(resolved_paths.log_file)
    setup_logging(log_file=resolved_log, level=log_level.upper(), console=True)

    click.echo(f"Starting pipeline execution using config: {cfg.config_path}")
    if skip_summary:
        click.echo("Skipping end-of-run summary.")

    pipeline = IngestionPipeline(
        cfg.config_path,
        spark_master=spark_master,
        spark_conf=cfg.hpc.spark_conf,
        run_id_override=run_id
    )
    pipeline.run(skip_summary=skip_summary)
    click.echo("Pipeline execution completed successfully.")


@main.command("run-abandon", hidden=True)
@click.option('--config', default=None)
@click.argument('run_id')
def run_abandon(config, run_id):
    """Idempotently mark a run as unfinished.

    Called from sbatch `trap` as belt-and-suspenders for crash
    finalization. Safe to call on already-finalized runs — only
    writes when the current state is still 'running'.
    """
    import json as _json
    cfg = Config(config or discover_config())
    path = cfg.paths.run_metadata_file_for(run_id)
    if not path.exists():
        return
    try:
        data = _json.loads(path.read_text())
    except Exception:
        return
    if data.get("exit_status") != "running":
        return
    import datetime as _dt2
    data["exit_status"] = "unfinished"
    data["finished_at"] = _dt2.datetime.now(_dt2.timezone.utc).isoformat()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(_json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)


@main.command()
@click.option('--config', default=None)
@click.option('--slurm', is_flag=True)
@slurm_options
def summarize(config, slurm, **overrides):
    """Generate summary stats."""
    cfg = Config(config or discover_config(), overrides=overrides)
    if slurm:
        _validate_slurm_account(cfg.resolve_resources()['account'])
        SlurmManager(cfg).submit_job("summarize.sh", f"Submitting summary job for {cfg.config_path}...")
    else:
        from .pipeline import IngestionPipeline
        IngestionPipeline(cfg.config_path, spark_conf=cfg.hpc.spark_conf).summarize()

@main.command()
@click.option('--config', default=None)
@click.option('--with-spark', is_flag=True)
@click.option('--with-summary', is_flag=True)
@click.option('--dry-run', is_flag=True)
@slurm_options
def submit(config, with_spark, with_summary, dry_run, **overrides):
    """Submit pipeline as a Slurm job."""
    cfg = Config(config or discover_config(), overrides=overrides)
    res = cfg.resolve_resources()
    _validate_slurm_account(res['account'])

    template = "submit_with_spark.sh" if with_spark else "submit_bare.sh"
    extra = {'summary_flag': '' if with_summary else ' --skip-summary'}
    
    if dry_run:
        mgr = SlurmManager(cfg)
        click.echo(mgr._render_template(template, **mgr.build_template_params(extra)))
    else:
        SlurmManager(cfg).submit_job(template, f"Submitting job (with-spark={with_spark})...", extra_params=extra)

@main.command()
@click.option('--file', '-f', 'log_file', default=None,
              help='Path to JSONL log file. If omitted, picks latest run.json under the project log dir.')
@click.option('--config', default=None,
              help='Config path used to resolve the project log dir when --file is omitted.')
@click.option('--run-id', 'run_id', default=None, help='Inspect a specific run_id (requires --config).')
@click.option('--level', '-l', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), help='Filter by log level.')
@click.option('--batch', '-b', 'batch_id', help='Filter by batch ID.')
@click.option('--last', '-n', 'last_n', type=int, default=0, help='Show only last N entries.')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON.')
@click.option('--errors', is_flag=True, help='Shortcut for --level ERROR.')
def logs(log_file, config, run_id, level, batch_id, last_n, output_json, errors):
    """View pipeline logs."""
    import json as json_module
    if errors: level = 'ERROR'
    if log_file is None:
        cfg = Config(config or discover_config())
        if run_id:
            log_file = str(cfg.paths.log_file_for(run_id))
        else:
            log_file = _latest_log_file(cfg.paths.log_dir)
            if not log_file:
                raise click.ClickException(
                    f"No log files found under {cfg.paths.log_dir}"
                )
    if not os.path.exists(log_file): raise click.ClickException(f"Log file not found: {log_file}")
    entries = []
    with open(log_file, 'r') as f:
        for line in f:
            try: entries.append(json_module.loads(line))
            except: continue
    if level: entries = [e for e in entries if e.get('level', '').upper() == level.upper()]
    if batch_id: entries = [e for e in entries if batch_id and batch_id in e.get('batch_id', '')]
    if last_n > 0: entries = entries[-last_n:]
    if not entries:
        click.echo("No matching log entries found.")
        return
    for e in entries:
        if output_json: click.echo(json_module.dumps(e))
        else:
            ts, lvl, msg = e.get('timestamp', '').replace('T', ' ')[:19], e.get('level', 'INFO'), e.get('message', '')
            bid = f" [{e['batch_id']}]" if e.get('batch_id') else ""
            click.echo(f"{ts} [{lvl}]{bid} {msg}")

@main.command()
@click.option('--scripts', is_flag=True)
@click.option('--container', is_flag=True)
@click.option('--output', '-o', default='.')
@click.option('--force', is_flag=True)
def eject(scripts, container, output, force):
    """Eject bundled scripts and container files."""
    from .templates import APPTAINER_DEF, DOCKERFILE_TEMPLATE, DOCKERIGNORE, MAKEFILE_TEMPLATE, DRIVER_TEMPLATE, DRIVER_SCHEMA_TEMPLATE, SPARK_START_TEMPLATE, HPC_SCRIPT
    e_scripts, e_cont = scripts or not container, container or not scripts
    if output != '.': os.makedirs(output, exist_ok=True)
    created = []
    def write(rel, content, exe=False):
        full = os.path.join(output, rel)
        if os.path.exists(full) and not force:
            click.echo(f"  Skipping (exists): {rel}")
            return
        if os.path.dirname(full): os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, 'w') as f: f.write(content)
        if exe: os.chmod(full, 0o755)
        click.echo(f"  Created: {rel}")
        created.append(rel)
    if e_cont:
        click.echo("Ejecting container files...")
        for f, c in [('Apptainer.def', APPTAINER_DEF), ('Dockerfile', DOCKERFILE_TEMPLATE), ('.dockerignore', DOCKERIGNORE), ('Makefile', MAKEFILE_TEMPLATE)]: write(f, c)
    if e_scripts:
        click.echo("Ejecting Slurm/HPC scripts...")
        for f, c in [('scripts/driver.sh', DRIVER_TEMPLATE), ('scripts/driver-schema.sh', DRIVER_SCHEMA_TEMPLATE), ('scripts/fairway-spark-start.sh', SPARK_START_TEMPLATE), ('scripts/fairway-hpc.sh', HPC_SCRIPT)]: write(f, c, True)
    if created: click.echo(f"\nEjected {len(created)} files to {output}")
    else: click.echo("\nNo files created (all already exist). Use --force to overwrite.")

def _get_apptainer_binds(cfg) -> str:
    """Return a comma-separated bind spec derived from config dirs + table roots.

    Mirrors `Config.binds_list` but accepts any object (including mocks) that
    exposes `raw_dir`, `processed_dir`, `curated_dir`, `temp_dir`, `tables`,
    and `apptainer_binds`. Non-existent storage dirs are skipped.
    """
    bind_paths = set()
    for path in [getattr(cfg, "raw_dir", None), getattr(cfg, "processed_dir", None), getattr(cfg, "curated_dir", None)]:
        if not path:
            continue
        abs_p = os.path.abspath(path)
        if os.path.exists(abs_p):
            bind_paths.add(os.path.dirname(abs_p) if os.path.isfile(abs_p) else abs_p)
    temp_dir = getattr(cfg, "temp_dir", None)
    if temp_dir:
        bind_paths.add(os.path.abspath(temp_dir))
    for tbl in getattr(cfg, "tables", []) or []:
        root = tbl.get("root") if isinstance(tbl, dict) else getattr(tbl, "root", None)
        if root:
            bind_paths.add(os.path.abspath(root))
    extra = getattr(cfg, "apptainer_binds", None)
    if extra:
        for p in str(extra).split(","):
            if p.strip():
                bind_paths.add(p.strip())
    return ",".join(sorted(bind_paths))


def _get_dev_bind_path() -> str | None:
    """Return a `host:container` bind string that overlays the local source
    checkout onto the container's installed fairway package. Used by --dev
    to let edits on the host take effect inside the container.

    Prefers ./src/fairway in CWD; falls back to the module's own location.
    """
    container_target = "/opt/venv/lib/python3.10/site-packages/fairway"
    local_src = os.path.join(os.getcwd(), "src", "fairway")
    if os.path.isdir(local_src):
        return f"{os.path.abspath(local_src)}:{container_target}"
    try:
        import fairway as _fw
        mod_path = os.path.dirname(os.path.abspath(_fw.__file__))
        return f"{mod_path}:{container_target}"
    except Exception:
        return None


@main.command()
@click.option('--force', is_flag=True)
def build(force):
    """Build the container image."""
    if os.path.exists('Apptainer.def'):
        if os.path.exists('fairway.sif'):
            if force:
                click.echo("Overwriting existing fairway.sif")
            elif not click.confirm("fairway.sif exists. Overwrite?"):
                return
        subprocess.run(["apptainer", "build", "fairway.sif", "Apptainer.def"], check=True)
        click.echo("Build complete: fairway.sif")
    elif os.path.exists("Dockerfile"):
        subprocess.run(["docker", "build", "-t", "fairway", "."], check=True)
        click.echo("Build complete: fairway:latest")
    else: raise click.ClickException("No container definition found.")

@main.command()
@click.option('--config', default=None)
@click.option('--image', default='fairway.sif')
@click.option('--bind', multiple=True)
@click.option('--dev', is_flag=True)
def shell(config, image, bind, dev):
    """Enter an interactive shell inside the container."""
    cfg = Config(config or discover_config())
    binds = set(cfg.binds_list.split(',')) | set(bind)
    if dev:
        dev_bind = _get_dev_bind_path()
        if dev_bind:
            binds.add(dev_bind)
    cmd = ["apptainer", "shell"]
    if binds: cmd.extend(["--bind", ",".join(filter(None, binds))])
    cmd.append(image)
    subprocess.run(cmd)

@main.group()
def cache():
    """Manage fairway cache."""
    pass

@cache.command()
@click.option('--config', default=None)
@click.option('--force', is_flag=True)
def clean(config, force):
    """Clear the archive extraction cache.

    Targets PathResolver.cache_dir (under FAIRWAY_SCRATCH). Also sweeps
    the legacy CWD-local `.fairway_cache` location so operators
    upgrading from pre-v4.1 installs can clean up in one step.
    """
    targets = []
    # Only swallow "no config discoverable" — real config errors must
    # surface so operators don't think the cache cleaned when it didn't.
    try:
        cfg_path = config or discover_config()
    except click.ClickException:
        cfg_path = None
    if cfg_path:
        cfg = Config(cfg_path)
        resolved_cache = cfg.paths.cache_dir
        if resolved_cache.exists() or os.path.islink(str(resolved_cache)):
            targets.append(str(resolved_cache))

    legacy = os.path.join(os.getcwd(), '.fairway_cache')
    if (os.path.exists(legacy) or os.path.islink(legacy)) and legacy not in targets:
        targets.append(legacy)

    if not targets:
        click.echo("No cache to clear.")
        return

    for target in targets:
        if force or click.confirm(f"Delete {target}?"):
            # rmtree refuses symlinked dirs; unlink those explicitly.
            if os.path.islink(target):
                os.unlink(target)
            else:
                shutil.rmtree(target)
            click.echo(f"Cleared {target}.")

@main.command()
def pull():
    """Pull the Apptainer container."""
    subprocess.run(["apptainer", "pull", "--force", DEFAULT_SIF_NAME, "docker://ghcr.io/dissc-yale/fairway:latest"], check=True)

@main.group()
def manifest():
    """Inspect and query the file manifest."""
    pass


def _resolve_manifest_dir(config):
    """Return the manifest dir to read from.

    Prefer PathResolver.manifest_dir (via --config or discover_config);
    fall back to CWD-relative "manifest" when no config is present.
    Tests set up the fixture either way, so both paths need to work.
    """
    try:
        if config:
            cfg = Config(config)
        else:
            cfg = Config(discover_config())
        return str(cfg.paths.manifest_dir)
    except Exception:
        return "manifest"


@manifest.command('list')
@click.option('--config', default=None)
def manifest_list(config):
    """List tables recorded in the manifest."""
    from .manifest import ManifestStore
    from tabulate import tabulate
    store = ManifestStore(_resolve_manifest_dir(config))
    tables = store.list_tables()
    if not tables:
        click.echo("No tables found in manifest.")
        return
    rows = []
    for t in tables:
        m = store.get_table_manifest(t)
        files = m.data.get("files", {})
        rows.append([
            t,
            len(files),
            sum(1 for f in files.values() if f.get("status") == "success"),
            sum(1 for f in files.values() if f.get("status") == "failed"),
        ])
    click.echo(tabulate(rows, headers=["Table", "Files", "Success", "Failed"], tablefmt="simple"))


@manifest.command('query')
@click.option('--config', default=None)
@click.option('--table', required=True, help='Table name to query.')
@click.option('--file', 'file_key', default=None, help='Filter by file path/basename.')
@click.option('--status', default=None, help='Filter by status (success, failed, ...).')
@click.option('--batch-id', 'batch_id', default=None, help='Filter by batch id.')
@click.option('--json', 'as_json', is_flag=True, default=False, help='Emit JSON.')
def manifest_query(config, table, file_key, status, batch_id, as_json):
    """Query files recorded for a table, with optional filters."""
    import json as _json
    from .manifest import ManifestStore
    from tabulate import tabulate
    store = ManifestStore(_resolve_manifest_dir(config))
    if table not in store.list_tables():
        click.echo(f"Table {table!r} not found in manifest.")
        return
    m = store.get_table_manifest(table)
    files = m.data.get("files", {})
    rows = []
    for path, entry in files.items():
        if status and entry.get("status") != status:
            continue
        entry_batch = entry.get("batch_id") or entry.get("metadata", {}).get("batch_id")
        if batch_id and entry_batch != batch_id:
            continue
        if file_key and file_key not in path:
            continue
        rows.append({
            "file": path,
            "status": entry.get("status"),
            "batch_id": entry_batch,
            "metadata": entry.get("metadata", {}),
        })
    if as_json:
        click.echo(_json.dumps(rows, indent=2, default=str))
        return
    if not rows:
        click.echo("No files match the given filters.")
        return
    click.echo(tabulate(
        [[r["file"], r["status"], r["batch_id"]] for r in rows],
        headers=["File", "Status", "Batch"],
        tablefmt="simple",
    ))

@main.command()
@click.option('--user', default=None)
@click.option('--job-id', help='Filter by job ID.')
def status(user, job_id):
    """Show status of Slurm jobs."""
    import getpass
    cmd = ['squeue', '--user', user or getpass.getuser()]
    if job_id: cmd.extend(['--jobs', job_id])
    subprocess.run(cmd)

@main.command()
@click.argument('job_id', required=False)
@click.option('--all', 'kill_all', is_flag=True)
def cancel(job_id, kill_all):
    """Cancel a Slurm job."""
    if kill_all:
        import getpass
        user = getpass.getuser()
        if not click.confirm(f"Cancel ALL Slurm jobs for user {user!r}?"):
            return
        subprocess.run(['scancel', '--user', user], check=True)
    elif job_id:
        subprocess.run(['scancel', job_id], check=True)

@main.group()
def state():
    """Inspect and initialize fairway state directories."""
    pass


def _state_dirs_for(cfg):
    """Ordered list of (label, Path) for every fairway-managed dir."""
    p = cfg.paths
    return [
        ("project_state_dir", p.project_state_dir),
        ("manifest_dir", p.manifest_dir),
        ("log_dir", p.log_dir),
        ("slurm_log_dir", p.slurm_log_dir),
        ("spark_coordination_dir", p.spark_coordination_dir),
        ("lock_dir", p.lock_dir),
        ("project_scratch_dir", p.project_scratch_dir),
        ("cache_dir", p.cache_dir),
        ("temp_dir", p.temp_dir),
    ]


@state.command("init")
@click.option('--config', default=None)
def state_init(config):
    """Create the fairway state and scratch dirs for this project.

    Idempotent — re-running never fails on existing dirs. Operators
    should run this once per project to pre-create the FAIRWAY_HOME
    tree so the first `fairway run` doesn't have to mkdir under a
    hot Lustre/GPFS write path.
    """
    from pathlib import Path
    cfg = Config(config or discover_config())
    for label, path in _state_dirs_for(cfg):
        path.mkdir(parents=True, exist_ok=True)
        click.echo(f"  {label}: {path}")
    click.echo(f"Initialized state for project {cfg.project!r}.")

    # Detect pre-FAIRWAY_HOME manifest dir. Check both the config's
    # directory (covers `state init --config /path/to/project/foo.yaml`
    # from anywhere) and CWD (covers legacy `cd project && state init`).
    # Silently leaving legacy manifests stranded would cause the next
    # run to re-ingest every file as new. Dedupe by resolved form so
    # the same real dir referenced two different ways warns only once.
    new_manifest = cfg.paths.manifest_dir
    try:
        new_resolved = new_manifest.resolve()
    except OSError:
        new_resolved = new_manifest
    seen = set()
    for raw in (Path(cfg.config_path).parent / "manifest", Path.cwd() / "manifest"):
        try:
            key = raw.resolve()
        except OSError:
            key = raw
        if key in seen or key == new_resolved:
            continue
        seen.add(key)
        if raw.is_dir() and any(raw.glob("*.json")):
            click.echo(
                f"\nWARNING: legacy manifest dir found at {raw}\n"
                f"  Active manifest dir is {new_manifest}\n"
                f"  Copy {raw}/*.json there to resume prior state;\n"
                f"  otherwise the next run will treat every file as unprocessed.",
                err=True,
            )


@state.command("path")
@click.option('--config', default=None)
def state_path(config):
    """Print resolved state/scratch paths for this project."""
    cfg = Config(config or discover_config())
    for label, path in _state_dirs_for(cfg):
        click.echo(f"{label}\t{path}")


@main.command()
@click.option('--config', default=None)
def doctor(config):
    """Diagnose fairway env: state roots, project paths, last run."""
    click.echo(f"FAIRWAY_HOME={os.environ.get('FAIRWAY_HOME') or '(unset — using platformdirs)'}")
    click.echo(f"FAIRWAY_SCRATCH={os.environ.get('FAIRWAY_SCRATCH') or '(unset — using platformdirs)'}")
    click.echo(f"FAIRWAY_RUN_ID={os.environ.get('FAIRWAY_RUN_ID') or '(unset — generated per-run)'}")

    try:
        cfg = Config(config or discover_config())
    except Exception as exc:
        click.echo(f"config:    ERROR — {exc}", err=True)
        sys.exit(1)

    click.echo(f"project:   {cfg.project}")
    click.echo(f"config:    {cfg.config_path}")
    click.echo("state dirs:")
    for label, path in _state_dirs_for(cfg):
        exists = "ok " if path.exists() else "miss"
        click.echo(f"  [{exists}] {label}: {path}")

    latest = _latest_log_file(str(cfg.paths.log_dir))
    if latest:
        click.echo(f"last run:  {latest}")
    else:
        click.echo("last run:  (no runs recorded under log_dir)")


@main.command()
@click.option('--config', default=None)
@click.option('--require-account', is_flag=True, default=False,
              help='Fail if slurm account is missing or a placeholder.')
def preflight(config, require_account):
    """Validate a config before submitting it — cheap sanity checks only.

    Runs validate_config_paths (all paths absolute, post-resolver),
    checks that each table root / preprocess script actually exists,
    and optionally refuses placeholder slurm accounts. Never writes.
    """
    from .config_loader import validate_config_paths
    cfg = Config(config or discover_config())
    errors = []

    try:
        validate_config_paths(cfg)
    except Exception as exc:
        errors.append(f"config paths: {exc}")

    for t in cfg.tables:
        root = getattr(t, "root", None)
        if root and not os.path.exists(root):
            errors.append(f"table {t.name!r}: root does not exist ({root})")
        pp = getattr(t, "preprocess", None) or {}
        action = pp.get("action")
        if action and not action.startswith(("python:", "module:")) and not os.path.exists(action):
            errors.append(f"table {t.name!r}: preprocess action missing ({action})")

    if require_account:
        account = cfg.resolve_resources().get("account")
        try:
            _validate_slurm_account(account)
        except click.ClickException as exc:
            errors.append(f"slurm: {exc.message}")
        if not account:
            errors.append("slurm: account is unset")

    if errors:
        for e in errors:
            click.echo(f"FAIL  {e}", err=True)
        click.echo(f"\npreflight failed with {len(errors)} problem(s).", err=True)
        sys.exit(1)
    click.echo(f"preflight OK — {cfg.project} ({cfg.config_path})")


if __name__ == '__main__':
    main()
