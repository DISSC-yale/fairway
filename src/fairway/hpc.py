import os
import shlex
import subprocess
import sys
import logging
import click
from fairway.apptainer import FAIRWAY_SIF_ENV_VAR, DEFAULT_SIF_NAME

logger = logging.getLogger("fairway.hpc")

class SlurmManager:
    """Handles Slurm job submission and template rendering."""
    
    def __init__(self, config):
        self.config = config

    def _render_template(self, name, **params):
        """Load a .sh template from fairway.slurm_templates and %-substitute params."""
        import importlib.resources as _resources
        from fairway import slurm_templates as _slurm_templates
        template = _resources.files(_slurm_templates).joinpath(name).read_text()
        return template % params

    def _spark_start_args(self, resources):
        tokens = ["--config", self.config.config_path]
        for flag, key in (
            ("--account", "account"),
            ("--partition", "partition"),
            ("--time", "time"),
            ("--cpus", "cpus"),
            ("--mem", "mem"),
            ("--nodes", "nodes"),
        ):
            value = resources.get(key)
            if value is not None:
                tokens.extend([flag, str(value)])
        return f" {shlex.join(tokens)}"

    def build_template_params(self, extra_params=None):
        resources = self.config.resolve_resources()
        params = {
            'log_dir': str(self.config.paths.slurm_log_dir),
            'sif_env_var': FAIRWAY_SIF_ENV_VAR,
            'default_sif': DEFAULT_SIF_NAME,
            'slurm_time': resources['time'],
            'mem': resources['mem'],
            'cpus': resources['cpus'],
            'partition': resources['partition'],
            'account': resources['account'],
            'apptainer_binds': self.config.binds_list,
            'config': self.config.config_path,
            'fairway_home': str(self.config.paths.state_root),
            'fairway_scratch': str(self.config.paths.scratch_root),
            'spark_coordination_dir': str(self.config.paths.spark_coordination_dir),
            'spark_start_args': self._spark_start_args(resources),
        }
        if extra_params:
            params.update(extra_params)
        return params

    def submit_job(self, template_name, submit_message, extra_params=None):
        """Render a template and submit it as a Slurm job."""
        # Slurm stdout/stderr lands in slurm_log_dir (flat sibling of
        # the structured log_dir). Resolved once per Config, so every
        # submission from one project shares the same directory.
        log_dir = str(self.config.paths.slurm_log_dir)
        os.makedirs(log_dir, exist_ok=True)
        params = self.build_template_params(extra_params)
        job_script = self._render_template(template_name, **params)
        
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(job_script)
            script_path = f.name

        try:
            click.echo(submit_message)
            result = subprocess.run(['sbatch', script_path], capture_output=True, text=True)
            if result.returncode == 0:
                click.echo(result.stdout.strip())
                click.echo("Job submitted. Check status with 'fairway status'.")
                return True
            else:
                click.echo(f"Error submitting job: {result.stderr}", err=True)
                sys.exit(1)
        except FileNotFoundError:
            click.echo("Error: 'sbatch' command not found. Are you on a system with Slurm?", err=True)
            sys.exit(1)
        finally:
            os.unlink(script_path)
        return False
