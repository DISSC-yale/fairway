"""Pin the sbatch template contract.

The run-abandon EXIT trap and the FAIRWAY_RUN_ID / FAIRWAY_RUN_MONTH
exports exist entirely in shell strings. Without these tests a future
template refactor could silently drop the OOM/walltime safety net.
"""
from __future__ import annotations

import shutil
import subprocess

import pytest

pytestmark = pytest.mark.hpc_contract


TEMPLATES = ["submit_bare.sh", "submit_with_spark.sh"]


def _render(template_name: str) -> str:
    import importlib.resources as _resources
    from fairway import slurm_templates as _slurm_templates
    template = _resources.files(_slurm_templates).joinpath(template_name).read_text()
    params = {
        "log_dir": "/tmp/log",
        "sif_env_var": "FAIRWAY_SIF",
        "default_sif": "fairway.sif",
        "slurm_time": "01:00:00",
        "mem": "16G",
        "cpus": "4",
        "partition": "day",
        "account": "test_acct",
        "apptainer_binds": "/tmp",
        "config": "/tmp/config.yaml",
        "fairway_home": "/tmp/state",
        "fairway_scratch": "/tmp/scratch",
        "spark_coordination_dir": "/tmp/state/projects/demo/spark",
        "spark_start_args": " --config /tmp/config.yaml --account test_acct --partition day --time 01:00:00 --cpus 4 --mem 16G --nodes 2",
        "summary_flag": "",
    }
    return template % params


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_exports_run_id(name):
    rendered = _render(name)
    assert 'export FAIRWAY_RUN_ID="${FAIRWAY_RUN_ID:-' in rendered, rendered


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_exports_run_month(name):
    rendered = _render(name)
    assert 'export FAIRWAY_RUN_MONTH="${FAIRWAY_RUN_MONTH:-' in rendered, rendered


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_installs_run_abandon_trap(name):
    rendered = _render(name)
    # submit_bare.sh traps EXIT directly; submit_with_spark.sh wraps
    # the call in a cleanup() function also registered for EXIT.
    assert "fairway run-abandon" in rendered, rendered
    assert "trap" in rendered, rendered
    assert "EXIT" in rendered, rendered


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_warns_when_fairway_missing_from_host_path(name):
    rendered = _render(name)
    assert 'command -v fairway' in rendered, rendered


def test_spark_template_forwards_run_id_and_month_to_apptainer():
    rendered = _render("submit_with_spark.sh")
    # Apptainer exec must carry the pre-computed ids through so the
    # containerized pipeline writes the same run.json the host trap
    # will look up.
    assert '--env FAIRWAY_RUN_ID=' in rendered, rendered
    assert '--env FAIRWAY_RUN_MONTH=' in rendered, rendered


@pytest.mark.parametrize("name", TEMPLATES)
def test_templates_export_fairway_state_roots(name):
    rendered = _render(name)
    assert 'export FAIRWAY_HOME="' in rendered, rendered
    assert 'export FAIRWAY_SCRATCH="' in rendered, rendered


def test_spark_template_uses_rendered_coordination_dir():
    rendered = _render("submit_with_spark.sh")
    assert 'STATE_DIR="/tmp/state/projects/demo/spark/$SLURM_JOB_ID"' in rendered, rendered
    assert '/.fairway-spark/' not in rendered, rendered


def test_spark_template_forwards_state_roots_to_apptainer():
    rendered = _render("submit_with_spark.sh")
    assert '--env FAIRWAY_HOME=' in rendered, rendered
    assert '--env FAIRWAY_SCRATCH=' in rendered, rendered


def test_spark_template_passes_config_and_resources_to_cluster_start():
    rendered = _render("submit_with_spark.sh")
    assert "fairway spark start --driver-job-id $SLURM_JOB_ID --config /tmp/config.yaml" in rendered, rendered
    assert "--account test_acct" in rendered, rendered
    assert "--partition day" in rendered, rendered
    assert "--cpus 4" in rendered, rendered
    assert "--mem 16G" in rendered, rendered


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
@pytest.mark.parametrize("name", TEMPLATES)
def test_template_bash_syntax_valid(name, tmp_path):
    rendered = _render(name)
    script = tmp_path / name
    script.write_text(rendered)
    result = subprocess.run(
        ["bash", "-n", str(script)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"bash -n failed: {result.stderr}"
