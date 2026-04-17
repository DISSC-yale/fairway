"""Apptainer/container identifiers used across cli.py, hpc.py, and slurm_cluster.py.

These are the lone magic strings that would break SIF discovery if they
drifted, so they live in one place. Everything else container-related
(Dockerfile text, Apptainer.def text, bind computation) lives in
`templates.py` or the CLI.
"""

FAIRWAY_SIF_ENV_VAR = "FAIRWAY_SIF"
DEFAULT_SIF_NAME = "fairway.sif"
