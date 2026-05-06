"""Packaged Slurm job-script templates rendered by fairway.cli._submit_slurm_job.

Templates use Python %%-formatting with named placeholders (e.g. `%(config)s`)
so bash's `${...}` / `$(...)` syntax is never escaped. Literal `%` characters
required by sbatch (e.g. `%j` in `--output=...log_%j`) are written as `%%`.
"""
