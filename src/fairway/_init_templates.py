"""String templates rendered by ``fairway init`` — kept out of cli.py to
hold its LOC budget (≤ 250 for Step 9.1, ≤ 280 after 9.5)."""
from __future__ import annotations

# Per-dataset YAML scaffold. ``str.format`` placeholder is ``{name}``;
# YAML braces (``{}``) are doubled so .format() leaves them literal.
DATASET_YAML = """\
# fairway dataset config — Required fields uncommented; optionals show defaults.
dataset_name: {name}
python: datasets/{name}.py
storage_root: data
source_glob: data/raw/{name}/*.csv
naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{{4}})\\.csv'
partition_by: [state, year]

# storage_raw / storage_processed / storage_curated: <path>   # layer overrides
# layer: raw                                                  # raw|processed|curated
# source_format: delimited        # delimited | fixed_width
# delimiter: "\\t"
# has_header: true
# encoding: utf-8
# encoding_fallback: latin-1
# allow_encoding_fallback: false
# date_columns: {{}}
# apply_type_a: true
# unzip: false
# zip_inner_pattern: "*.csv|*.tsv|*.txt"
# zip_password_file: null
# validations:
#   min_rows: 1
#   max_rows: null
#   check_nulls: []
#   expected_columns: {{columns: [], strict: false}}
#   check_range: {{}}
#   check_values: {{}}
#   check_pattern: {{}}
# sort_by: null
# row_group_size: 122880
# slurm_account: null
# slurm_partition: null
# slurm_chunk_size: 4000
# slurm_concurrency: 64
# slurm_mem: 64G
# slurm_cpus_per_task: 8
# slurm_time: "2:00:00"
# scratch_dir: null
"""

DATASET_PY = '''\
"""Ingest transform for {name}.

Edit ``transform`` to customize. Default is ``default_ingest(con, ctx)``.
See :mod:`fairway.defaults` for ``default_ingest_read_only``, ``apply_type_a``.
"""
from fairway.defaults import default_ingest


def transform(con, ctx):
    return default_ingest(con, ctx)
'''

GITIGNORE = "data/\nbuild/\n.venv/\n__pycache__/\n*.pyc\n"

README = """\
# {name}

fairway project (v0.3 layout). ``datasets/<name>.{{yaml,py}}`` per dataset;
``transforms/{{raw_to_processed,processed_to_curated}}/`` for Stage 2;
``data/`` and ``build/`` are gitignored.

Quickstart::

    fairway init {name} --dataset mydataset
    fairway submit datasets/mydataset.yaml
    fairway status --dataset mydataset
"""
