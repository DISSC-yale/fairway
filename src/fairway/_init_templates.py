"""Scaffold template strings rendered by ``fairway init``.

Pure string data — no runtime imports, no logic. Each constant is one
file the scaffolder writes into a fresh project or table dir.
"""
from __future__ import annotations


FAIRWAY_YAML_TEMPLATE = """\
# fairway.yaml — project defaults. Per-table override allowed in tables/<t>/config.yaml.

# Storage — output paths per layer (per-table-overrideable)
storage_root:      data
storage_processed: data/processed
storage_curated:   data/curated
scratch_dir:       null

# Encoding policy
encoding: utf-8
encoding_fallback: latin-1
allow_encoding_fallback: false

# Output
row_group_size: 122880

# Slurm
slurm_account: null
slurm_partition: null
slurm_chunk_size: 4000
slurm_concurrency: 64
slurm_mem: 64G
slurm_cpus_per_task: 8
slurm_time: "2:00:00"
"""


TABLE_CONFIG_YAML_TEMPLATE = """\
# tables/{name}/config.yaml — table-specific pipeline config.
# Folder name is the table name; no `dataset_name:` field needed.

source_glob: data/raw/{name}/*.csv
naming_pattern: '(?P<state>[A-Z]+)_(?P<year>\\d{{4}})\\.csv'
partition_by: [state, year]

# shard_by must be a strict prefix of partition_by (or [] for one shard).
# Defaults to partition_by (one shard per leaf).
# shard_by: [state]

source_format: delimited           # delimited | fixed_width
delimiter: ","
has_header: true
apply_type_a: false                # per-table only

# date_columns: {{}}
# unzip: false
# zip_inner_pattern: "*.csv|*.tsv|*.txt"
# zip_password_file: null
# sort_by: null
# slurm_mem: 128G                  # override fairway.yaml for this table
# slurm_time: "4:00:00"

validations:
  min_rows: 1
  expected_columns: {{columns: [], strict: false}}
"""


TABLE_SCHEMA_YAML_STUB = """\
# tables/{name}/schema.yaml — required output schema.
# Run `fairway discover {name}` to populate columns automatically,
# or edit this file by hand.

on_drift: strict                   # strict | lenient | per-axis dict
columns: []
"""


TABLE_TRANSFORM_PY_TEMPLATE = '''\
"""Optional transform hook for table `{name}`.

If this file is absent, the source view is passed through unchanged.
Delete this file if you don't need a custom transform.
"""
from __future__ import annotations


def transform(con, ctx):
    """Return a DuckDB relation. ``ctx.input_view`` is the source view."""
    return con.sql(f"SELECT * FROM {{ctx.input_view}}")
'''


GITIGNORE_TEMPLATE = """\
data/
build/
tables/*/manifest.json
tables/*/_state/
.venv/
__pycache__/
*.pyc
"""


README_TEMPLATE = """\
# {name}

fairway project (v0.3 layout). `fairway.yaml` holds project defaults;
`tables/<t>/` holds per-table config (`config.yaml`), required schema
(`schema.yaml`), and an optional `transform.py` hook. The per-table
`manifest.json` is the source of truth for idempotent re-runs.

Quickstart:

    cd {name}
    # add a new table
    fairway init . --table sales
    # populate tables/sales/schema.yaml from sample CSV files
    fairway discover sales
    # single-machine run (inline finalize):
    fairway run sales
    # Slurm array submission (separate finalize dependency job):
    fairway submit sales
    fairway status sales
"""
