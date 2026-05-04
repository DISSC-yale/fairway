# Fairway

Fairway is an internal data-engineering tool for the DISSC group, used to land
large research datasets onto a shared filesystem (Yale Grace) as partitioned
Parquet, in a form that downstream researchers can query directly with DuckDB,
R, Stata, or pandas.

Scope, deliberately narrow (v0.3):

- DuckDB-only execution. No Spark, no Delta Lake, no Nextflow, no container.
- One Slurm job-array shard per partition group. Shuffle-free.
- Per-shard manifest fragments → finalized into a single `manifest.json` plus a
  `schema_summary.json` fingerprint per partition.
- Two-stage layout: raw → processed (ingest) → curated (transform).

## Install

```bash
pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[duckdb]"
```

Requires Python 3.10+. The `[duckdb]` extra pulls `duckdb`, `pyarrow`, `numpy`.

## Project layout

`fairway init` scaffolds:

```
my_project/
├── datasets/
│   └── mydataset.yaml      # dataset config (source glob, partition_by, validations)
│   └── mydataset.py        # transform(con, ctx) entry point
├── transforms/
│   ├── raw_to_processed/
│   └── processed_to_curated/
├── data/                   # gitignored output root
└── build/                  # gitignored sbatch / shards artifacts
```

One YAML + one Python file per dataset. The Python file exposes a single
`transform(con, ctx)` function that receives a DuckDB connection and an
`IngestCtx` (input paths, output path, partition values, shard id, scratch
dir). The default implementation is `fairway.defaults.default_ingest`.

## CLI surface

```bash
fairway init my_project --dataset mydataset            # scaffold project + dataset
fairway submit datasets/mydataset.yaml                 # submit Slurm array
fairway submit datasets/mydataset.yaml --dry-run       # render shards.json + sbatch only
fairway submit datasets/mydataset.yaml --allow-skip    # skip files not matching naming_pattern
fairway submit datasets/mydataset.yaml --force         # ignore idempotent-resume cache
fairway run --shards-file build/<dataset>/shards.json --shard-index 0   # one shard
fairway status --dataset mydataset --storage-root data --layer processed
fairway transform transforms/processed_to_curated/<recipe>.yaml   # Stage 2
fairway validate <dataset_path> <rules.yaml>           # standalone validation
fairway summarize <dataset_path> <output.csv>          # column summary CSV
fairway manifest finalize <layer_root>                 # merge fragments → manifest.json
```

## Quick start

```bash
fairway init demo --dataset sales
# edit datasets/sales.yaml: set source_glob, naming_pattern, partition_by
fairway submit datasets/sales.yaml --dry-run    # confirm shard count
fairway submit datasets/sales.yaml              # actually submit on a Slurm host
fairway status --dataset sales --storage-root data --layer processed
fairway manifest finalize data/processed/sales
```

Required dataset YAML keys: `dataset_name`, `python`, `storage_root`,
`source_glob`, `naming_pattern`, `partition_by`. See the scaffolded
`datasets/<name>.yaml` for optional knobs (validations, encoding fallback,
zip handling, sort, Slurm resource overrides).

## Querying landed data (researchers)

Output is plain Hive-partitioned Parquet under `<storage_root>/<layer>/<dataset>/`.
No special client library is needed.

```python
import duckdb
duckdb.sql("SELECT * FROM read_parquet('data/processed/sales/**/*.parquet', hive_partitioning=true) LIMIT 10")
```

```r
library(arrow)
ds <- open_dataset("data/processed/sales", partitioning = c("state", "year"))
```
