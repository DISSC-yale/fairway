# Fairway Backend Codemap

> Freshness: 2026-03-20

## Source Tree

```
src/fairway/
├── __init__.py              # Package init
├── cli.py                   # Click CLI (1606 lines) — run, init, validate, schema, summarize
├── pipeline.py              # Core orchestrator (1171 lines) — IngestionPipeline class
├── config_loader.py         # YAML config parsing (472 lines) — Config, _expand_tables()
├── manifest.py              # File tracking (302 lines) — ManifestStore, TableManifest
├── batcher.py               # Partition batching (110 lines) — group_files_by_partition()
├── fixed_width.py           # Fixed-width parser (221 lines) — parse_layout, convert_to_csv
├── schema_pipeline.py       # Schema discovery (139 lines) — infer/merge schemas
├── summarize.py             # Stats generation (103 lines) — generate_summary()
├── templates.py             # Jinja template loader (43 lines)
├── logging_config.py        # Structured JSON logging (175 lines) — setup_logging()
├── generate_test_data.py    # Test data generator
├── engines/
│   ├── duckdb_engine.py     # DuckDB execution — DuckDBEngine.ingest()
│   ├── pyspark_engine.py    # PySpark/Delta execution — PySparkEngine.ingest()
│   └── slurm_cluster.py     # SLURM cluster management — SparkSlurmCluster
├── validations/
│   ├── __init__.py          # Package init
│   ├── result.py            # ValidationResult dataclass (39 lines) — findings + threshold logic
│   └── checks.py            # Data quality checks (495 lines) — Validator, run_all(), column-level
├── transformations/
│   ├── base.py              # Transformer interface — BaseTransformer ABC
│   └── registry.py          # Dynamic loader — TransformationRegistry
├── enrichments/
│   └── geospatial.py        # Geospatial enrichment (mock)
├── exporters/
│   └── redivis_exporter.py  # Redivis platform export
└── data/
    ├── scripts/             # Bundled helper scripts
    └── (config templates)   # YAML/Apptainer templates
```

## Core Classes

### IngestionPipeline (pipeline.py)
Central orchestrator. Owns engine lifecycle, manifest, and table iteration.
- `run()` — main entry, iterates tables
- `_preprocess()` — archive extraction, custom scripts
- `_run_bulk()` — standard single-pass ingestion
- `_run_partition_aware()` — batched by partition values
- `_check_skip()` — manifest-based skip logic

### Config (config_loader.py)
Loads and validates YAML configuration.
- `__init__(config_path)` — parse + validate
- `_expand_tables()` — glob expansion, regex metadata extraction
- `_validate_table()` — schema enforcement per table entry
- Per-table `validations` block support with type checking

### ManifestStore / TableManifest (manifest.py)
JSON-based file tracking with SHA-256 hashing.
- `ManifestStore.get_table()` → `TableManifest`
- `TableManifest.should_process(path)` — hash comparison
- `TableManifest.update_file(path, status)` — record outcome
- Format: `manifest/{table_name}.json` (v3.0)

### DuckDBEngine (engines/duckdb_engine.py)
Local SQL-based ingestion via DuckDB.
- `ingest(paths, table, output, **kwargs)` — read_csv_auto/read_json/read_parquet → COPY TO
- `_format_path_sql()` — safe path interpolation
- `_strip_fairway_kwargs()` — remove non-DuckDB params

### PySparkEngine (engines/pyspark_engine.py)
Distributed ingestion via PySpark + optional Delta Lake.
- `ingest()` — DataFrame API → write.parquet/delta
- Delta Lake support for ACID writes

### SparkSlurmCluster (engines/slurm_cluster.py)
SLURM HPC integration with Apptainer containerization.
- Cluster provisioning, executor auto-defaults
- `submit()`, `status()`, `cancel()`

## CLI Commands (cli.py)

| Command | Description |
|---------|-------------|
| `fairway run` | Execute ingestion pipeline |
| `fairway init` | Scaffold new project from template |
| `fairway validate` | Validate config without running |
| `fairway schema` | Discover/merge file schemas |
| `fairway summarize` | Generate dataset statistics |
| `fairway manifest` | Inspect/manage manifest state |

### Validator (validations/checks.py)
Data quality gate with column-level checks.
- `run_all(df, config)` — execute all configured checks, return ValidationResult
- `_normalize_validation_config()` — accept flat or legacy level1/level2 nesting
- Checks: `min_rows`, `max_rows`, `check_nulls`, `expected_columns`, `check_range`, `check_values`, `check_pattern`, `check_unique`, `check_custom`

### ValidationResult (validations/result.py)
Structured result of validation checks.
- `add_finding(finding)` — apply threshold-based severity downgrade
- Fields: `passed`, `errors`, `warnings`

## Extension Points

| Extension | Interface | Location |
|-----------|-----------|----------|
| Custom transforms | `BaseTransformer` ABC | transformations/base.py |
| Validation checks | `Validator.run_all()` → `ValidationResult` | validations/checks.py |
| Enrichments | Function-based | enrichments/ |
| Exporters | Module-based | exporters/ |
| Engines | Duck-typed `ingest()` | engines/ |

## Test Coverage

```
tests/
├── conftest.py                  # Fixtures: duckdb_engine, pyspark_engine, engine (parametrized)
├── test_pipeline_lifecycle.py   # Integration: full pipeline runs
├── test_pipeline_batching.py    # Partition-aware batching
├── test_pipeline_reliability.py # Error handling, retries
├── test_batcher.py              # Unit: batcher.py
├── test_manifest.py             # Unit: manifest.py
├── test_config_validation.py    # Config schema validation
├── test_config_path_resolution.py # Path resolution edge cases
├── test_fixed_width.py          # Fixed-width parsing
├── test_cli_*.py                # CLI command tests (6 files)
├── test_ingestion_formats.py    # CSV/JSON/Parquet formats
├── test_validation.py           # Data quality checks
├── test_validation_phase*.py    # Phased validation tests (4 files)
├── test_validation_review_fixes.py # Validation review fixes
├── test_transformation.py       # Custom transforms
├── test_schema_*.py             # Schema discovery/merge
├── test_summarize*.py           # Summary statistics
├── test_logging_config.py       # Logging setup
└── ... (42 test files total)
```

Run: `pytest tests/ -m "not spark"` for local-only tests.
