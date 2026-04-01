# Fairway Architecture Codemap

> Freshness: 2026-03-20

## Overview

Config-driven data ingestion pipeline: YAML config → Python orchestration → DuckDB/PySpark output. Converts raw files (CSV, JSON, Parquet, fixed-width) into analytics-ready Parquet with validation, transformation, and enrichment stages.

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      CLI (cli.py)                       │
│  click commands: run, init, validate, schema, summarize │
│  SLURM integration, dry-run mode                        │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│               Config Loader (config_loader.py)          │
│  YAML parsing, glob expansion, metadata extraction      │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              Pipeline (pipeline.py)                      │
│  Orchestrates: preprocess → ingest → validate →         │
│  transform → enrich → export                            │
│  Supports bulk and partition-aware batch strategies      │
├──────────┬───────────────────────────────┬──────────────┤
│          │                               │              │
│  ┌───────▼───────┐            ┌──────────▼──────────┐   │
│  │ DuckDB Engine │            │  PySpark Engine     │   │
│  │ (local SQL)   │            │  (distributed/Delta)│   │
│  └───────────────┘            └─────────────────────┘   │
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Manifest    │  │ Batcher      │  │ Fixed-Width   │  │
│  │ (tracking)  │  │ (partitions) │  │ (format)      │  │
│  └─────────────┘  └──────────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────┘
         │                │                │
┌────────▼───┐  ┌────────▼─────┐  ┌───────▼──────────┐
│ Validations│  │Transformations│  │ Enrichments      │
│ checks.py  │  │ registry.py  │  │ geospatial.py    │
│ result.py  │  │              │  │                  │
└────────────┘  └──────────────┘  └──────────────────┘
                                          │
                               ┌──────────▼──────────┐
                               │ Exporters           │
                               │ redivis_exporter.py │
                               └─────────────────────┘
```

## Module Dependency Graph

```
cli.py
 ├── config_loader.py
 ├── pipeline.py
 ├── manifest.py
 ├── schema_pipeline.py
 ├── summarize.py
 ├── templates.py
 ├── logging_config.py
 └── engines/slurm_cluster.py

pipeline.py
 ├── config_loader.py
 ├── manifest.py
 ├── batcher.py
 ├── fixed_width.py
 ├── logging_config.py
 ├── summarize.py
 ├── engines/duckdb_engine.py
 ├── engines/pyspark_engine.py
 ├── validations/checks.py (→ validations/result.py)
 ├── transformations/registry.py
 ├── enrichments/geospatial.py
 └── exporters/redivis_exporter.py

config_loader.py
 └── (stdlib only: yaml, glob, re, pathlib)

manifest.py
 └── (stdlib only: json, hashlib, pathlib)

batcher.py
 └── (stdlib only: re, collections)
```

## Execution Flow

```
fairway run --config fairway.yaml
  → CLI parses args
  → ConfigLoader.load() validates YAML, expands globs
  → IngestionPipeline(config) initializes engine + manifest
  → For each table:
      → _preprocess() — unzip/custom scripts (cached via manifest)
      → _check_skip() — manifest hash comparison
      → if partition_aware: _run_partition_aware() via batcher.py
        else: _run_bulk()
      → Engine.ingest() — format-specific read → Parquet write
      → Validator.run_all() — column-level quality checks (ValidationResult)
      → Transformer.apply() — user-defined transforms
      → Enricher.enrich() — column additions
  → Summarizer.generate() — statistics
  → Exporter.upload() — optional Redivis push
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Dual engine (DuckDB/PySpark) | Local dev speed + cluster scalability |
| File-level manifest tracking | Skip unchanged files, retry failed ones |
| Partition-aware batching | Hive-style output for efficient querying |
| Config-driven (no code) | Non-programmers can define pipelines |
| Engine kwargs stripping | DuckDB rejects unknown params; fairway-specific keys filtered before SQL |
| Defense-in-depth security | Path validation → identifier validation → SQL escaping |
| ValidationResult model | Structured findings with threshold-based severity downgrade |
| Per-table validations | Each table defines its own checks in YAML config |
