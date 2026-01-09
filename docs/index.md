# Fairway Data Ingestion

**fairway** is a portable, scalable data ingestion framework designed for sustainable management of centralized research data. 

## Core Philosophy

Traditional data ingestion often suffers from undocumented transformations, rigid pipelines, and difficult-to-scale infrastructure. **fairway** addresses these pain points by being:

*   **Config-Driven**: Define your pipeline in YAML, not just code.
*   **Engine-Agnostic**: Shift from local DuckDB processing to distributed PySpark on Slurm with a single config change.
*   **Orchestration-Native**: Built to run seamlessly with Nextflow and Slurm.
*   **Validation-First**: Multi-level sanity and distribution checks are baked into the pipeline.

## Where to Start?

*   [**Getting Started**](getting-started.md): Install fairway and run your first pipeline.
*   [**Architecture**](architecture.md): Understand the underlying pipeline lifecycle and tech stack.
*   [**Configuration Guide**](configuration.md): Learn how to define sources, metadata, and validations.
*   [**Custom Transformations**](transformations.md): Extend the pipeline with your own processing logic.
