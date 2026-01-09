# Architecture

**fairway** is designed as a modular pipeline that abstracts away the complexities of distributed computing and data orchestration.

## High-Level Workflow

The pipeline operates in three distinct phases:

### Phase I: Discovery & Triggering
1.  **File Discovery**: fairway scans the landing zone defined in the config.
2.  **Manifest Check**: It compares incoming files against the `fmanifest` to skip files that have already been processed with the same hash.

### Phase II: Raw Processing
The goal of this phase is to convert vendor data into a queryable, verifiable format without altering the original schema.

1.  **Validation (Level 1)**: Basic sanity checks (e.g., column counts).
2.  **Ingestion**: Conversion of source formats (CSV, etc.) to Parquet, optimized with partitioning and "salting" to prevent data skew in Spark.
3.  **Enrichment**: Optional geospatial enrichment (geocoding, H3 indexing).
4.  **Validation (Level 2)**: Content-level checks (e.g., null spikes, schema adherence).
5.  **Summarization**: Automatic generation of summary statistics and markdown reports.

### Phase III: Transformation
This phase reshapes the data for specific analytical needs.

1.  **Custom Logic**: Execution of project-specific Python transformations.
2.  **Lineage**: Manifest updates that link transformed products back to their raw sources.

## Tech Stack

### Orchestration
*   **Nextflow**: Manages the dependency graph and executes tasks in parallel.
*   **Slurm**: Handles resource allocation and job scheduling on HPC clusters.

### Compute Engines
*   **DuckDB**: Used for local, single-node processing. Exceptionally fast for smaller-than-memory datasets.
*   **PySpark**: Leveraged for large-scale distributed processing on Slurm clusters.

### Storage
*   **Parquet**: The primary storage format. It provides efficient columnar storage and is natively supported by both DuckDB and Spark.
*   **fmanifest**: A JSON-based manifest system that tracks file hashes, processing status, and metadata.
