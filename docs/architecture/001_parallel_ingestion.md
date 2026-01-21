# Architecture Decision 001: Parallel Ingestion Strategies

**Date**: 2026-01-21
**Status**: Adopted

## Context
Fairway ingests data that often requires preprocessing (e.g., unzipping, decrypting, parsing) before the underlying compute engine (Spark/DuckDB) can read it.
When dealing with thousands of small files (e.g., daily zipped CSVs), the preprocessing step becomes a bottleneck if run sequentially on the Driver node.

## Considered Strategies

### 1. Driver-Side Loop (Default)
- **Mechanism**: The Fairway process loops through files and processes them one by one.
- **Pros**: Simple, easy debugging. Works with local engines (DuckDB).
- **Cons**: Slow for large numbers of files. Single point of failure.

### 2. Orchestrator Fan-Out (Nextflow)
- **Mechanism**: Use Nextflow to spawn one `fairway ingest` process per file.
- **Pros**: Infinite horizontal scaling. Fault isolation.
- **Cons**: High overhead ("Driver Storm"). Each process needs its own JVM/Memory. Danger of exhausting head-node resources.

### 3. Distributed Preprocessing (Spark Push-Down)
- **Mechanism**: Use the *existing* Spark Driver to dispatch Python tasks to the cluster executors via `sc.parallelize().map()`.
- **Pros**: Efficient (Single Driver). Uses already-provisioned cluster resources. "Batteries Included".
- **Cons**: Requires Python environment consistency across workers.

## Decision: Distributed Preprocessing
We chose **Distributed Preprocessing** as the scalable default for Spark-based ingestion. It balances performance with simplicity.

### Custom Scripts
Users can provide a path to a Python script in the `preprocess.action` field.
- **Contract**: The script must define a `process(file_path) -> str` function.
- **Execution**: Fairway loads this function and dispatches it (locally or to the cluster).

### Environment Requirements
For `cluster` execution, the environment (including the custom script's dependencies) must be consistent across workers.
- **Slurm**: Shared Conda env.
- **Cloud**: Docker/Bootstrap.

## Usage
Set `execution_mode: cluster` in the preprocessing config. This is only available when `engine: pyspark`.
