# Nextflow Fan-Out/Fan-In Implementation Plan

## Overview

Transform Nextflow from a thin wrapper into an actual orchestrator that handles parallel batch processing for large heterogeneous datasets.

**Branch:** `explore/orchestration-submission`

---

## Problem

**Current:** One Nextflow process → One `fairway run` → One Spark job → ALL files → Crash

**Needed:** Many parallel jobs processing file batches with fan-out/fan-in patterns

---

## Design Decision

**Hybrid approach:**
- Fairway provides reusable Nextflow process definitions
- `fairway init` generates working workflow template
- Config controls parameters (batch size, transforms)
- Users CAN customize `main.nf` for advanced cases

---

## Current vs Proposed

| Aspect | Current | Proposed |
|--------|---------|----------|
| Jobs submitted | 1 | 10-50+ (batched) |
| Failure scope | All or nothing | Per-batch retry |
| Memory per job | Huge (all data) | Small (batch only) |
| Nextflow role | Wrapper | Actual orchestrator |

---

## Implementation Phases

### Phase 1: Worker CLI Commands

New batch-aware commands for Nextflow to call.

**File:** `src/fairway/cli.py`

```bash
# File discovery
fairway files --table TABLE --count
fairway files --table TABLE --batch N

# Batch calculation (encapsulates batch count logic)
fairway batches --table TABLE --size 100    # Outputs: 10

# Schema operations
fairway schema-scan --table TABLE --batch N
fairway schema-merge --table TABLE

# Ingestion
fairway ingest --table TABLE --batch N

# Transforms (explicit commands, no ambiguity)
fairway transform-batch --table TABLE --name NAME --batch N --spark-master URL
fairway transform-fanin --table TABLE --name NAME --spark-master URL

# Spark cluster lifecycle (existing commands, reads config/spark.yaml)
# Called by driver.sh BEFORE/AFTER Nextflow, not inside workflow
fairway spark start                    # Starts cluster, writes master URL to file
fairway spark stop                     # Stops cluster

# Finalization
fairway finalize --table TABLE         # Also stops Spark cluster if running
```

**Design Decision:** Transform commands are split into explicit `transform-batch` and `transform-fanin` to avoid ambiguous optional `--batch` parameter.

### Phase 2: Batch Processing Module

Core logic for batch operations.

**File:** `src/fairway/batch_processor.py`

```python
class BatchProcessor:
    def __init__(self, config_path: str, table: str, batch_size: int = 100):
        ...

    def get_file_count(self) -> int:
        """Total files for table."""

    def get_batch_count(self) -> int:
        """Calculate number of batches needed. Encapsulates ceiling division."""
        return math.ceil(self.get_file_count() / self.batch_size)

    def get_files_for_batch(self, batch: int) -> list[str]:
        """Files for specific batch."""

    def scan_schema(self, batch: int) -> Path:
        """Scan batch, write schema_{batch}.json."""

    def merge_schemas(self) -> Path:
        """Merge all partial schemas."""

    def ingest_batch(self, batch: int) -> Path:
        """Ingest batch with unified schema."""

    def transform_batch(self, name: str, batch: int) -> Path:
        """Run parallel transform on single batch."""

    def transform_fanin(self, name: str) -> Path:
        """Run fan-in transform across all batches."""

    def finalize(self):
        """Validate, export, cleanup. Reconciles Nextflow cache with manifest."""
```

### Phase 3: Nextflow Process Definitions

Reusable process library. Uses `$FAIRWAY_NF_LIB` for robust include paths.

**File:** `src/fairway/data/fairway_processes.nf`

```groovy
// NOTE: Spark cluster is started/stopped OUTSIDE Nextflow by driver.sh
// Spark master URL is passed via --spark-master parameter

process SCHEMA_SCAN {
    tag "batch_${batch_id}"
    input: val batch_id
    output: path "schema_${batch_id}.json"
    script:
    """
    fairway schema-scan --table ${params.table} --batch ${batch_id}
    """
}

process SCHEMA_MERGE {
    input: path schemas
    output: path "unified_schema.json"
    script:
    """
    fairway schema-merge --table ${params.table}
    """
}

process INGEST {
    tag "batch_${batch_id}"
    input: val batch_id; path schema; val spark_master
    output: path "partition_${batch_id}.parquet"
    script:
    """
    fairway ingest --table ${params.table} --batch ${batch_id} \
        --spark-master "${spark_master}"
    """
}

process TRANSFORM_BATCH {
    tag "${transform_name}_${batch_id}"
    input: tuple val(batch_id), path(partition); val transform_name; val spark_master
    output: tuple val(batch_id), path("${transform_name}_${batch_id}.parquet")
    script:
    """
    fairway transform-batch --table ${params.table} --name ${transform_name} \
        --batch ${batch_id} --spark-master "${spark_master}"
    """
}

process TRANSFORM_FANIN {
    input: path partitions; val transform_name; val spark_master
    output: path "${transform_name}_out/*.parquet"
    script:
    """
    fairway transform-fanin --table ${params.table} --name ${transform_name} \
        --spark-master "${spark_master}"
    """
}

process FINALIZE {
    input: path partitions
    script:
    """
    fairway finalize --table ${params.table}
    # Stops Spark cluster if spark_mode=cluster
    """
}
```

### Phase 4: Main Workflow Template

**File:** `src/fairway/data/main.nf`

```groovy
nextflow.enable.dsl=2

// Robust include path: uses FAIRWAY_NF_LIB env var or falls back to relative path
def nf_lib = System.getenv('FAIRWAY_NF_LIB') ?: "${workflow.projectDir}"
include { SCHEMA_SCAN; SCHEMA_MERGE; INGEST;
          TRANSFORM_BATCH; TRANSFORM_FANIN; FINALIZE } from "${nf_lib}/fairway_processes.nf"

// Parameters - config is single source of truth, these are runtime overrides
params.table = null
params.config = 'config/fairway.yaml'
params.n_batches = null       // If null, calculated dynamically via fairway batches
params.spark_master = null    // Passed by driver.sh after starting cluster

workflow {
    // Calculate batch count dynamically if not provided
    if (params.n_batches == null) {
        n_batches = "fairway batches --table ${params.table} --config ${params.config}".execute().text.trim().toInteger()
    } else {
        n_batches = params.n_batches
    }

    // Spark master URL (passed by driver.sh, defaults to local for testing)
    spark_master = params.spark_master ?: "local[*]"

    // Phase 1: Schema scan (fan-out) - no Spark needed
    batches = Channel.of(0..<n_batches)
    schemas = SCHEMA_SCAN(batches)

    // Phase 2: Schema merge (fan-in) - no Spark needed
    unified = SCHEMA_MERGE(schemas.collect())

    // Phase 3: Ingest (fan-out) - all batches submit to shared cluster
    partitions = INGEST(batches, unified, spark_master)

    // Phase 4: Transforms - customize this section
    result = partitions.map { b -> tuple(b, file("partition_${b}.parquet")) }

    // Example: parallel transform (submits to shared cluster)
    // result = TRANSFORM_BATCH(result, 'standardize', spark_master)

    // Example: fan-in transform (submits to shared cluster)
    // collected = result.map { it[1] }.collect()
    // result = TRANSFORM_FANIN(collected, 'deduplicate', spark_master)

    // Phase 5: Finalize
    FINALIZE(result.map { it[1] }.collect())
}
```

**Design Decisions:**
- **Include path resilience:** Uses `FAIRWAY_NF_LIB` env var with fallback to `workflow.projectDir`
- **Single source of truth:** `batch_size` comes from config only; `n_batches` calculated dynamically
- **Spark mode:** Explicit parameter controls cluster coordination strategy

### Phase 5: Config Schema Updates

**File:** `src/fairway/config_loader.py`

```yaml
# New/updated config sections in fairway.yaml

orchestration:
  batch_size: 100              # Single source of truth for batch sizing
  work_dir: .fairway/work

tables:
  # Table A: Raw CSV → Parquet (wide format)
  - name: wide_claims
    path: "data/raw/**/*.csv"    # Reads from raw files
    format: csv

    validation:
      min_rows: 1
      check_nulls: [claim_id]

    performance:
      target_rows: 500000

    transforms:
      - name: standardize
        script: src/transforms/standardize.py
        parallel: true

  # Table B: Reads from Table A, reshapes wide → long
  - name: long_claims
    source: wide_claims          # Reads from another table's output

    transforms:
      - name: reshape_to_long
        script: src/transforms/reshape.py
        parallel: false          # Source tables start with fan-in

  # Table C: Further processing
  - name: final_claims
    source: long_claims

    transforms:
      - name: deduplicate
        script: src/transforms/deduplicate.py
        parallel: false
        sort_by: [claim_date]
        repartition: 10
```

**Table input types:**
| Field | Meaning |
|-------|---------|
| `path:` | Read from raw files (batched by file count) |
| `source:` | Read from another table's output (starts as fan-in, reads all parquet) |

**Note:** `path:` + `source:` combination (join raw files with derived table) is a future consideration.

**Config Migration:** `validations` and `performance` sections move under each table.

**Spark Configuration**

Spark config lives in `config/spark.yaml` (existing file, not referenced in fairway.yaml).

**File:** `config/spark.yaml` (already exists in template)

```yaml
# Spark cluster configuration for HPC
nodes: 2
cpus_per_node: 32
mem_per_node: "200G"

# Slurm-specific
account: "myaccount"
partition: "day"
time: "24:00:00"

# Spark dynamic allocation
dynamic_allocation:
  enabled: true
  min_executors: 5
  max_executors: 150
```

**Spark Lifecycle (managed by driver.sh, outside Nextflow):**

```
driver.sh:
├── fairway spark start          # Starts cluster, writes master URL
├── nextflow run main.nf --spark-master $SPARK_MASTER
│   └── All batch jobs submit to shared cluster
└── fairway spark stop           # Cleanup (via trap)
```

**Local testing:** Skip `make submit-hpc`, use `make run` → no cluster started, batches use `local[*]`

### Phase 6: Transform Interface

**File:** `src/fairway/transformations/base.py`

```python
class BatchTransformer:
    """
    Base class for batch-aware transforms.

    Engine-agnostic: supports both Pandas (DuckDB) and Spark DataFrames.
    See RULE-104: Engine Agnostic Components.
    """

    # Subclass overrides
    parallel: bool = True
    sort_by: list[str] | None = None
    repartition: int | None = None

    def transform(self, df) -> "DataFrame":
        """
        In-memory transform logic. Override in subclass.

        Args:
            df: Pandas DataFrame or Spark DataFrame (engine-agnostic)
        Returns:
            Transformed DataFrame (same type as input)
        """
        raise NotImplementedError

    def transform_batch(self, input_path: str, output_path: str, engine: str = "spark") -> None:
        """
        File-based transform for batch processing.

        Uses native engine I/O to avoid memory issues (RULE-103).
        - Spark: spark.read.parquet() / df.write.parquet()
        - DuckDB: Native parquet read/write
        """
        if engine == "spark":
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet(input_path)
            result = self.transform(df)
            result.write.mode("overwrite").parquet(output_path)
        else:
            import pandas as pd
            df = pd.read_parquet(input_path)
            result = self.transform(df)
            result.to_parquet(output_path)
```

### Phase 7: Init Command Updates

Update `fairway init` to generate orchestration-ready project.

**Files:** `src/fairway/cli.py`, `src/fairway/templates.py`

---

### Phase 8: Manifest Concurrency (Append-Only Strategy)

**Problem:** Parallel workers writing to shared manifest creates race conditions.

**Solution:** Append-only partial status files with merge on finalize.

```
.fairway/work/
  batch_0/
    status.jsonl    # Append-only log
  batch_1/
    status.jsonl
  ...
```

**Status file format (JSONL):**
```json
{"file": "data/raw/file_001.csv", "status": "success", "hash": "abc123", "ts": 1706900000}
{"file": "data/raw/file_002.csv", "status": "success", "hash": "def456", "ts": 1706900001}
```

**Conflict Resolution (in `finalize`):**
- Same file, same status → keep most recent
- Same file, different status → last-write-wins by timestamp
- Log conflicts for user review

**File:** `src/fairway/manifest.py` - Add `PartialStatus` class and merge logic

---

### Phase 9: Workflow Templates

Provide tiered templates to reduce Groovy complexity barrier.

**Files:**
- `src/fairway/data/workflows/simple.nf` - Fan-out ingest + finalize only
- `src/fairway/data/workflows/with_transforms.nf` - Adds parallel transform support
- `src/fairway/data/workflows/advanced.nf` - Full customization with documented insertion points

```bash
fairway init myproject --template simple        # Default
fairway init myproject --template transforms    # With transform chain
fairway init myproject --template advanced      # Full control
```

---

## File Changes Summary

| File | Action |
|------|--------|
| `src/fairway/cli.py` | Add batch commands (`batches`, `transform-batch`, `transform-fanin`) |
| `src/fairway/batch_processor.py` | Create |
| `src/fairway/data/fairway_processes.nf` | Create |
| `src/fairway/data/main.nf` | Replace (dynamic batch calc, spark_mode, robust includes) |
| `src/fairway/data/nextflow.config` | Update (add spark_mode profiles) |
| `src/fairway/config_loader.py` | Add orchestration section |
| `src/fairway/transformations/base.py` | Replace with `BatchTransformer` class |
| `src/fairway/manifest.py` | Add `PartialStatus` class and merge logic |
| `src/fairway/data/workflows/simple.nf` | Create (basic template) |
| `src/fairway/data/workflows/with_transforms.nf` | Create (transform template) |
| `src/fairway/data/workflows/advanced.nf` | Create (full control template) |
| `src/fairway/data/Makefile` | Update (add orchestration targets) |
| `tests/test_batch_processor.py` | Create |

---

## Execution Flow

```
User runs:
$ make submit-hpc TABLE=final_claims

driver.sh (SLURM job):
├── fairway spark start
│
├── nextflow run main.nf --table final_claims --spark-master spark://node01:7077
│   │
│   │  # Table DAG resolved: wide_claims → long_claims → final_claims
│   │
│   │  === wide_claims (path: data/raw/*.csv) ===
│   ├── 10x SCHEMA_SCAN jobs (parallel)
│   ├── 1x SCHEMA_MERGE job
│   ├── 10x INGEST jobs (parallel)
│   ├── 10x transform: standardize (parallel)
│   │
│   │  === long_claims (source: wide_claims) ===
│   ├── 1x transform: reshape_to_long (fan-in, reads all wide_claims output)
│   │
│   │  === final_claims (source: long_claims) ===
│   ├── 1x transform: deduplicate (fan-in)
│   ├── 1x FINALIZE job
│   │
└── fairway spark stop
```

**Table dependency resolution:**
- `--table X` triggers X and all upstream `source:` dependencies
- Tables with `path:` are roots (no dependencies)
- Tables with `source:` wait for source table to complete

**Data flow (file locations):**
```
data/raw/*.csv
    │
    ▼ [wide_claims: ingest]
.fairway/work/wide_claims/
    ingested/batch_*.parquet
    │
    ▼ [wide_claims: standardize] (parallel)
    standardize/batch_*.parquet     ← long_claims reads from here
    │
    ▼ [long_claims: reshape_to_long] (fan-in)
.fairway/work/long_claims/
    reshape_to_long/part_*.parquet  ← final_claims reads from here
    │
    ▼ [final_claims: deduplicate] (fan-in)
.fairway/work/final_claims/
    deduplicate/part_*.parquet
    │
    ▼ [finalize]
data/final/final_claims/
    part_*.parquet
```

**Local testing:** `make run TABLE=final_claims` → no cluster, batches use `local[*]`

---

## Resolved Decisions

1. **Parallelism declaration:** Both (code declares default via `BatchTransformer.parallel`, config overrides)

2. **Work directory:** `.fairway/work/` (predictable, inspectable)

3. **Repartition strategy:** Transform writes N files directly

4. **Batch calculation:** Encapsulated in `fairway batches` command; Nextflow calculates dynamically

5. **Spark coordination:** Cluster started/stopped by driver.sh (outside Nextflow); master URL passed to Nextflow; local mode for testing

6. **Transform commands:** Split into explicit `transform-batch` and `transform-fanin` (no ambiguity)

7. **Include paths:** Uses `FAIRWAY_NF_LIB` env var with fallback to `workflow.projectDir`

8. **Parameter source of truth:** Config owns `batch_size`; Nextflow params are runtime overrides only

9. **Table-specific config:** `validation` and `performance` move under each table definition (not global sections)

---

## Testing Strategy

1. Unit tests for `batch_processor.py`
2. Integration test with 100-file synthetic dataset
3. E2E test: parallel → fan-in → parallel transform chain
4. SLURM test (manual or CI)

---

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Nextflow Groovy complexity | High | Working examples, tiered templates (simple/transforms/advanced) |
| Schema merge memory | Medium | Streaming merge with conflict logging |
| Manifest concurrency (race conditions) | High | **Append-only partial status**: Workers write to `batch_N/status.jsonl`; `finalize` merges with timestamp-based conflict resolution |
| Nextflow/Manifest desync on resume | Medium | `finalize` reconciles Nextflow cache with manifest state |
| Spark cluster coordination | Medium | `local` mode for testing, `cluster` mode with shared cluster for production |

---

## Dependencies

```
Phase 2 (Batch Processor) ──► Phase 1 (CLI) ──► Phase 8 (Manifest)
                                                       │
Phase 5 (Config) ◄─────────────────────────────────────┤
       │                                               │
       ▼                                               ▼
Phase 6 (Transform) ──────────────────────────► Phase 3 (NF Processes)
                                                       │
                                                       ▼
                                               Phase 4 (Workflow)
                                                       │
                                                       ▼
                                               Phase 9 (Templates)
                                                       │
                                                       ▼
                                               Phase 7 (Init)
```

**Critical path:** 2 → 1 → 8 → 3 → 4

---

## Usage After Implementation

All execution goes through Makefile targets (consistent with existing pattern).

```bash
# Initialize project with orchestration
fairway init myproject --engine spark

# Run orchestrated pipeline locally
make run TABLE=claims

# Run on HPC (interactive, orchestrator on login node)
make run-hpc TABLE=claims

# Submit as fire-and-forget job
make submit-hpc TABLE=claims

# Override batch count
make run-hpc TABLE=claims N_BATCHES=20

# Check batch count before running
make batches TABLE=claims

# Monitor
make status
make logs
```

### Makefile Updates

**File:** `src/fairway/data/Makefile`

```makefile
# New orchestration targets
TABLE ?=
N_BATCHES ?=

# Calculate batches for a table
batches: ## Show batch count for TABLE
	@fairway batches --table $(TABLE)

# Run orchestrated pipeline (local)
# Spark mode read from config/spark.yaml
run: bootstrap ## Run locally with fan-out/fan-in
	./scripts/run_pipeline.sh -profile standard \
		--table $(TABLE) \
		$(if $(N_BATCHES),--n_batches $(N_BATCHES),) \
		-resume

# Run on HPC
# Spark mode read from config/spark.yaml
run-hpc: bootstrap ## Run on Slurm with fan-out/fan-in
	@if [ "$(HAS_APPTAINER)" = "yes" ]; then \
		./scripts/run_pipeline.sh -profile slurm,apptainer \
			--table $(TABLE) \
			$(if $(N_BATCHES),--n_batches $(N_BATCHES),) \
			-resume; \
	else \
		./scripts/run_pipeline.sh -profile slurm \
			--table $(TABLE) \
			$(if $(N_BATCHES),--n_batches $(N_BATCHES),) \
			-resume; \
	fi

# Submit driver job
submit-hpc: bootstrap ## Submit Nextflow orchestrator as Slurm job
	FAIRWAY_TABLE=$(TABLE) FAIRWAY_N_BATCHES=$(N_BATCHES) \
		sbatch scripts/driver.sh $(HAS_APPTAINER)

# Status and monitoring
status: ## Show pipeline status
	@nextflow log -q | tail -5
	@echo "---"
	@squeue -u $$USER 2>/dev/null || echo "Not on HPC"

logs: ## Show recent logs
	@tail -50 .nextflow.log 2>/dev/null || echo "No logs yet"
```

---

## Success Criteria

- [ ] 1000-file table processes without crash
- [ ] Fan-out/fan-in pattern works correctly
- [ ] Transform chain with mixed parallelism works
- [ ] Resume on failure works (Nextflow -resume)
- [ ] Makefile targets work: `make run`, `make run-hpc`, `make submit-hpc`
- [ ] `make batches TABLE=X` returns correct count
- [ ] Manifest concurrency: parallel workers don't corrupt state
- [ ] Documentation updated
