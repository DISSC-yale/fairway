# Orchestrator Decision Document

## Executive Summary

This document synthesizes our design discussion on orchestration for Fairway. The core question: **How should Fairway handle pipelines with 1000+ heterogeneous files that require parallel processing?**

**Key Decision:** Fairway should remain a **worker** (not an orchestrator). Orchestration should be handled externally via Make + SLURM, with optional migration path to Parsl or Nextflow for more complex needs.

---

## Problem Statement

### Current Architecture
```
Nextflow (thin wrapper)
    └── fairway run (ONE job)
        └── Spark processes ALL files
```

### The Problem
- **1000 CSVs with different schemas** per table
- Spark crashes when reading too many heterogeneous files in one job
- Schema inference needs to scan ALL files (can't sample)
- Some transforms need full dataset, others are per-partition
- Current Nextflow setup provides no actual parallelism

### What's Needed
```
Schema Scan (fan-out) → Schema Merge (fan-in) →
Ingest (fan-out) → Transform (mixed) → Finalize
```

---

## Key Concepts

### Fan-Out (One → Many)
Split a single task into multiple parallel jobs.

```
Single Input        ┌─── Job 0: files 0-99
(1000 files) ───────┼─── Job 1: files 100-199
                    ├─── Job 2: files 200-299
                    └─── ... (10 parallel jobs)
```

**SLURM mechanism:** `sbatch --array=0-9`

### Fan-In (Many → One)
Collect results from parallel jobs into a single job.

```
Job 0: schema_0.json ───┐
Job 1: schema_1.json ───┼───► Merge Job: unified_schema.json
Job 2: schema_2.json ───┘
```

**SLURM mechanism:** `sbatch --dependency=afterok:12345`

### Repartition
After fan-in produces one large output, split it for subsequent fan-out.

---

## Pipeline Phases

```
Phase 1: Schema Discovery (fan-out)
┌─────────────────────────────────────────────────────────────┐
│  Job[0-N]: Each scans batch of files → schema_N.json        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Phase 2: Schema Merge (fan-in)
┌─────────────────────────────────────────────────────────────┐
│  Single job merges all schemas → unified_schema.json        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Phase 3: Ingest (fan-out)
┌─────────────────────────────────────────────────────────────┐
│  Job[0-N]: Each reads batch → partition_N.parquet           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Phase 4: Transforms (mixed)
┌─────────────────────────────────────────────────────────────┐
│  parallel=true:  Fan-out (each partition independent)       │
│  parallel=false: Fan-in (needs full dataset, e.g., dedup)   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Phase 5: Finalize (single)
┌─────────────────────────────────────────────────────────────┐
│  Validation, export, cleanup                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Transform Patterns

| Pattern | Example | Parallel? | Implementation |
|---------|---------|-----------|----------------|
| Row-level | Rename columns, cast types | Yes | Fan-out per partition |
| Lookup/Join | Enrich with reference data | Yes | Fan-out (broadcast ref) |
| Aggregation | Deduplicate across files | No | Fan-in (full dataset) |
| Sorting | Order by date | No | Fan-in with sort |

### Config Representation
```yaml
tables:
  - name: claims
    transforms:
      - name: standardize
        script: src/transforms/standardize.py
        parallel: true

      - name: deduplicate
        script: src/transforms/deduplicate.py
        parallel: false
        sort_by: [claim_date]
        repartition_output: 10  # Split output for next fan-out

      - name: enrich
        script: src/transforms/enrich.py
        parallel: true
```

---

## Options Evaluated

### Option 1: Fairway as Orchestrator
Build orchestration into Fairway itself.

**Rejected because:**
- Adds complexity to Fairway codebase
- Reinvents orchestration (poorly)
- User preference: "I do not want Fairway to be an orchestrator"

### Option 2: Nextflow (Current)
Keep Nextflow but enhance `main.nf` with actual fan-out/fan-in.

| Pros | Cons |
|------|------|
| Already in place | Groovy DSL learning curve |
| Excellent container support | Debugging is harder |
| Cloud-portable | Currently just a thin wrapper |
| Mature ecosystem | Overkill for single-table pipelines |

### Option 3: Parsl
Python-native HPC workflow library from Argonne National Lab.

| Pros | Cons |
|------|------|
| Pure Python (team knows it) | Another dependency |
| Excellent SLURM support | Smaller community than Nextflow |
| Low overhead, scales well | Less mature cloud support |
| Easy debugging (Python traces) | Container support is manual |

### Option 4: Make + SLURM
Use Make to wrap SLURM commands. Let SLURM handle orchestration natively.

| Pros | Cons |
|------|------|
| Zero new dependencies | SLURM-only (no cloud portability) |
| Team already knows Make | Manual dependency chaining |
| Simple, debuggable | Less elegant for complex DAGs |
| Uses native SLURM features | Dynamic transforms need scripting |

---

## Nextflow vs Parsl Comparison

| Factor | Nextflow | Parsl |
|--------|----------|-------|
| Language | Groovy DSL | Python |
| SLURM support | Excellent | Excellent |
| Cloud support | Excellent | Good |
| Container support | Native | Manual |
| Debugging | Groovy traces | Python traces |
| Learning curve | Medium | Low (if you know Python) |
| Community | Large (bioinformatics) | Smaller (HPC) |
| Maturity | 10+ years | 7 years |

**Bottom line:** Nextflow is better for container-centric, cloud-portable pipelines. Parsl is better for HPC-focused, Python-native workflows.

---

## Recommendation

### Primary: Make + SLURM

Given that:
- Currently only targeting SLURM
- Cloud portability is theoretical/future
- Team knows Make
- Simplicity is valued
- Fairway should stay a worker

**Use Make to orchestrate SLURM job arrays and dependencies.**

```makefile
# Simplified example
$(STATE_DIR)/schema_scan.jobid:
	sbatch --array=0-$(N_BATCHES) \
		--wrap="fairway scan-schema --batch $$SLURM_ARRAY_TASK_ID" > $@

$(STATE_DIR)/schema_merge.jobid: $(STATE_DIR)/schema_scan.jobid
	sbatch --dependency=afterok:$$(cat $<) \
		--wrap="fairway merge-schemas" > $@
```

### Secondary: Parsl (if more complexity needed)

If pipelines grow more complex or need programmatic DAG construction:

```python
@bash_app
def scan_schema(batch: int):
    return f"fairway scan-schema --batch {batch}"

@bash_app
def merge_schemas(inputs=[]):
    return "fairway merge-schemas"

# Fan-out → fan-in
futures = [scan_schema(i) for i in range(10)]
merged = merge_schemas(inputs=futures)
```

### Tertiary: Keep Nextflow (if cloud portability becomes real)

If multi-cloud deployment becomes a concrete requirement, enhance the existing Nextflow setup rather than switching.

---

## Fairway Changes Required

Regardless of orchestrator choice, Fairway needs granular worker commands:

```bash
# File discovery
fairway list-files --table TABLE --count
fairway list-files --table TABLE --batch N

# Schema operations
fairway scan-schema --table TABLE --batch N
fairway merge-schemas --table TABLE

# Batch operations
fairway ingest --table TABLE --batch N
fairway transform --table TABLE --transform NAME --batch N   # parallel
fairway transform --table TABLE --transform NAME             # fan-in

# Finalization
fairway finalize --table TABLE
```

These commands are thin wrappers around existing functionality, scoped to file batches.

---

## Migration Path

```
Current State                Future State (if needed)
─────────────────────────    ─────────────────────────
Make + SLURM
  │
  ├──► Works well ──────────► Stay here
  │
  └──► Needs more ──────────► Parsl (Python-native)
        complexity                   │
                                     └──► Needs cloud ──► Nextflow
```

---

## Open Questions

1. **Batch size tuning:** What's the optimal files-per-batch? Depends on file sizes, cluster capacity.

2. **Spark integration:** Do batch commands start their own Spark context, or connect to a running cluster?

3. **State management:** Where does batch progress get tracked? Manifest per table?

4. **Error handling:** If batch 5 of 10 fails, how is retry handled?

5. **Transform interface:** How does a transform declare `parallel: true/false`? Class attribute? Config only?

---

## Next Steps

1. **Implement worker commands** in Fairway CLI (`scan-schema`, `ingest --batch`, etc.)

2. **Create Makefile template** for orchestrated pipelines

3. **Test on real workload** with 1000+ file table

4. **Document** the batch processing workflow

---

## Appendix: Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Makefile                                │
│  - Human-readable targets                                       │
│  - Wraps sbatch commands                                        │
│  - Chains via SLURM --dependency                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ sbatch --array / --dependency
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          SLURM                                  │
│  - Job arrays for fan-out                                       │
│  - Dependencies for fan-in                                      │
│  - Native retry, monitoring (squeue, sacct)                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ executes
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Fairway (Worker)                             │
│  - fairway scan-schema --batch N                                │
│  - fairway ingest --batch N                                     │
│  - fairway transform --batch N                                  │
│  - Reads/writes via manifest                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ reads/writes
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Data Storage                               │
│  - data/raw/*.csv (input)                                       │
│  - .fairway/schemas/*.json (intermediate)                       │
│  - data/intermediate/*.parquet (bronze)                         │
│  - data/final/*.parquet (gold)                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

*Document generated from design discussion on 2025-02-01*
*Branch: explore/orchestration-submission*
