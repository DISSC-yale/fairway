# Fairway Pipeline Improvement Plan

## Overview

This plan organizes improvements to the Fairway data ingestion pipeline into manageable chunks that can be worked on incrementally. The goal is maintainable, easy-to-understand code with an intuitive user experience for data engineers.

---

## Current State Summary

- **CLI**: `fairway` command with subcommands (`run`, `init`, `generate_schema`, `spark start/stop`, etc.)
- **Engines**: DuckDB (single-node) and PySpark (distributed)
- **Pipeline Steps**: 8 steps (preprocessing → ingestion → validation → enrichment → transformations → summarization → export → finalization)
- **Orchestration**: Nextflow (thin wrapper for SLURM job submission)
- **Config**: `fairway.yaml` with tables (formerly sources), storage, validations, enrichment sections
- **Manifest**: Single `fmanifest.json` tracking all tables

---

## Improvement Chunks

### Chunk A: Safety First (Foundation)
**Goal:** Create test safety net before making changes

| ID | Task | Description |
|----|------|-------------|
| A.1 | Audit existing tests | Document what's currently tested |
| A.2 | Create dummy datasets | Test data for each format (CSV, TSV, JSON, Parquet) |
| A.3 | Schema merge test case | Files with different column sets to verify merge behavior |
| A.4 | Zip handling test case | Nested zip files with data |
| A.5 | Pipeline step tests | Tests for preprocessing, transformation, validation scripts |

**Open Questions:**
- What test framework is currently used? (pytest?)
- Where should test fixtures live?

---

### Chunk B: Schema Merging Fix
**Goal:** Ensure schema captures superset of all columns across all files

| ID | Task | Description |
|----|------|-------------|
| B.1 | Document current behavior | Trace `schema_pipeline.py` - how does inference work today? |
| B.2 | Identify sampling issue | Is it reading all files or sampling N files? |
| B.3 | Fix schema union | Ensure all files read, columns unioned |
| B.4 | Add verbose logging | Log which files contributed to schema |
| B.5 | Validate fix | Run against test case A.3 |

**Key Files:**
- `src/fairway/schema_pipeline.py`
- `src/fairway/engines/*_engine.py` (schema inference methods)

---

### Chunk C: Config Redesign
**Goal:** Make config intuitive for data engineers

**Design Rule:** Default `fairway.yaml` template must include ALL options with comments. Users comment/uncomment as needed - no hidden options, no doc lookups required.

| ID | Task | Description | Decision |
|----|------|-------------|----------|
| C.1 | Rename `source` → `table` | Rename throughout codebase | **DO IT** - no backwards compat |
| C.2 | Clarify `root` purpose | Document role in file relocation / manifest portability | |
| C.3 | Separate zip handling | New `archives` + `files` keys | **DO IT** - explicit syntax |
| C.4 | Add `--dry-run` preview | Show matched files without processing | |
| C.5 | Config validation | Error on missing fields, bad paths, duplicates | **DO IT** - fail fast |
| C.6 | Decide on unzip support | Remove and make preprocessing step? Or keep? | **KEEP** - via C.3 syntax |
| C.7 | Multi-table zip handling | Persistent cache, extract once per archive | **DO IT** - with manifest tracking |

**Archive Handling Decision (C.3 + C.7):**
Table-centric with persistent shared cache:
```yaml
tables:
  - name: sales
    archives: "data.zip"
    files: "sales_*.csv"
  - name: customers
    archives: "data.zip"     # same zip, extracted once, cached
    files: "customers_*.csv"
```
- Extract to `.fairway_cache/archives/{name}_{hash}/`
- Track extractions in manifest
- On resume: skip extraction if archive unchanged

**Detailed implementation:** See `plan_phase2.md`

---

### Chunk D: Performance Fixes ✅ COMPLETE
**Goal:** Right-sized parquet files, remove unnecessary salting

| ID | Task | Description | Status |
|----|------|-------------|--------|
| D.1 | Remove/disable salting | Make opt-in only if needed | ✅ Done |
| D.2 | Increase parquet file size | Current size too small; target ~128-256MB | ✅ Done |
| D.3 | Review SLURM defaults | Document current settings, create tuning guide | ✅ Done (in docs/engines.md) |
| D.4 | Scratch output location | Support writing intermediate files outside data dir | ✅ Done |

**Implementation Summary:**
- `config_loader.py`: Added `salting` (default: False), `target_file_size_mb` (default: 128), `compression` (default: snappy), `scratch_dir` with env var expansion
- `pyspark_engine.py`: Changed `balanced=False` default, added `maxRecordsPerFile` option
- `pipeline.py`: Passes performance config to engine, uses `scratch_dir` for intermediate output
- `tests/test_performance.py`: 12 new tests covering all performance changes
- Docs updated: `configuration.md`, `engines.md`, `README.md`

**Key Files:**
- `src/fairway/engines/pyspark_engine.py` (write options)
- `src/fairway/config_loader.py` (storage settings)

---

### Chunk E: Manifest & File Reorganization
**Goal:** Cleaner project structure, one manifest per table

| ID | Task | Description |
|----|------|-------------|
| E.1 | Design per-table manifest | `manifest/<table>.json` structure |
| E.2 | Design schema directory | `schema/<table>.json` structure |
| E.3 | Implement migration | Convert single manifest to per-table |
| E.4 | Update manifest code | All reads/writes use new structure |

**Current State:**
- Single `fmanifest.json` with all tables
- Schemas stored in `config/data/schemas/`

**Benefits:**
- Easier to inspect individual tables
- Reduces merge conflicts
- Cleaner git diffs

---

### Chunk F: Fixed-Width File Support
**Goal:** Read fixed-width files with external spec

| ID | Task | Description |
|----|------|-------------|
| F.1 | Define spec format | Column name, start position, length, type |
| F.2 | Add fixed-width reader | New engine capability |
| F.3 | Config support | Point to spec file in source config |
| F.4 | Test case | Sample fixed-width data + spec |

**Proposed Spec Format:**
```yaml
columns:
  - name: id
    start: 0
    length: 10
    type: INTEGER
  - name: name
    start: 10
    length: 50
    type: STRING
```

---

### Chunk G: Workflow Simplification (Nextflow Removal)
**Goal:** Simplify orchestration by removing Nextflow

| ID | Task | Description |
|----|------|-------------|
| G.1 | Evaluate Nextflow value | What does it provide beyond SLURM submission? |
| G.2 | Add SLURM submission to CLI | `fairway submit` command for batch jobs |
| G.3 | Deprecate Nextflow | Keep files for reference, document removal |
| G.4 | Update docs | Remove Nextflow references |

**Current Findings:**
- Nextflow is a thin wrapper - just submits `fairway run` to SLURM
- CLI already handles everything substantive
- Cloud portability is theoretical (not currently needed)
- Debugging Nextflow failures adds complexity

**Proposed CLI Addition:**
```bash
fairway submit --config config.yaml --slurm-partition day --slurm-mem 16G
# Equivalent to current: nextflow run main.nf --profile slurm
```

**Risk:** Low - Nextflow isn't providing significant value beyond job submission

---

### Chunk H: Documentation & Logging
**Goal:** Clear docs, observable execution

| ID | Task | Description |
|----|------|-------------|
| H.1 | Improve logging | Progress, files processed, decisions made |
| H.2 | Update README | New config format, examples |
| H.3 | Update automated docs | API docs, config reference |
| H.4 | Document Redivis workflow | Push process, metadata handling |

---

### Chunk I: Code Refactoring (Ongoing)
**Goal:** Maintainable, easy-to-understand code

| ID | Task | Description |
|----|------|-------------|
| I.1 | Incremental cleanup | Refactor as we touch code in other chunks |
| I.2 | Consistent patterns | Apply same patterns across engines |
| I.3 | Remove dead code | Clean up unused functions/modules |
| I.4 | Add type hints | Where missing, especially public APIs |

---

## Parallel Execution Plan

```
                    ┌─── Chunk B (Schema Fix) ────────────────────┐
                    │                                              │
Chunk A (Testing) ──┼─── Chunk C (Config Redesign) ──→ Chunk E (Reorg)
                    │         │                                    │
                    │         └─── Chunk J (Validator) ───────────┤
                    │                                              │
                    ├─── Chunk D (Performance) ✅ ─────────────────┤
                    │                                              │
                    └─── Chunk F (Fixed-Width) ────────────────────┘

Chunk G (Nextflow Removal) ─── Independent, can start anytime
Chunk H (Docs) ─────────────── Continuous, as chunks complete
Chunk I (Refactoring) ──────── Ongoing with each chunk
```

**Dependencies:**
- Chunk E (Reorg) depends on Chunk C (Config) decisions
- Chunk J (Validator) depends on Chunk C (Config) decisions being finalized
- All chunks benefit from Chunk A (Testing) being done first
- Chunk G is fully independent

---

## Recommended Execution Order

### Phase 1: Foundation
- **Chunk A**: Testing foundation (enables safe changes)
- **Chunk B**: Schema merging fix (correctness issue)

### Phase 2: UX Improvements (Parallel)
- **Chunk C**: Config redesign (see `plan_phase2.md` for detailed implementation)
- **Chunk D**: Performance fixes

### Phase 3: Structure
- **Chunk E**: Manifest & file reorganization
- **Chunk F**: Fixed-width support

### Phase 4: Simplification
- **Chunk G**: Nextflow removal

### Continuous
- **Chunk H**: Documentation (as each chunk lands)
- **Chunk I**: Refactoring (with each change)

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Config `sources`→`tables` rename breaks configs | HIGH | **Breaking change** - all configs must update. Provide sed one-liner. |
| Schema fix changes output | MEDIUM | Version schemas, compare outputs before/after |
| Manifest restructure breaks tooling | MEDIUM | Provide migration script |
| Nextflow removal | LOW | CLI already handles everything; just job submission |

---

## Open Questions

1. What test framework is currently used?
2. ~~What's the target parquet file size?~~ → **128MB** (configurable)
3. ~~Should `source` be renamed to `table`?~~ → **YES, rename** (no backwards compat)
4. ~~Should unzip support be removed or made more explicit?~~ → **Made explicit** via `archives`/`files` keys
5. Fixed-width spec format - YAML, JSON, or something else?

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/fairway/cli.py` | CLI entry point, all commands |
| `src/fairway/pipeline.py` | Main ingestion pipeline (8 steps) |
| `src/fairway/schema_pipeline.py` | Schema discovery |
| `src/fairway/config_loader.py` | Parses fairway.yaml |
| `src/fairway/manifest.py` | Tracks processing state |
| `src/fairway/engines/duckdb_engine.py` | DuckDB backend |
| `src/fairway/engines/pyspark_engine.py` | PySpark backend |
| `src/fairway/data/main.nf` | Nextflow pipeline (to be removed) |

---

## Appendix: Orchestrator Discussion

### Do We Need an Orchestrator After Removing Nextflow?

**What Nextflow currently provides vs. what Fairway already has:**

| Capability | Nextflow | Fairway Already Has |
|------------|----------|---------------------|
| SLURM job submission | Yes | No (but trivial to add via `sbatch`) |
| Step sequencing | Yes | Yes - `pipeline.py` runs 8 steps in order |
| File-level caching | No | Yes - manifest tracks processed files |
| Retry on failure | Yes | Partial - manifest skips already-done files |
| Parallelism within steps | No | Yes - Spark handles this |
| Cross-table parallelism | Could | No |
| Logging | Yes | Yes |

### Use Cases Identified

**1. Resume from failure**
- Manifest handles file-level resume — sufficient for our needs
- No orchestrator needed for this

**2. Parallel table processing**
- Sometimes need to run multiple tables in parallel
- Can be handled with simple shell parallelism:

```bash
# Option 1: Background jobs
fairway run --config config.yaml --table sales &
fairway run --config config.yaml --table customers &
wait

# Option 2: GNU parallel
parallel fairway run --config config.yaml --table {} ::: sales customers inventory

# Option 3: SLURM job array
sbatch --array=0-2 run_table.sh  # each array task handles one table
```

**3. Transform creates new table (table chains)**
- Some transforms produce output that becomes input for another table
- Proposed solution: Model as config dependency, not orchestrator dependency

```yaml
tables:
  - name: raw_data
    root: data/raw
    path: "*.csv"

  - name: derived_table
    root: data/intermediate/raw_data  # points to previous table's output
    path: "*.parquet"
    depends_on: raw_data  # optional: for documentation/ordering
```

### Conclusion

**Likely don't need a formal orchestrator.** Simple solutions suffice:

| Need | Solution |
|------|----------|
| SLURM submission | `fairway submit` command (simple `sbatch` wrapper) |
| Parallel tables | Shell parallelism or SLURM job arrays |
| Table dependencies | Config-based (`root` points to previous output) + run order |
| Resume | Manifest already handles |

### Open Questions to Resolve

1. Does `fairway run` currently support a `--table` filter to run specific tables only?
2. How many tables typically run in parallel? (2-5? 10-20? 50+?)
3. How deep do table chains go? (A → B? or A → B → C → D?)
4. Is the table chain pattern common or rare?

If chains are deep or complex, consider a lightweight Makefile or simple Python DAG runner — but still not a full orchestrator like Airflow/Prefect/Nextflow

---

### Chunk J: Config Validator (Expands C.5)
**Goal:** Comprehensive config validation with clear error messages and CLI support

**Status:** PLANNED

**Background:** Users encounter confusing errors due to:
- Using wrong keys (`sources:` instead of `tables:`)
- Misconfigured paths (relative vs absolute, wrong structure)
- Schema references pointing to non-existent files
- Missing required fields
- Invalid enum values

Current validation in `config_loader.py` is basic and happens after parsing. We need dedicated validation that fails fast with actionable messages.

#### Implementation Phases

**Phase 1: Package Structure**
Create `src/fairway/validation/`:
```
validation/
  __init__.py
  errors.py           # ValidationError, ValidationWarning, ValidationResult
  schema.py           # JSON Schema definition for fairway.yaml
  suggestions.py      # Typo detection & "Did you mean X?" suggestions
  config_validator.py # Main ConfigValidator class
```

**Phase 2: Error Classes**

| Class | Purpose |
|-------|---------|
| `ValidationError` | Fatal error with message, path, line number, suggestion |
| `ValidationWarning` | Non-fatal issue (e.g., glob matches no files) |
| `ValidationResult` | Collects all errors/warnings, provides summary |

**Phase 3: JSON Schema Definition**

Define schema covering:
- **Required fields:** `dataset_name`, `tables`
- **Enum values:** `engine` (duckdb\|pyspark), `format` (csv\|tsv\|tab\|json\|parquet\|fixed_width)
- **Path fields:** `schema`, `fixed_width_spec`, `transformation`, `root`
- **Nested structures:** `tables[]`, `storage`, `performance`, `validations`

**Phase 4: Suggestion Engine**

Detect common typos and provide suggestions:
```
sources: → "Unknown key 'sources'. Did you mean 'tables'?"
enigne: → "Unknown key 'enigne'. Did you mean 'engine'?"
format: "CSV" → "Invalid format 'CSV'. Did you mean 'csv'?"
```

**Phase 5: CLI Command**

Add `fairway validate` command:
```bash
# Basic validation
fairway validate config/fairway.yaml

# JSON output for CI/CD integration
fairway validate --format json config/fairway.yaml

# Check paths exist
fairway validate --check-paths config/fairway.yaml
```

**Phase 6: Integration**

- Call validator from `Config.__init__()` before table expansion
- Provide `--skip-validation` flag for advanced users
- Integrate with existing `ConfigValidationError` class

#### Tasks

| ID | Task | Description | Effort |
|----|------|-------------|--------|
| J.1 | Create validation package | Package structure, `__init__.py` | 15 min |
| J.2 | Implement error classes | `ValidationError`, `ValidationWarning`, `ValidationResult` | 30 min |
| J.3 | Define JSON Schema | Schema for all config fields | 1 hour |
| J.4 | Implement suggestion engine | Levenshtein distance for typo detection | 45 min |
| J.5 | Implement ConfigValidator | Main validation logic | 2 hours |
| J.6 | Add CLI command | `fairway validate` with options | 45 min |
| J.7 | Create test fixtures | Valid/invalid config samples | 30 min |
| J.8 | Write unit tests | >90% coverage of validator | 1.5 hours |
| J.9 | Integrate with config_loader | Call validator on load | 30 min |
| J.10 | Update documentation | Usage examples, error reference | 30 min |

**Total estimated effort:** 8-10 hours

#### Key Validation Rules

| Rule | Error Message Example |
|------|----------------------|
| Unknown top-level key | "Unknown key 'sources'. Did you mean 'tables'?" |
| Missing required field | "Missing required field 'dataset_name'" |
| Invalid engine | "Invalid engine 'spark'. Must be 'duckdb' or 'pyspark'" |
| Invalid format | "tables[0]: Invalid format 'CSV'. Must be one of: csv, tsv, tab, json, parquet, fixed_width" |
| Missing table name | "tables[0]: Missing required field 'name'" |
| Duplicate table name | "tables[1]: Duplicate table name 'sales' (first defined at tables[0])" |
| Schema file not found | "tables[0].schema: File not found: 'schema/missing.yaml'" |
| Glob matches nothing | "WARNING: tables[0].path: Pattern 'data/*.csv' matches no files" |
| fixed_width without spec | "tables[0]: format 'fixed_width' requires 'fixed_width_spec'" |

#### Dependencies

- Add `jsonschema>=4.0.0` to `pyproject.toml` (or implement manually)

#### Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing tests | High | Run full test suite after each change |
| Schema too strict | Medium | Start permissive, tighten based on feedback |
| Performance overhead | Low | Validation is fast, configs are small |
| Missing edge cases | Medium | Add tests as issues discovered |

#### Success Criteria

1. `fairway validate` command works standalone
2. Clear error messages with field paths (e.g., `tables[0].name`)
3. Detects `sources:` vs `tables:` typo with suggestion
4. Validates all enum values
5. Checks file path existence (optional flag)
6. Zero breaking changes to existing functionality
7. All existing tests pass
8. New tests achieve >90% coverage

#### Files to Create/Modify

| File | Action |
|------|--------|
| `src/fairway/validation/__init__.py` | Create |
| `src/fairway/validation/errors.py` | Create |
| `src/fairway/validation/schema.py` | Create |
| `src/fairway/validation/suggestions.py` | Create |
| `src/fairway/validation/config_validator.py` | Create |
| `src/fairway/cli.py` | Modify - add `validate` command |
| `src/fairway/config_loader.py` | Modify - integrate validator |
| `tests/test_config_validator.py` | Create |
| `tests/fixtures/validation/*.yaml` | Create |
| `pyproject.toml` | Modify - add jsonschema dependency |

---

### Chunk K: Nextflow Two-Phase Workflow & Manifest Integration
**Goal:** Separate schema discovery from ingestion, integrate manifest for incremental processing

**Status:** IN PROGRESS

**Background:**
The Nextflow orchestration currently runs schema + ingest as a single workflow, starting Spark before schema discovery even though schema doesn't need it. Additionally, batch commands (`schema-scan`, `ingest`) don't integrate with the manifest system.

#### K.1: Two-Phase Workflow (COMPLETE)

Separate Nextflow entry points:
- `nextflow run main.nf -entry schema` — Schema discovery only (no Spark)
- `nextflow run main.nf -entry ingest` — Ingestion only (requires Spark)
- Default — Full pipeline (backwards compatible)

**Makefile targets:**
```bash
make submit-schema    # Phase 1: Schema discovery (no Spark)
make submit-ingest    # Phase 2: Ingestion (starts Spark automatically)
make submit-hpc       # Full pipeline (legacy, starts Spark for everything)
```

**Benefits:**
- Spark cluster not started until needed (saves resources)
- Schema phase can run on smaller allocations
- Clearer separation of concerns

#### K.2: Manifest Integration for Batch Commands (PLANNED)

**Current state:** Batch commands (`schema-scan`, `ingest`) don't use manifest — they process all files every time.

**Target state:** All processing driven by manifest:

| Command | Manifest Integration |
|---------|---------------------|
| `schema-scan` | Check `get_preprocessed_path()` before extracting zips |
| `schema-scan` | Call `record_preprocessing()` after extracting |
| `ingest` | Use `should_process()` to skip already-ingested files |
| `ingest` | Call `update_file()` after successful ingestion |

#### K.3: Incremental Schema Discovery (PLANNED)

**Problem:** Schema discovery currently scans all files every time. For large datasets with 1000s of files, this is slow even though most files haven't changed.

**Approach: Simple Incremental (Recommended)**

```
1. Load existing unified_schema.json (if exists)
2. Query manifest for file status:
   - NEW files (not in manifest) → scan for columns
   - UNCHANGED files (hash matches) → skip
   - CHANGED files (hash differs) → re-scan
3. Merge discovered columns into existing schema (additive)
4. Save updated schema
5. Update manifest with new file hashes
```

**Why additive-only is sufficient:**
- Raw data files rarely change (append-only datasets)
- Columns are rarely removed from source files
- If full rescan needed, provide `--full` flag

**Implementation tasks:**

| ID | Task | Description | Effort |
|----|------|-------------|--------|
| K.3.1 | Add manifest to BatchProcessor | Pass manifest store to batch commands | 30 min |
| K.3.2 | Implement incremental schema-scan | Check manifest before scanning | 1 hour |
| K.3.3 | Schema merge with existing | Load + merge + save unified schema | 45 min |
| K.3.4 | Add `--full` flag | Force full rescan when needed | 15 min |
| K.3.5 | Update manifest after scan | Record which files contributed to schema | 30 min |
| K.3.6 | Tests | Test incremental behavior | 1 hour |

**Total estimated effort:** 4-5 hours

#### K.4: Incremental Ingestion (PLANNED)

**Problem:** Ingest currently processes all files in each batch, even if already ingested.

**Approach:**
```
1. For each file in batch:
   - Check manifest.should_process(file)
   - Skip if unchanged
2. After successful ingestion:
   - Call manifest.update_file(file, status="success")
3. Support `--force` flag to reprocess all
```

**Implementation tasks:**

| ID | Task | Description | Effort |
|----|------|-------------|--------|
| K.4.1 | Add manifest check to ingest | Filter batch files by should_process() | 30 min |
| K.4.2 | Update manifest after ingest | Record successful ingestion | 30 min |
| K.4.3 | Add `--force` flag | Bypass manifest check | 15 min |
| K.4.4 | Handle partial batch success | Track per-file status | 45 min |
| K.4.5 | Tests | Test incremental ingestion | 1 hour |

**Total estimated effort:** 3-4 hours

#### Files to Modify

| File | Changes |
|------|---------|
| `src/fairway/data/main.nf` | Add `schema` and `ingest` entry points |
| `src/fairway/data/Makefile` | Add `submit-schema`, `submit-ingest` targets |
| `src/fairway/data/scripts/driver.sh` | Support `FAIRWAY_ENTRY` env var |
| `src/fairway/batch_processor.py` | Add manifest integration |
| `src/fairway/cli.py` | Update `schema-scan` and `ingest` commands |
| `src/fairway/manifest.py` | May need new methods for batch tracking |

#### Success Criteria

1. `make submit-schema` runs without starting Spark
2. `make submit-ingest` starts Spark and uses schemas from phase 1
3. Re-running schema phase only scans new/changed files
4. Re-running ingest phase only processes new/changed files
5. `--full` / `--force` flags bypass incremental behavior
6. All file processing tracked in manifest
