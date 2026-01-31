# Project Rules

**Version:** 1.0.0
**Status:** Active

---

## Active Rules

### [RULE-100] CLI Build Process

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
CLI should have a build process for building the container (implemented as `fairway build`).

---

### [RULE-101] Job Management Commands

**Priority:** SHOULD
**Category:** [PRO] Process & Workflow

**Rule:**
CLI should include `status` and `cancel` commands to manage Slurm jobs (wrapping `squeue` and `scancel`).

---

### [RULE-102] Decoupled Worker Architecture

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
`fairway run` MUST NOT launch Nextflow. It is a worker process.

---

### [RULE-103] Scalable Data Collection

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
Methods like `toPandas()` or `collect()` MUST NOT be used on full datasets in production paths. Use iterators, sampling, or native engine operations.

---

### [RULE-104] Engine Agnostic Components

**Priority:** SHOULD
**Category:** [ARC] Architectural Principles

**Rule:**
Where possible, downstream components (Validators, Enrichers) should be engine-agnostic or support multiple backends (Pandas, Spark, DuckDB) to avoid bottlenecks.

---

### [RULE-105] Apptainer Variable Definitions

**Priority:** MUST
**Category:** [CON] Content Requirements

**Rule:**
`Apptainer.def` templates MUST define necessary build variables (e.g., `SPARK_VERSION`, `HADOOP_VERSION`) within the `%post` section before they are used.

---

### [RULE-106] Single Source of Truth for Templates

**Priority:** MUST
**Category:** [STR] Structural Rules

**Rule:**
All templates MUST be stored as static files in `src/fairway/data/` and loaded via `_read_data_file`. `src/fairway/templates.py` should act as a loader.

---

### [RULE-107] Consistent Spark Version

**Priority:** MUST
**Category:** [CON] Content Requirements

**Rule:**
Ensure the Spark version is consistent across all configuration files (Use Spark 4.1.1):
- `pyproject.toml`
- `src/fairway/data/Apptainer.def`
- `src/fairway/data/fairway-hpc.sh` (or generated scripts)

---

### [RULE-108] Zero-Dependency Bootstrap

**Priority:** MUST
**Category:** [PRO] Process & Workflow

**Rule:**
Execution wrappers (e.g., `Makefile`) MUST robustly handle missing dependencies. 
Nextflow Logic: Check PATH -> Check `./nextflow` -> Check `module avail` -> Download from web.

---

### [RULE-109] Driver Job Pattern

**Priority:** SHOULD
**Category:** [PRO] Process & Workflow

**Rule:**
The preferred method for HPC submission is the "Driver Job" pattern:
- Submit the orchestrator (Nextflow) as a small single-task Slurm job (`scripts/driver.sh`).
- This job then runs Nextflow, which submits the actual worker tasks.

---

### [RULE-110] Scripts Directory Structure

**Priority:** MUST
**Category:** [STR] Structural Rules

**Rule:**
Helper scripts (e.g., `driver.sh`, `fairway-hpc.sh`) MUST be placed in `scripts/` during project initialization.

---

### [RULE-111] Local Testing Standard

**Priority:** SHOULD
**Category:** [PRO] Process & Workflow

**Rule:**
Run all tests using `pytest` from the project root.
```bash
pytest tests/
```

---

### [RULE-112] Import Isolation

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
Ensure the pipeline remains importable even if optional dependencies (like DuckDB) are missing.
- Run the isolation test: `python tests/test_imports_isolation.py`
- (Note: This is also covered by `pytest` but useful for verification in clean environments).

---

### [RULE-113] Test Environment Requirements

**Priority:** MAY
**Category:** [PRO] Process & Workflow

**Rule:**
Some tests (e.g., PySpark) require a compatible Java environment. If testing locally on a machine without Spark/Java properly configured, these tests may fail or be skipped.

---

### [RULE-114] Integration Testing Cleanliness

**Priority:** MUST
**Category:** [PRO] Process & Workflow

**Rule:**
When testing project initialization:
- Create a temporary directory (e.g., `test_proj`).
- Run `fairway init` and verify the output.
- **CRITICAL**: Delete the temporary directory immediately after verification to prevent clutter.

---

### [RULE-115] Data Integrity (Never Drop)

**Priority:** MUST
**Category:** [CON] Content Requirements

**Rule:**
Ingestion pipelines MUST NOT silently drop data.
- If a source file contains columns NOT present in the defined schema, the pipeline MUST fail with an error (forcing a schema update) rather than ignoring the extra columns.
- The "Strict Schema" pattern means "Strictly conform or Fail", never "Strictly conform and Drop the rest".

---

### [RULE-116] Test Coverage for File Formats

**Priority:** MUST
**Category:** [PRO] Process & Workflow

**Rule:**
When adding support for a new file format (e.g., fixed-width, Avro, ORC):
1. Add dummy test data to `tests/fixtures/formats/<format_name>/`
2. Add schema merge test case to `tests/fixtures/schema_merge/<format>/`
3. Add test in `tests/test_ingestion_formats.py` covering read + schema inference
4. Update the checklist in `tests/fixtures/README.md`

**Checklist for new formats:**
- [ ] `tests/fixtures/formats/<format>/sample.<ext>` — Basic file
- [ ] `tests/fixtures/schema_merge/<format>/` — Files with different columns
- [ ] `tests/test_ingestion_formats.py::test_<format>_basic_read`
- [ ] `tests/test_schema_merge.py::test_<format>_schema_union`
