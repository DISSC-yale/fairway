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
`fairway run` is a worker process. It executes the pipeline directly and MUST NOT launch orchestrators or submit jobs. Use `fairway submit` for job submission.

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
Execution wrappers (e.g., `Makefile`) MUST work without external orchestrators.
The `fairway` CLI should be available via pip install or module system. No external orchestrators required.

---

### [RULE-109] Driver Job Pattern

**Priority:** SHOULD
**Category:** [PRO] Process & Workflow

**Rule:**
The preferred method for HPC submission is the "Driver Job" pattern:
- Use `fairway submit --with-spark` to submit the pipeline as a Slurm job.
- The driver job provisions Spark cluster (reading config from `config/spark.yaml`), then runs `fairway run`.
- For simple jobs without Spark: use `fairway submit` (no --with-spark flag).

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

**Priority:** MUST
**Category:** [PRO] Process & Workflow

**Rule:**
PySpark tests require a compatible Java environment. Test skip policy depends on context:

- **Docker/CI (`FAIRWAY_TEST_ENV=docker`):** PySpark tests MUST run. If PySpark is unavailable, tests MUST fail (not skip). A missing dependency in the container is a real error.
- **Local development:** PySpark tests MAY be skipped when PySpark/Java is not installed, but pytest MUST emit a terminal summary warning reporting the number of skipped PySpark tests and directing the developer to `make test-docker` for full coverage.

Test fixtures MUST use real engine constructors (`PySparkEngine()`) rather than bypassing initialization via `__new__`. This ensures tests exercise the same code path as production.

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

---

### [RULE-117] Schema Inference Completeness

**Priority:** MUST
**Category:** [CON] Content Requirements

**Rule:**
Schema inference MUST capture ALL columns from ALL files, not just sampled files.
- **Phase 1**: Scan all files for column names (headers only, fast)
- **Phase 2**: Sample files with coverage guarantee for type inference
- Schema inference MUST be deterministic (same input → same output)
- Type conflicts between files MUST resolve to the broader type (STRING wins)

**Type Hierarchy (narrowest to broadest):**
```
BOOLEAN < INTEGER < BIGINT < DOUBLE < STRING
```

**Rationale:**
Sampling a subset of files for schema inference can miss columns that only exist in non-sampled files, leading to data loss during ingestion.

---

### [RULE-118] Performance Defaults Must Be Conservative

**Priority:** SHOULD
**Category:** [ARC] Architectural Principles

**Rule:**
Performance optimization features that incur computational cost SHOULD be opt-in by default:
- Salting (requires `.count()`) → default: `false`
- Expensive preprocessing → default: `driver` mode
- Features that add columns to output → explicitly enabled

**Rationale:**
Users should not pay performance costs for features they don't need. Expensive operations should require explicit opt-in.

---

### [RULE-119] Input Validation for User-Provided Identifiers

**Priority:** MUST
**Category:** [SEC] Security

**Rule:**
User-provided identifiers (column names, table names) that are interpolated into SQL or code MUST be validated against a safe pattern before use.

```python
import re
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

if not VALID_IDENTIFIER.match(user_input):
    raise ValueError(f"Invalid identifier: {user_input}")
```

**Rationale:**
Prevents SQL injection and code injection from untrusted configuration files (e.g., spec files, schema definitions).

---

### [RULE-120] Validation Costs Must Be Bounded

**Priority:** SHOULD
**Category:** [ARC] Architectural Principles

**Rule:**
Data validation operations SHOULD have bounded cost (O(1) or O(sample_size)), not O(n) where n is dataset size. Full-dataset validation SHOULD be opt-in via explicit configuration.

**Examples:**
- Line length validation: Sample first 10,000 rows, not full count
- Schema validation: Check structure, not every value
- Uniqueness checks: Use sampling or probabilistic methods for large datasets

**Rationale:**
Validation should help catch errors quickly, not become the bottleneck for large datasets. A 1TB file should not require scanning 1TB just to check line lengths.

---

### [RULE-121] Config Path Resolution Pattern

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
All file paths in config files MUST be resolved using this fallback pattern:
1. Try path as-is (absolute paths, CWD-relative)
2. Try path relative to config file directory
3. Raise clear error if not found

**Rationale:**
This ensures configs work both locally (CWD = project root) and on HPC environments where CWD may be a work directory. Without this pattern, relative paths fail when the execution context differs from the config file location.

**Applies to:**
- `schema` file references
- `fixed_width_spec` file references
- `transformation` script paths
- `preprocess.action` script paths (when `.py` files)

---

### [RULE-122] Container Foreground Processes

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
When running long-lived services inside Apptainer containers (e.g., Spark master), MUST use foreground execution (`spark-class`) instead of daemonizing scripts (`start-master.sh`). Daemonized processes fork outside the container's mount namespace and lose access to container paths.

**Example:**
```bash
# WRONG: Daemonizes and loses /opt/spark access after apptainer exec exits
apptainer exec $SIF start-master.sh

# CORRECT: Stays in container namespace, maintains mount access
apptainer exec $SIF spark-class org.apache.spark.deploy.master.Master
```

**Rationale:**
When `start-master.sh` daemonizes the JVM via `nohup`/fork, the child process is orphaned outside the container's mount namespace when `apptainer exec` exits. The orphaned JVM loses access to `/opt/spark/jars/`, causing `ChannelInitializer` failures on every incoming connection. Running `spark-class` directly keeps the JVM inside the container.

---

### [RULE-123] Lazy Engine Initialization

**Priority:** SHOULD
**Category:** [ARC] Architectural Principles

**Rule:**
Engine instances (Spark, DuckDB) SHOULD be lazily initialized on first use, not at pipeline construction time. This prevents unnecessary cluster startup when only metadata operations are needed.

**Example:**
```python
class IngestionPipeline:
    def __init__(self, config_path):
        self._engine = None  # Lazy-initialized

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self._get_engine()
        return self._engine
```

**Rationale:**
Spark cluster startup can take 30+ seconds. Operations like config validation, preprocessing cache checks, or manifest queries don't need the engine. Lazy initialization defers this cost until actual data processing begins.

---

### [RULE-124] Temp Directory Fallback Hierarchy

**Priority:** MUST
**Category:** [ARC] Architectural Principles

**Rule:**
Temporary/scratch directory resolution MUST follow this fallback hierarchy:
1. `FAIRWAY_TEMP` environment variable
2. `storage.temp` in config
3. `storage.scratch_dir` in config (HPC fast-scratch)
4. `$SCRATCH/fairway` (HPC convention)
5. `/tmp/fairway_$USER` (system default)

**Example:**
```python
temp_loc = (
    os.environ.get('FAIRWAY_TEMP') or
    config.storage.get('temp') or
    config.storage.get('scratch_dir') or
    (os.path.join(os.environ['SCRATCH'], 'fairway') if os.environ.get('SCRATCH') else None) or
    os.path.join(tempfile.gettempdir(), f'fairway_{os.getenv("USER", "default")}')
)
```

**Rationale:**
HPC environments often have fast local scratch storage (`$SCRATCH`) that performs better than shared filesystems for intermediate files. This hierarchy allows users to configure optimal storage while providing sensible defaults for both HPC and local development.
