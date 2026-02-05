# Fairway Pipeline Improvement Plan

## Overview

This plan organizes improvements to the Fairway data ingestion pipeline into manageable chunks that can be worked on incrementally. The goal is maintainable, easy-to-understand code with an intuitive user experience for data engineers.



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


__ less unstructured

Here is the updated comprehensive summary of the "Fairway" ingestion pipeline architecture.

This version expands significantly on the **Partition-Aware Batching** strategy and includes a new section on **Observability & Logging** to ensure users aren't flying blind.

---

### **1. Core Architectural Shift**

* **Decision:** Move away from **Nextflow** and adopt a **Python + Native Spark** architecture ("Fairway").
* **Rationale:** Nextflow introduces "double-scheduling" overhead (orchestrator launching driver launching executors) that conflicts with Spark's native resource management.
* **Philosophy:** "Logic in Python, Muscle in Spark."

### **2. The Strategy: Partition-Aware Batching (Detailed)**

* **The Problem:**
* **Method A (Looping):** Submitting 1,000 separate Spark jobs (one per file) creates massive driver overhead (1000x startup costs).
* **Method B (Bulk Read):** Reading all 1,000 files at once (e.g., `spark.read.csv(["*.csv"])`) forces Spark to shuffle data across the network to organize it into output partitions, causing "Shuffle Storms" and high I/O costs.


* **The "Fairway" Solution:**
1. **The Grouper (Python):** The Python Coordinator queries the Manifest for all pending files. It applies the configured Regex (e.g., `state=(?P<state>\w+)`) to every file path in memory.
2. **The Batches:** It organizes the files into a dictionary where the key is the partition tuple and the value is the list of files:
```python
batches = {
    ('CT', '2023'): ['s3://.../CT_2023_01.csv', 's3://.../CT_2023_02.csv'],
    ('NY', '2023'): ['s3://.../NY_2023_01.csv', ...]
}

```


3. **The Execution:** The Coordinator loops through this dictionary and calls the Spark Engine **once per batch**.
4. **The Result (No Shuffle):** Spark receives a list of files that *it knows* all belong to "CT/2023". It reads them and writes them directly to the `state=CT/year=2023` output folder. No network shuffle is required because the data is pre-organized.



### **3. Component Design**

* **The Manifest (Source of Truth):**
* Tracks file state (`pending`, `success`, `failed`).
* *Implementation:* **SQLite** for local dev; **DynamoDB/External DB** for Cloud/Kubernetes to handle stateless containers.


* **The Configuration (`schema/{table}.yaml`):**
* Contains the "Business Logic" (Regex patterns) and Schema definitions.
* Decouples code from data structure.


* **The Coordinator (Python):**
* Owns the "Grouper" logic.
* Orchestrates the loop: Fetch  Group  Submit  Commit Status.


* **The "Dumb" Engine (Spark):**
* Strictly I/O.
* Accepts `[list_of_files]` and `{'constant': 'metadata'}`.
* Does **not** know about Regex, patterns, or business rules.



### **4. Observability & Logging**

* **CLI Feedback (User Facing):**
* The CLI must provide real-time visibility into the "Coordinator Loop."
* *Display:* "Processing Batch 5/50: Partition [CT/2023] - 24 Files... SUCCESS."


* **Structured Logging (Debug Facing):**
* Logs must persist exactly *which* files were in a failed batch to allow for targeted debugging.
* *Format:* JSON logs are preferred for ingestion into Splunk/CloudWatch.
* *Context:* Every log entry should include `batch_id`, `partition_key`, and `file_count`.


* **Manifest State:**
* The Manifest itself acts as a log. Users can query it to answer: "Did file X get processed?"



### **5. Infrastructure Compatibility**

* **Kubernetes (EKS/OpenShift):**
* Works natively using `spark-submit --master k8s://`.
* *Constraint:* Manifest must be externalized (PVC or Cloud DB).


* **OpenShift Specifics:**
* Avoid Nextflow to bypass "Double-Layer" permission issues (SCCs).
* Use the **Red Hat Spark Operator** for security compliance.


* **AWS Cloud:**
* **EMR Serverless** is the "Golden Path" (bursts to handle batches, scales to zero).
* **EKS** is preferred for custom Docker images (e.g., piloting Spark 4.1).



### **6. Future-Proofing (Spark 4.1 & SDP)**

* **Goal:** Prepare for **Spark Declarative Pipelines (SDP)**.
* **Strategy:** Strict separation of *Logic* (Python) and *Execution* (Spark).
* **The Migration:**
1. Keep `schema.yaml` (Logic).
2. Delete the Python Coordinator loop.
3. Replace the Engine with an SDP definition (when Spark 4.1 is stable).


* **Constraint:** Do **not** expose the pipeline API to users. Expose only the **CLI** and **YAML**.

### **7. Ambiguities & Trade-offs**

* **Manifest Persistence:** Decision required between SQLite (simple/local) vs. DynamoDB (robust/cloud).
* **Error Granularity:** The plan fails a whole batch if one file is bad. This trades file-level precision for massive performance gains.
* **Compute Target:** Decision required between EMR Serverless (Easy Ops) vs. EKS (High Control).