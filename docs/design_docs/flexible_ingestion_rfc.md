# RFC: Flexible Data Ingestion

**Status**: Proposed
**Created**: 2026-01-09

## Objective
Refactor the ingestion pipeline to support multiple file formats (CSV, JSON, Parquet, etc.) by decoupling format logic from the pipeline orchestration.

## Proposal

### Generic Engine Interface

Move away from `ingest_csv` to a generic `ingest` method in the base Engine class (DuckDB and PySpark).

```python
# Proposed Interface
class Engine:
    def ingest(self, source_config: dict, output_path: str) -> bool:
        """
        Ingest data from source to output path.
        Dispatches to specific methods (ingest_csv, ingest_json) based on config.
        """
        pass
```

### Configuration Changes
The `config.yaml` `sources` block already supports a `format` key. This will be strictly enforced and used to drive dispatch logic.

```yaml
sources:
  - name: "my_data"
    path: "data/raw/data.json"
    format: "json"
```

### Implementation Plan

1.  **Engine Refactor**:
    *   **DuckDBEngine**: Implement `ingest_json` (using `read_json_auto`) and `ingest_parquet` (using `read_parquet`). Create main `ingest` dispatcher.
    *   **PySparkEngine**: Implement `ingest` dispatcher using `spark.read.format(fmt).load(path)`.

2.  **Pipeline Update**:
    *   Update `src/fairway/pipeline.py` to call `self.engine.ingest(source, ...)` instead of `ingest_csv`.
    *   Remove `.csv` hardcoding in output path generation; use `os.path.splitext` or similar to handle variable input extensions.

3.  **Validation**:
    *   Add tests for JSON and Parquet input sources.
