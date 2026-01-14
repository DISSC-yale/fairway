# RFC: Engine Abstraction & Read Unification

**Status**: Proposed
**Date**: 2026-01-14
**Context**: Spark SQL Interception & Compatibility

## Problem
Currently, `pipeline.py` generates generic SQL queries to read validation data:
```python
df = self.engine.query(f"SELECT * FROM '{query_path}'")
```

This works natively for **DuckDB** (which supports `FROM 'path/to/file'`), but fails for **Spark SQL**, which requires specific syntax like `FROM parquet.\`path\``.
To solve this, `PySparkEngine` currently implements an **Interception Layer**: it uses regex to parse the incoming SQL, extracts the path, and redirects the call to `pyspark.read.parquet(path)`.

## Issues
1.  **Fragility**: Relies on regex parsing of SQL strings. If `pipeline.py` changes the query structure (e.g. adds columns, aliases), the interceptor might fail.
2.  **Security**: While low risk in the current containerized environment, parsing raw SQL strings always carries a risk of injection or misinterpretation if not handled carefully.
3.  **Inconsistency**: Logic for "how to read a file" is split between the pipeline (constructing the path/glob) and the engine (interpreting it).

## Proposed Solution: Explicit Read Method
Refactor the `Engine` interface to support an explicit `read_table` or `load_dataframe` method, removing the need for `pipeline.py` to construct SQL for simple reads.

### Interface Changes

**Base Engine / Protocol**
```python
class Engine:
    def read_table(self, path: str, format: str = 'parquet', recursive: bool = True) -> Any:
        """
        Reads a table from a path into the engine's native DataFrame/Relation format.
        """
        raise NotImplementedError
```

### Implementation

**DuckDB Implementation**
```python
class DuckDBEngine(Engine):
    def read_table(self, path: str, format: str = 'parquet', recursive: bool = True):
        # DuckDB handles globs and format inference nicely
        wildcard = "/**" if recursive else ""
        ext = f".{format}"
        full_path = f"{path}{wildcard}{ext}" 
        return self.conn.sql(f"SELECT * FROM '{full_path}'")
```

**PySpark Implementation**
```python
class PySparkEngine(Engine):
    def read_table(self, path: str, format: str = 'parquet', recursive: bool = True):
        # Spark handles directory reading natively usage read API
        reader = self.spark.read.format(format)
        
        # Determine strict path (remove globs as Spark expects directory root for partitioning)
        clean_path = self._clean_path_for_spark(path)
        
        return reader.load(clean_path).toPandas()
```

### Benefits
1.  **Security**: Removes the need to parse SQL strings for file paths.
2.  **Portability**: Engines encapsulate their specific file reading quirks (globs vs directory roots).
3.  **Simplicity**: `pipeline.py` becomes cleaner:
    ```python
    # Old
    # df = self.engine.query(f"SELECT * FROM '{query_path}'")
    
    # New
    df = self.engine.read_table(output_path, fmt)
    ```

## Migration Plan
1.  Define the `read_table` method in existing `PySparkEngine` and `DuckDBEngine`.
2.  Update `pipeline.py` to call `read_table` instead of `query` for the validation/enrichment loading step.
3.  Retain `query` method solely for actual SQL transformations/aggregations, if needed.
