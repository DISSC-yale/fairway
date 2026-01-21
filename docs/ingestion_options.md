# Ingestion Options

Fairway provides flexible options for handling messy or non-standard data sources.

## Read Options (`read_options`)

You can pass engine-specific options directly to the underlying reader (DuckDB or Spark) using `read_options`.

### Common Use Cases

#### Headerless CSVs
If your file has no header, you must provide a `schema` and set `header: false`. Fairway will map the schema column names to the file.

```yaml
sources:
  - name: "no_header_data"
    path: "data/raw_data.csv"
    format: "csv"
    schema:
      id: "INTEGER"
      value: "DOUBLE"
    read_options:
      header: false
```

#### Custom Delimiters
```yaml
    read_options:
      delim: "|"
      quote: "'"
```

## Preprocessing (`preprocess`)

Fairway can handle compressed or complex files before ingestion using the `preprocess` hook.

### Actions
- **Built-in**: `unzip` (Extracts zip files).
- **Custom Script**: Provide a path to a python file (e.g., `scripts/parser.py`).
    - The script must define a function `process(input_path: str) -> str`.
    - It should return the path to the processed file/directory.

### Global vs Per-File
- **`scope: global`**: Runs ONCE for the source entry.
- **`scope: per_file`**: Runs for EACH file matching the path pattern.

### Execution Modes
- **`execution_mode: driver` (Default)**: Runs locally.
- **`execution_mode: cluster`**: Distributes tasks to Spark Executors.
    - *Requirement*: `engine: pyspark`.
    - *Benefit*: Massively parallel.

### Configuration Example

```yaml
sources:
  - name: "custom_etl"
    path: "data/incoming/*.dat"
    preprocess:
      action: "scripts/decrypt.py" 
      scope: "per_file"
      execution_mode: "cluster" 
    write_mode: "append"
```
