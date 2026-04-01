"""
Shared test helpers for fairway tests.
Provides: build_config, read_output, read_curated, read_processed, get_output_mtime
"""
import yaml
from pathlib import Path


def build_config(tmp_path, table, engine="duckdb", extra_tables=None, **top_level):
    """
    Write a minimal fairway config to tmp_path/fairway.yaml and return its path.

    Args:
        tmp_path: pytest tmp_path fixture (Path object)
        table: dict — single table definition
        engine: 'duckdb' or 'pyspark'
        extra_tables: list of additional table dicts
        **top_level: extra top-level config keys (e.g. validations={...})

    Returns:
        str — absolute path to the written config file
    """
    tables = [table]
    if extra_tables:
        tables.extend(extra_tables)

    data = {
        "dataset_name": "test",
        "engine": engine,
        "storage": {
            "root": str(tmp_path / "data"),
        },
        "tables": tables,
    }
    data.update(top_level)

    config_path = tmp_path / "fairway.yaml"
    config_path.write_text(yaml.dump(data, default_flow_style=False))
    return str(config_path)


def read_output(output_path):
    """
    Read parquet output from any path. Returns a list of dicts (rows).
    Works regardless of which engine produced the output.
    Uses pyarrow directly — no DuckDB or PySpark dependency.
    """
    import pyarrow.parquet as pq
    output_path = Path(output_path)

    parquet_files = list(output_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {output_path}")

    tables = [pq.read_table(str(f)) for f in parquet_files]
    import pyarrow as pa
    combined = pa.concat_tables(tables, promote_options="default")
    return combined.to_pydict()


def read_as_df(output_path):
    """
    Read parquet output as a pandas DataFrame.
    Useful for sorted/value assertions.
    """
    import pandas as pd
    import pyarrow.parquet as pq
    output_path = Path(output_path)
    parquet_files = list(output_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {output_path}")
    import pyarrow as pa
    tables = [pq.read_table(str(f)) for f in parquet_files]
    combined = pa.concat_tables(tables, promote_options="default")
    return combined.to_pandas()


def read_curated(tmp_path, table_name):
    """Read curated output for a named table as a pandas DataFrame."""
    return read_as_df(Path(tmp_path) / "data" / "curated" / table_name)


def read_processed(tmp_path, table_name):
    """Read processed output for a named table as a pandas DataFrame."""
    return read_as_df(Path(tmp_path) / "data" / "processed" / table_name)


def get_output_mtime(tmp_path, table_name, layer="curated"):
    """Return the max mtime of all parquet files in the named table output."""
    output_path = Path(tmp_path) / "data" / layer / table_name
    parquet_files = list(output_path.rglob("*.parquet"))
    if not parquet_files:
        return None
    return max(f.stat().st_mtime for f in parquet_files)


def row_count(output_path):
    """Count rows in a parquet output directory."""
    df = read_as_df(output_path)
    return len(df)
