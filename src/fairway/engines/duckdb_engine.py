try:
    import duckdb
except ImportError as e:
    duckdb = None
    _duckdb_import_error = e

import os

class DuckDBEngine:
    def __init__(self):
        if duckdb is None:
            additional_info = f" Original error: {_duckdb_import_error}" if '_duckdb_import_error' in globals() else ""
            raise ImportError(f"DuckDB is not installed or failed to load. Please install fairway[duckdb] or fairway[all].{additional_info}")
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute("INSTALL aws; LOAD aws;") # Or gcs if needed

    def ingest(self, input_path, output_path, format='csv', partition_by=None, metadata=None, hive_partitioning=False, **kwargs):
        """
        Generic ingestion method that dispatches to format-specific handlers.
        """
        if format == 'csv':
            return self._ingest_csv(input_path, output_path, partition_by, metadata, hive_partitioning)
        elif format == 'json':
            return self._ingest_json(input_path, output_path, partition_by, metadata)
        elif format == 'parquet':
            return self._ingest_parquet(input_path, output_path, partition_by, metadata)
        else:
            raise ValueError(f"Unsupported format for DuckDB engine: {format}")

    def _ingest_csv(self, input_path, output_path, partition_by=None, metadata=None, hive_partitioning=False):
        """
        Converts CSV to Parquet using DuckDB, with metadata injection.
        """
        # Load data
        if hive_partitioning:
             # When hive partitioning is enabled, we need to ensure we read recursively 
             # and enable the option.
             # If input_path is a directory, append '/**/*.csv' logic or similar if implicit?
             # DuckDB read_csv_auto usually handles directories if glob is provided.
             # But if the user provided a directory path without a glob, we might need to append keys.
             # However, assuming config_loader passed the directory path directly for hive_partitioning=True.
             # We should probably append '/**/*.csv' or just '**' if the user didn't provide a pattern.

             # Check if input path looks like a glob. If not, make it recursive.
             if '*' not in input_path:
                 read_path = os.path.join(input_path, "**/*.csv")
             else:
                 read_path = input_path
             
             self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_csv_auto('{read_path}', hive_partitioning=1)")
        else:
             self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_csv_auto('{input_path}')")
        
        return self._write_to_parquet(output_path, partition_by, metadata)

    def _ingest_json(self, input_path, output_path, partition_by=None, metadata=None):
        """
        Converts JSON to Parquet.
        """
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_json_auto('{input_path}')")
        return self._write_to_parquet(output_path, partition_by, metadata)

    def _ingest_parquet(self, input_path, output_path, partition_by=None, metadata=None):
        """
        Pass-through Parquet ingestion (useful for unifying pipeline logic).
        """
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_parquet('{input_path}')")
        return self._write_to_parquet(output_path, partition_by, metadata)

    def _write_to_parquet(self, output_path, partition_by=None, metadata=None):
        # Inject metadata columns if provided
        select_clause = "*"
        if metadata:
            meta_cols = ", ".join([f"'{val}' AS {key}" for key, val in metadata.items()])
            select_clause = f"*, {meta_cols}"

        # Write to Parquet with optional Hive partitioning
        partition_clause = ""
        if partition_by:
            # DuckDB 1.0+ supports PARTITION_BY in COPY
            partition_clause = f", PARTITION_BY ({', '.join(partition_by)})"
            
        self.con.execute(f"""
            COPY (SELECT {select_clause} FROM raw_data) 
            TO '{output_path}' 
            (FORMAT PARQUET{partition_clause}, OVERWRITE TRUE)
        """)
        return True

    def query(self, query):
        return self.con.execute(query).df()

    def read_result(self, path):
        """
        Reads a Parquet result from the given path into a DuckDB Relation (Lazy).
        """
        # Recursively read all parquet files in the path
        return self.con.sql(f"SELECT * FROM '{path}/**/*.parquet'")
