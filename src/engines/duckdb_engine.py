import duckdb
import os

class DuckDBEngine:
    def __init__(self):
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("INSTALL httpfs; LOAD httpfs;")
        self.con.execute("INSTALL aws; LOAD aws;") # Or gcs if needed

    def ingest_csv(self, input_path, output_path, partition_by=None, metadata=None):
        """
        Converts CSV to Parquet using DuckDB, with metadata injection.
        """
        # Load data
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW raw_data AS SELECT * FROM read_csv_auto('{input_path}')")
        
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
