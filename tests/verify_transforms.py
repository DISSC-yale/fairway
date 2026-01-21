import os
import shutil
import pandas as pd
import duckdb
from fairway.pipeline import IngestionPipeline

TEST_DIR = "test_multi_transform"

def setup():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    
    os.makedirs(TEST_DIR)
    os.makedirs(os.path.join(TEST_DIR, "data/raw"), exist_ok=True)
    os.makedirs(os.path.join(TEST_DIR, "src/transformations"), exist_ok=True)
    os.makedirs(os.path.join(TEST_DIR, "data/intermediate"), exist_ok=True)
    os.makedirs(os.path.join(TEST_DIR, "data/final"), exist_ok=True)

    # 1. Create Data
    df_a = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    df_b = pd.DataFrame({"id": [3, 4], "val": ["c", "d"]})
    
    df_a.to_csv(os.path.join(TEST_DIR, "data/raw/source_a.csv"), index=False)
    df_b.to_csv(os.path.join(TEST_DIR, "data/raw/source_b.csv"), index=False)
    
    # 2. Create Transformers
    trans_a_code = """
from fairway.transformations.base import BaseTransformer

class ATransformer(BaseTransformer):
    def transform(self):
        print(f"DEBUG: ATransformer input columns: {self.df.columns}")
        self.df['source_tag'] = 'AA'
        print(f"DEBUG: ATransformer output columns: {self.df.columns}")
        return self.df
"""
    trans_b_code = """
from fairway.transformations.base import BaseTransformer

class BTransformer(BaseTransformer):
    def transform(self):
        print(f"DEBUG: BTransformer input columns: {self.df.columns}")
        self.df['source_tag'] = 'BB'
        return self.df
"""
    with open(os.path.join(TEST_DIR, "src/transformations/trans_a.py"), "w") as f:
        f.write(trans_a_code)
    with open(os.path.join(TEST_DIR, "src/transformations/trans_b.py"), "w") as f:
        f.write(trans_b_code)

    # 3. Create Config
    config_yaml = f"""
dataset_name: "test_multi_transform"
engine: "duckdb"

storage:
  raw_dir: "data/raw"
  intermediate_dir: "data/intermediate"
  final_dir: "data/final"

sources:
  - name: "source_a"
    path: "data/raw/source_a.csv"
    format: "csv"
    transformation: "src/transformations/trans_a.py"

  - name: "source_b"
    path: "data/raw/source_b.csv"
    format: "csv"
    transformation: "src/transformations/trans_b.py"

validations:
  level1:
    min_rows: 1
"""
    with open(os.path.join(TEST_DIR, "fairway.yaml"), "w") as f:
        f.write(config_yaml)
        
    return os.path.join(TEST_DIR, "fairway.yaml")

def verify():
    con = duckdb.connect()
    
    print("\n--- Verifying Source A ---")
    
    # Check Ingested (Bronze) - Should NOT have source_tag
    ingest_path = os.path.join(TEST_DIR, "data/intermediate/source_a.parquet")
    if not os.path.exists(ingest_path):
        print(f"FAILED: Ingest path missing: {ingest_path}")
        return False
        
    df_ingest = con.execute(f"SELECT * FROM '{ingest_path}'").df()
    if 'source_tag' in df_ingest.columns:
        print("FAILED: Ingested file A has 'source_tag' column (should be raw copy).")
        return False
    print("PASS: Ingested file A is raw.")

    # Check Processed (Silver/temp) - Should have source_tag 'AA'
    processed_path = os.path.join(TEST_DIR, "data/intermediate/source_a_processed.parquet")
    if not os.path.exists(processed_path):
        print(f"FAILED: Processed path missing: {processed_path}")
        return False
        
    df_proc = con.execute(f"SELECT * FROM '{processed_path}'").df()
    if 'source_tag' not in df_proc.columns or df_proc['source_tag'][0] != 'AA':
        print(f"FAILED: Processed file A missing 'source_tag' or wrong value: {df_proc}")
        return False
    print("PASS: Processed file A transformed correctly.")

    # Check Final (Gold) - Should match Processed
    final_path = os.path.join(TEST_DIR, "data/final/source_a.parquet")
    if not os.path.exists(final_path):
        # Try finding whatever generic parquet if name mismatch
        print(f"FAILED: Final path missing: {final_path}")
        return False
        
    df_final = con.execute(f"SELECT * FROM '{final_path}'").df()
    if 'source_tag' not in df_final.columns or df_final['source_tag'][0] != 'AA':
        print("FAILED: Final file A missing 'source_tag'.")
        return False
    print("PASS: Final file A finalized correctly.")

    print("\n--- Verifying Source B ---")
    # Just check Final for B to ensure separate transform worked
    final_path_b = os.path.join(TEST_DIR, "data/final/source_b.parquet")
    df_final_b = con.execute(f"SELECT * FROM '{final_path_b}'").df()
    if 'source_tag' not in df_final_b.columns or df_final_b['source_tag'][0] != 'BB':
        print("FAILED: Final file B missing 'source_tag' or wrong value (should be BB).")
        return False
    print("PASS: Final file B finalized correctly.")
    
    return True

if __name__ == "__main__":
    try:
        config_path = setup()
        print("Setup complete. Running pipeline...")
        
        # Switch to test directory to ensure relative paths (like manifest) work as expected
        original_cwd = os.getcwd()
        os.chdir(TEST_DIR)
        
        try:
            # Config path is now relative to new CWD
            pipeline = IngestionPipeline("fairway.yaml")
            pipeline.run()
        finally:
            os.chdir(original_cwd)
        
        if verify():
            print("\nSUCCESS: All verifications passed.")
        else:
            print("\nFAILURE: Verification failed.")
            exit(1)
            
    finally:
        # cleanup
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)
