import os
import sys
import shutil

# Ensure source is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from fairway.pipeline import IngestionPipeline

def test_flexible_ingestion():
    config_path = "tests/fairway_test_flexible.yaml"
    
    # Clean output
    if os.path.exists("tests/output"):
        shutil.rmtree("tests/output")
        
    pipeline = IngestionPipeline(config_path)
    try:
        pipeline.run()
        print("\nSUCCESS: Pipeline run completed without errors.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\nFAILURE: Pipeline failed with error: {e}")
        sys.exit(1)
        
    # Verify outputs
    # 1. Headerless Source
    chk_path = "tests/output/final/headerless_source.parquet"
    if os.path.exists(chk_path):
        print(f"[OK] Headerless output exists: {chk_path}")
    else:
        print(f"[FAIL] Headerless output missing: {chk_path}")
        
    # 2. Zipped Source
    chk_path = "tests/output/final/zipped_source.parquet"
    if os.path.exists(chk_path):
        print(f"[OK] Zipped output exists: {chk_path}")
    else:
        print(f"[FAIL] Zipped output missing: {chk_path}")
        
    # 3. Custom Script Source
    chk_path = "tests/output/final/custom_script_source.parquet"
    if os.path.exists(chk_path):
        print(f"[OK] Custom Script output exists: {chk_path}")
    else:
         print(f"[FAIL] Custom Script output missing: {chk_path}")

    # 4. Batch Source
    chk_path = "tests/output/final/batch_source.parquet"
    if os.path.exists(chk_path):
        print(f"[OK] Batch Source output exists: {chk_path}")
    else:
         print(f"[FAIL] Batch Source output missing: {chk_path}")

if __name__ == "__main__":
    test_flexible_ingestion()
