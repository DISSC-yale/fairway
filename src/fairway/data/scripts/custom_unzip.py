import os
import zipfile
import shutil

def process(file_path):
    """
    Example custom preprocessing script.
    
    Args:
        file_path (str): Absolute path to the input file (e.g., the .zip file).
        
    Returns:
        str: Absolute path to the processed output (directory or file) that Fairway should ingest.
    """
    print(f"Custom Preprocessor: Processing {file_path}")
    
    # 1. Determine Output Directory
    # We create a hidden directory next to the original file to hold extracted contents.
    # This keeps things local and compatible with relative path logic if using 'root'.
    base_dir = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    name_no_ext = os.path.splitext(file_name)[0]
    
    # Example: data/raw/.preprocessed_VM2Uniform--UT--2021-07-08
    output_dir = os.path.join(base_dir, f".preprocessed_{name_no_ext}")
    
    # 2. Clean/Create Output Directory
    if os.path.exists(output_dir):
        # Optional: Skip if already extracted? 
        # But Fairway handles caching via Manifest V2 before calling us.
        # So if we are here, we generally should re-process.
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # 3. Unzip Logic
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
            
        print(f"  Extracted to: {output_dir}")
        
        # 4. Return the path to be consumed by the pipeline
        # If the zip contains a single CSV, we might want to return that specific file path.
        # If it contains many files, return the directory or a glob pattern.
        
        # Option A: Return directory (Fairway will ingest all supported files in it)
        return f"{output_dir}/*" 
        
        # Option B: Return specific file if structure is known
        # return os.path.join(output_dir, "data.csv")
    else:
        print("  Not a zip file, returning original path.")
        return file_path
