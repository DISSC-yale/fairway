import shutil
import os

def process(file_path, output_dir=None, **kwargs):
    """Copy input file to a prefixed output file, returning the new path."""
    base_dir = output_dir or os.path.dirname(file_path)
    os.makedirs(base_dir, exist_ok=True)
    filename = os.path.basename(file_path)
    output_path = os.path.join(base_dir, f"processed_{filename}")
    shutil.copy2(file_path, output_path)
    return output_path
