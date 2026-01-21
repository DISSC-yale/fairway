import shutil
import os

def process(file_path):
    """
    Dummy string reverse processor? No, just copy to a .processed file.
    """
    base_dir = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    output_path = os.path.join(base_dir, f"processed_{filename}")
    shutil.copy2(file_path, output_path)
    return output_path
