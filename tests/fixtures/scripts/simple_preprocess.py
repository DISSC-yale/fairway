"""
Fixture preprocess script.
Copies input file to output directory unchanged.
Proves that fairway's preprocess.action script execution path runs.

Fairway calls this as a module: module.process_file(file_path, output_dir)
"""
import shutil
import os


def process_file(file_path, output_dir, **kwargs):
    """Copy file_path into output_dir unchanged. Returns output_dir (directory)."""
    os.makedirs(output_dir, exist_ok=True)
    dst = os.path.join(output_dir, os.path.basename(file_path))
    shutil.copy(file_path, dst)
    return output_dir


if __name__ == "__main__":
    import sys
    src, dst_dir = sys.argv[1], sys.argv[2]
    process_file(src, dst_dir)
