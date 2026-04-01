"""
Fixture preprocess script.
Copies input file to output path unchanged.
Proves that fairway's preprocess.action script execution path runs.
Called by fairway pipeline as: python simple_preprocess.py <src> <dst>
"""
import shutil
import sys

if __name__ == "__main__":
    src, dst = sys.argv[1], sys.argv[2]
    shutil.copy(src, dst)
