"""
Templates are loaded from src/fairway/data/ to ensure they're available
when fairway is installed via pip.

Note on Templates Policy:
All templates MUST be stored as static files in `src/fairway/data/` and loaded
via `_read_data_file`. This file acts as a loader. Do not hardcode large
templates as strings in this file.
"""

import sys
import importlib.resources as pkg_resources

def _read_data_file(filename):
    """Read a file from the fairway.data package."""
    try:
        if sys.version_info >= (3, 9):
            return pkg_resources.files('fairway.data').joinpath(filename).read_text(encoding='utf-8')
        else:
            return pkg_resources.read_text('fairway.data', filename, encoding='utf-8')
    except Exception as e:
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(current_dir, 'data', filename)
        if os.path.exists(data_path):
            with open(data_path, 'r', encoding='utf-8') as f:
                return f.read()
        raise e

# Container/spark template constants (APPTAINER_DEF, DOCKERFILE_TEMPLATE,
# SPARK_YAML_TEMPLATE, DRIVER_TEMPLATE, SPARK_START_TEMPLATE, HPC_SCRIPT)
# were removed in v0.3 Step 2 alongside the container bundle. The whole
# `templates.py` loader is slated for deletion in a later step (see
# PLAN.md categorization table).
DOCKERIGNORE = _read_data_file('.dockerignore')
MAKEFILE_TEMPLATE = _read_data_file('Makefile')
CONFIG_TEMPLATE = _read_data_file('fairway.yaml')

DRIVER_SCHEMA_TEMPLATE = _read_data_file('scripts/driver-schema.sh')
README_TEMPLATE = _read_data_file('README.md')
DOCS_TEMPLATE = _read_data_file('getting-started.md')
