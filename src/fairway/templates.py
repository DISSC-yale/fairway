"""Template files for fairway init command.

These are loaded from src/fairway/data/ to ensure they're available
when fairway is installed via pip.

Note on Templates Policy:
All templates MUST be stored as static files in `src/fairway/data/` and loaded
via `_read_data_file`. This file acts as a loader. Do not hardcode large
templates as strings in this file.
"""

import sys
if sys.version_info < (3, 9):
    import importlib.resources as pkg_resources
else:
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

HPC_SCRIPT = _read_data_file('fairway-hpc.sh')
APPTAINER_DEF = _read_data_file('Apptainer.def')
DOCKERFILE_TEMPLATE = _read_data_file('Dockerfile')
DOCKERIGNORE = _read_data_file('.dockerignore')
NEXTFLOW_CONFIG = _read_data_file('nextflow.config')
MAIN_NF = _read_data_file('main.nf')
MAKEFILE_TEMPLATE = _read_data_file('Makefile')
CONFIG_TEMPLATE = _read_data_file('fairway.yaml')
SPARK_YAML_TEMPLATE = _read_data_file('spark.yaml')

TRANSFORM_TEMPLATE = _read_data_file('example_transform.py')
DRIVER_TEMPLATE = _read_data_file('driver.sh')
RUN_PIPELINE_SCRIPT = _read_data_file('run_pipeline.sh')
README_TEMPLATE = _read_data_file('README.md')
DOCS_TEMPLATE = _read_data_file('getting-started.md')
