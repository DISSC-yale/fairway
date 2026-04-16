import hashlib
import importlib.util
import inspect
import logging
import os
import sys

logger = logging.getLogger(__name__)

_EXTRA_ALLOWED_DIRS = []


def _get_allowed_transform_dirs():
    """Get list of directories where transformation scripts are allowed.

    Returns paths relative to current working directory that are safe to load.
    """
    cwd = os.path.realpath(os.getcwd())
    dirs = [
        os.path.join(cwd, 'src', 'transformations'),
        os.path.join(cwd, 'transformations'),
        os.path.join(cwd, 'src'),
    ]
    dirs.extend(_EXTRA_ALLOWED_DIRS)
    return dirs


def _is_path_allowed(script_path):
    """Check if script path is within an allowed directory."""
    real_path = os.path.realpath(script_path)
    for allowed_dir in _get_allowed_transform_dirs():
        allowed_dir = os.path.realpath(allowed_dir)
        if real_path.startswith(allowed_dir + os.sep):
            return True
    return False


def add_allowed_directory(directory):
    """Add an allowed directory for loading transformers (for testing only)."""
    _EXTRA_ALLOWED_DIRS.append(os.path.realpath(directory))


def clear_allowed_directories():
    """Clear extra allowed directories (for testing cleanup)."""
    _EXTRA_ALLOWED_DIRS.clear()


def load_transformer(script_path):
    """Dynamically load a transformer class from a file path.

    Security: Only loads scripts from allowed project directories.
    Isolation: uses the script's full real path hash as the sys.modules key,
    so two scripts with the same basename in different directories don't
    overwrite each other's module.
    """
    if not os.path.exists(script_path):
        logger.error("Transformation script not found: %s", script_path)
        return None

    if not _is_path_allowed(script_path):
        raise ValueError(
            f"Security error: Transformation script must be in project's src/ or transformations/ directory. "
            f"Attempted to load: {script_path}"
        )

    if not script_path.endswith('.py'):
        raise ValueError(f"Security error: Transformation script must be a .py file: {script_path}")

    real_path = os.path.realpath(os.path.abspath(script_path))
    module_key = f"fairway_transformer_{hashlib.md5(real_path.encode()).hexdigest()[:12]}"

    if module_key not in sys.modules:
        spec = importlib.util.spec_from_file_location(module_key, real_path)
        if spec is None:
            logger.error("Could not create module spec for: %s", script_path)
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_key] = module
        spec.loader.exec_module(module)

    module = sys.modules[module_key]
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if (name.endswith('Transformer') or name == 'Transformer') \
                and name != 'BaseTransformer' \
                and obj.__module__ == module_key:
            return obj

    logger.error("No Transformer class found in: %s", script_path)
    return None
