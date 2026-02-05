import importlib.util
import os
import sys

# Allow additional directories for testing (set via environment variable)
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
    """
    if not os.path.exists(script_path):
        print(f"Transformation script not found: {script_path}")
        return None

    # Security: Validate script is in an allowed directory
    if not _is_path_allowed(script_path):
        raise ValueError(
            f"Security error: Transformation script must be in project's src/ or transformations/ directory. "
            f"Attempted to load: {script_path}"
        )

    # Security: Only allow .py files
    if not script_path.endswith('.py'):
        raise ValueError(f"Security error: Transformation script must be a .py file: {script_path}")

    module_name = os.path.splitext(os.path.basename(script_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # Look for a class that ends with 'Transformer' or just 'Transformer'
    # Ensure the class is defined in this module, not imported from elsewhere (like BaseTransformer)
    for name, obj in module.__dict__.items():
        if isinstance(obj, type) and (name.endswith('Transformer') or name == 'Transformer'):
            if obj.__module__ == module_name:
                return obj

    return None
