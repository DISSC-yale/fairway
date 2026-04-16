import hashlib
import importlib.util
import inspect
import logging
import os
import sys
import threading

logger = logging.getLogger(__name__)

_EXTRA_ALLOWED_DIRS = []
_LOAD_LOCK = threading.Lock()


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

    Security: only loads scripts from allowed project directories.
    Isolation: keys sys.modules by sha256(realpath)[:12], so scripts with
    the same basename in different directories do not collide.
    Freshness: re-execs on every call so edits to the script between loads
    are picked up. Load+exec is serialized via a lock to avoid a second
    thread observing a partially-initialized module.
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
    module_key = f"fairway_transformer_{hashlib.sha256(real_path.encode()).hexdigest()[:12]}"

    with _LOAD_LOCK:
        spec = importlib.util.spec_from_file_location(module_key, real_path)
        if spec is None:
            logger.error("Could not create module spec for: %s", script_path)
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_key] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_key, None)
            raise

    candidates = []
    for name, obj in vars(module).items():
        if not inspect.isclass(obj):
            continue
        if name == 'BaseTransformer':
            continue
        if not (name.endswith('Transformer') or name == 'Transformer'):
            continue
        if obj.__module__ != module_key:
            continue
        candidates.append(obj)

    if not candidates:
        logger.error("No Transformer class found in: %s", script_path)
        return None
    if len(candidates) > 1:
        names = ", ".join(c.__name__ for c in candidates)
        raise ValueError(
            f"Transformation script must define exactly one Transformer class; "
            f"found {len(candidates)} ({names}) in {script_path}"
        )
    return candidates[0]
