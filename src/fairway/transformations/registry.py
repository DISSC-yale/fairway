import importlib.util
import os
import sys

def load_transformer(script_path):
    """Dynamically load a transformer class from a file path."""
    if not os.path.exists(script_path):
        print(f"Transformation script not found: {script_path}")
        return None

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
