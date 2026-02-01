"""
Fixed-width file support for Fairway.

This module provides utilities for reading fixed-width text files where columns
are defined by character positions rather than delimiters.

Spec file format (YAML):
    columns:
      - name: id
        start: 0
        length: 3
        type: INTEGER
        trim: false  # optional, default false
      - name: name
        start: 3
        length: 20
        type: VARCHAR
        trim: true   # strip whitespace
"""

import yaml
import os
import re

# Valid SQL identifier pattern (letters, digits, underscores, must start with letter/underscore)
VALID_IDENTIFIER = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Supported types across engines
VALID_TYPES = {
    'INTEGER', 'INT',
    'BIGINT', 'LONG',
    'DOUBLE', 'FLOAT',
    'VARCHAR', 'STRING',
    'DATE', 'TIMESTAMP',
    'BOOLEAN', 'BOOL'
}


class FixedWidthSpecError(Exception):
    """Raised when fixed-width spec validation fails."""
    def __init__(self, errors):
        self.errors = errors
        message = "Fixed-width spec validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)


def load_spec(spec_path, config_dir=None):
    """
    Load and validate a fixed-width column specification from a YAML file.

    Args:
        spec_path: Path to the spec YAML file (absolute or relative to config_dir)
        config_dir: Optional base directory for relative path resolution

    Returns:
        dict: Validated spec with 'columns' list and computed 'line_length'

    Raises:
        FileNotFoundError: If spec file doesn't exist
        FixedWidthSpecError: If spec validation fails
    """
    # Resolve path
    if config_dir and not os.path.isabs(spec_path):
        resolved_path = os.path.join(config_dir, spec_path)
    else:
        resolved_path = spec_path

    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"Fixed-width spec file not found: {resolved_path}")

    with open(resolved_path, 'r') as f:
        spec = yaml.safe_load(f)

    # Validate and enrich the spec
    validated = validate_spec(spec)
    validated['_spec_path'] = resolved_path

    return validated


def validate_spec(spec):
    """
    Validate a fixed-width column specification.

    Checks:
        - Required fields present (name, start, length)
        - No overlapping columns
        - Valid types (if specified)
        - Reasonable values (non-negative start/length)

    Args:
        spec: Dict with 'columns' key containing list of column definitions

    Returns:
        dict: Enriched spec with computed line_length and defaults applied

    Raises:
        FixedWidthSpecError: If validation fails
    """
    errors = []

    if not spec:
        errors.append("Spec is empty")
        raise FixedWidthSpecError(errors)

    columns = spec.get('columns')
    if not columns:
        errors.append("Missing required 'columns' key")
        raise FixedWidthSpecError(errors)

    if not isinstance(columns, list):
        errors.append("'columns' must be a list")
        raise FixedWidthSpecError(errors)

    # Track column positions for overlap detection
    positions = []  # List of (start, end, name) tuples
    validated_columns = []

    for i, col in enumerate(columns):
        prefix = f"columns[{i}]"

        # Required: name
        name = col.get('name')
        if not name:
            errors.append(f"{prefix}: Missing required 'name' field")
            continue

        # Validate name is a valid SQL identifier (RULE-119: prevent injection)
        if not VALID_IDENTIFIER.match(name):
            errors.append(f"{prefix}: Invalid column name '{name}' - must be valid identifier (letters, digits, underscores)")
            continue

        prefix = f"columns['{name}']"

        # Required: start
        start = col.get('start')
        if start is None:
            errors.append(f"{prefix}: Missing required 'start' field")
            continue
        if not isinstance(start, int) or start < 0:
            errors.append(f"{prefix}: 'start' must be a non-negative integer, got {start}")
            continue

        # Required: length
        length = col.get('length')
        if length is None:
            errors.append(f"{prefix}: Missing required 'length' field")
            continue
        if not isinstance(length, int) or length <= 0:
            errors.append(f"{prefix}: 'length' must be a positive integer, got {length}")
            continue

        end = start + length

        # Check for overlaps
        for existing_start, existing_end, existing_name in positions:
            if not (end <= existing_start or start >= existing_end):
                errors.append(
                    f"{prefix}: Overlaps with column '{existing_name}' "
                    f"(positions {start}-{end} vs {existing_start}-{existing_end})"
                )

        positions.append((start, end, name))

        # Optional: type (default to STRING/VARCHAR)
        col_type = col.get('type', 'VARCHAR').upper()
        if col_type not in VALID_TYPES:
            errors.append(f"{prefix}: Invalid type '{col_type}'. Must be one of {sorted(VALID_TYPES)}")

        # Optional: trim (default False - preserve whitespace)
        trim = col.get('trim', False)
        if not isinstance(trim, bool):
            errors.append(f"{prefix}: 'trim' must be a boolean, got {trim}")
            trim = False

        validated_columns.append({
            'name': name,
            'start': start,
            'length': length,
            'type': col_type,
            'trim': trim
        })

    if errors:
        raise FixedWidthSpecError(errors)

    # Compute expected line length (for validation during reading)
    if validated_columns:
        line_length = max(col['start'] + col['length'] for col in validated_columns)
    else:
        line_length = 0

    return {
        'columns': validated_columns,
        'line_length': line_length
    }


def infer_types_from_data(data_lines, spec, sample_size=1000):
    """
    Optionally infer column types from data when not specified in spec.

    This is a fallback for when spec file doesn't include type information.
    Type inference examines actual data values to guess appropriate types.

    Args:
        data_lines: Iterable of data lines (strings)
        spec: Validated spec dict with columns
        sample_size: Number of lines to sample for inference

    Returns:
        dict: Updated spec with inferred types
    """
    columns = spec['columns']

    # Track candidate types per column
    type_candidates = {col['name']: set() for col in columns}

    for i, line in enumerate(data_lines):
        if i >= sample_size:
            break

        for col in columns:
            start = col['start']
            end = start + col['length']

            # Extract value (handle short lines gracefully for inference)
            if len(line) >= end:
                value = line[start:end].strip()
            elif len(line) > start:
                value = line[start:].strip()
            else:
                continue

            if not value:
                continue

            # Guess type from value
            candidates = type_candidates[col['name']]

            # Try INTEGER
            try:
                int(value)
                candidates.add('INTEGER')
                continue
            except ValueError:
                pass

            # Try DOUBLE/FLOAT
            try:
                float(value)
                candidates.add('DOUBLE')
                continue
            except ValueError:
                pass

            # Default to STRING
            candidates.add('VARCHAR')

    # Resolve types using hierarchy (STRING > DOUBLE > INTEGER)
    TYPE_PRIORITY = {'VARCHAR': 3, 'DOUBLE': 2, 'INTEGER': 1}

    for col in columns:
        candidates = type_candidates[col['name']]
        if candidates:
            # Pick the most general type that fits all values
            col['type'] = max(candidates, key=lambda t: TYPE_PRIORITY.get(t, 0))
        # else keep existing type from spec

    return spec
