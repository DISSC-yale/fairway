#!/usr/bin/env python3
"""
Preprocessing script for IPUMS census data.

Handles:
1. Extracting zip archives
2. Converting IPUMS YAML codebook to Fairway fixed-width spec

This script is designed to be called by Fairway's preprocessing system:
    preprocess:
      action: "scripts/preprocess_ipums.py"
      scope: "per_file"

It can also be run standalone to prepare specs before running Fairway:
    python scripts/preprocess_ipums.py data/raw/census/*.zip --specs-dir schema/
"""

import argparse
import logging
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import yaml

# Setup logging
LOG_DIR = Path("logs/preprocess")
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"preprocess_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)


def ipums_to_fairway_type(var: dict) -> str:
    """Convert IPUMS variable metadata to Fairway/DuckDB type."""
    if var.get("is_string_var", False):
        return "VARCHAR"
    if var.get("is_double_var", False):
        return "DOUBLE"
    return "BIGINT"


def convert_ipums_yaml(ipums_vars: list, record_type: str | None = None) -> dict:
    """
    Convert IPUMS YAML format to Fairway fixed-width spec format.

    Args:
        ipums_vars: List of IPUMS variable definitions
        record_type: Filter by record type ('H' or 'P'), None for all

    Returns:
        dict: Fairway-format spec with 'columns' list and optional 'record_type_filter'
    """
    columns = []
    rectype_info = None

    for var in ipums_vars:
        # Capture RECTYPE column position for filtering hierarchical data
        if var["name"].upper() == "RECTYPE":
            rectype_info = {
                "position": var["start_column"] - 1,  # 0-indexed
                "length": var["width"]
            }

        if record_type and var.get("record_type") != record_type:
            continue

        # IPUMS is 1-indexed, Fairway is 0-indexed
        start = var["start_column"] - 1

        columns.append({
            "name": var["name"].lower(),
            "start": start,
            "length": var["width"],
            "type": ipums_to_fairway_type(var),
            "trim": True,
        })

    columns.sort(key=lambda x: x["start"])

    result = {"columns": columns}

    # Add record_type_filter for hierarchical data
    if record_type and rectype_info:
        result["record_type_filter"] = {
            "position": rectype_info["position"],
            "length": rectype_info["length"],
            "value": record_type
        }

    return result


def process_zip(zip_path: str, output_dir: str, specs_dir: str | None = None, spec_name: str | None = None, password_file: str | None = None) -> str:
    """
    Process an IPUMS zip file: extract data and convert YAML spec.

    Args:
        zip_path: Path to the zip file
        output_dir: Directory to extract data files to
        specs_dir: Directory to write spec files to (default: 'specs/' relative to CWD)
        spec_name: Output spec filename (default: derived from zip name)
        password_file: Path to text file containing zip password (for encrypted zips)

    Returns:
        Path to the extracted data file
    """
    logger.info("=" * 60)
    logger.info("PREPROCESS START")
    logger.info("=" * 60)
    logger.info(f"Input zip: {zip_path}")
    logger.info(f"Output dir: {output_dir}")
    logger.info(f"Specs dir: {specs_dir}")
    logger.info(f"Password file: {password_file}")
    logger.info(f"CWD: {os.getcwd()}")

    zip_path = Path(zip_path)
    output_dir = Path(output_dir)

    if not zip_path.exists():
        logger.error(f"Zip file does not exist: {zip_path}")
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    if specs_dir:
        specs_dir = Path(specs_dir)
    else:
        # Default: 'schema/' relative to current working directory (project root)
        specs_dir = Path("schema")

    logger.info(f"Resolved specs_dir: {specs_dir.absolute()}")
    logger.info(f"Resolved output_dir: {output_dir.absolute()}")

    specs_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Created directories: specs_dir={specs_dir.exists()}, output_dir={output_dir.exists()}")

    data_file = None
    yaml_content = None
    yaml_name = None

    # Check if already extracted: look for .dat files in output_dir
    skip_extraction = False
    existing_dat = list(output_dir.glob("*.dat"))
    if existing_dat:
        logger.info(f"Already extracted: found {len(existing_dat)} .dat file(s) in {output_dir}, skipping extraction")
        data_file = str(existing_dat[0])
        skip_extraction = True

    # Handle encrypted zips
    password = None
    if password_file:
        pwd_path = Path(password_file)
        if pwd_path.exists():
            password = pwd_path.read_text().strip().encode('utf-8')
            logger.info(f"Loaded password from: {password_file}")
        else:
            logger.warning(f"Password file not found: {password_file}")

    logger.info(f"Opening zip file: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        if password:
            zf.setpassword(password)
        logger.info(f"Zip contents: {zf.namelist()[:10]}...")  # First 10 entries

        for name in zf.namelist():
            # Skip directories
            if name.endswith('/'):
                continue

            basename = os.path.basename(name)

            # Extract data file (.dat)
            if basename.endswith('.dat') and not skip_extraction:
                target = output_dir / basename
                logger.info(f"Extracting .dat file: {name} -> {target}")
                with zf.open(name) as src, open(target, 'wb') as dst:
                    # Stream copy for large files
                    chunk_size = 1024 * 1024  # 1MB chunks
                    total_bytes = 0
                    while True:
                        chunk = src.read(chunk_size)
                        if not chunk:
                            break
                        dst.write(chunk)
                        total_bytes += len(chunk)
                logger.info(f"Extracted {total_bytes:,} bytes to {target}")
                data_file = str(target)

            # Read YAML codebook
            elif basename.endswith('.yml') or basename.endswith('.yaml'):
                yaml_name = basename
                logger.info(f"Reading YAML codebook: {name}")
                for handler in logging.root.handlers:
                    handler.flush()
                try:
                    with zf.open(name) as src:
                        raw_bytes = src.read()
                        logger.info(f"Read {len(raw_bytes):,} bytes from YAML file")
                        for handler in logging.root.handlers:
                            handler.flush()
                        yaml_content = yaml.safe_load(raw_bytes)
                    logger.info(f"YAML loaded: type={type(yaml_content)}, len={len(yaml_content) if isinstance(yaml_content, list) else 'N/A'}")
                except Exception as e:
                    logger.error(f"Failed to parse YAML codebook: {e}")
                    for handler in logging.root.handlers:
                        handler.flush()
                    raise

    # Convert YAML to Fairway spec
    # IPUMS YAML can be either a flat list of variables or a dict with variable list nested
    record_types = None
    if yaml_content and isinstance(yaml_content, dict):
        logger.info(f"YAML is a dict with keys: {list(yaml_content.keys())}")
        # Capture metadata before extracting the variable list
        record_types = yaml_content.get('record_types')
        data_structure = yaml_content.get('data_structure')
        if record_types:
            logger.info(f"IPUMS data_structure={data_structure}, record_types={record_types}")
        # Try common IPUMS dict keys that contain the variable list
        for key in ('variable_infos', 'variables', 'var_info', 'cols', 'columns'):
            if key in yaml_content and isinstance(yaml_content[key], list):
                logger.info(f"Extracting variable list from key '{key}' ({len(yaml_content[key])} items)")
                yaml_content = yaml_content[key]
                break
        else:
            # If no known key found, check if any value is a list of dicts with 'start_column'
            for key, val in yaml_content.items():
                if isinstance(val, list) and val and isinstance(val[0], dict) and 'start_column' in val[0]:
                    logger.info(f"Found variable list under key '{key}' ({len(val)} items)")
                    yaml_content = val
                    break

    if yaml_content and isinstance(yaml_content, list):
        # Derive base name from YAML codebook name (more reliable than zip name)
        # Strip trailing letter (a/b/c) to get canonical name: us1950a -> us1950, us1950b -> us1950
        if not spec_name:
            # Prefer deriving from YAML filename (e.g., us1950b_usa_res.yml -> us1950)
            if yaml_name:
                base_name = Path(yaml_name).stem.split('_')[0]  # e.g., "us1950b" from "us1950b_usa_res.yml"
            else:
                base_name = zip_path.stem.split('_')[0]  # fallback to zip name
            # Remove trailing letter if it's a/b/c/d (common IPUMS splits)
            if base_name and base_name[-1] in 'abcd' and len(base_name) > 1:
                base_name = base_name[:-1]  # "us1950b" -> "us1950"
        else:
            base_name = Path(spec_name).stem.replace('_spec', '')

        # For hierarchical IPUMS data with multiple record types (H=household, P=person),
        # generate separate specs for each record type
        if record_types and len(record_types) > 1:
            logger.info(f"Hierarchical data with {len(record_types)} record types: {record_types}")
            specs_to_generate = record_types  # e.g., ['H', 'P']
        else:
            specs_to_generate = [None]  # Single record type, no filter needed

        for record_type in specs_to_generate:
            if record_type:
                current_spec_name = f"{base_name}_{record_type}_spec.yaml"
            else:
                current_spec_name = f"{base_name}_spec.yaml"

            spec_path = specs_dir / current_spec_name
            logger.info(f"Spec output path: {spec_path.absolute()}")

            # Only generate spec if it doesn't exist (same schema across a/b splits)
            if spec_path.exists():
                logger.info(f"Spec already exists: {spec_path} (skipping generation)")
            else:
                logger.info(f"Converting IPUMS YAML to Fairway spec (record_type={record_type})...")
                fairway_spec = convert_ipums_yaml(yaml_content, record_type=record_type)
                logger.info(f"Converted {len(fairway_spec['columns'])} columns")

                with open(spec_path, 'w') as f:
                    yaml.dump(fairway_spec, f, default_flow_style=False, sort_keys=False)

                logger.info(f"Wrote spec file: {spec_path}")
    else:
        logger.warning(f"No YAML codebook found or invalid format in {zip_path}")

    if not data_file:
        logger.warning(f"No .dat file found in {zip_path}")
        return str(output_dir)

    logger.info("=" * 60)
    logger.info(f"PREPROCESS COMPLETE: {data_file}")
    logger.info("=" * 60)

    return data_file


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess IPUMS census zip files"
    )
    parser.add_argument(
        "input",
        nargs="+",
        type=Path,
        help="Input zip file(s)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=Path("data/extracted"),
        help="Directory for extracted data files"
    )
    parser.add_argument(
        "--specs-dir", "-s",
        type=Path,
        default=Path("schema"),
        help="Directory for generated spec files"
    )

    args = parser.parse_args()

    for zip_file in args.input:
        if not zip_file.exists():
            logger.error(f"File not found: {zip_file}")
            continue

        process_zip(zip_file, args.output_dir, args.specs_dir)


# Fairway preprocessing entry point
def process_file(file_path: str, output_dir: str, **kwargs) -> str:
    """
    Entry point for Fairway preprocessing system.

    Called when configured as:
        preprocess:
          action: "scripts/preprocess_ipums.py"
          password_file: "/path/to/password.txt"  # optional, for encrypted zips
          specs_dir: "specs/"  # optional, directory for generated spec files
    """
    logger.info(f"Fairway preprocessing entry point called")
    logger.info(f"  file_path: {file_path}")
    logger.info(f"  output_dir: {output_dir}")
    logger.info(f"  kwargs: {kwargs}")

    # Skip non-zip files (already decompressed)
    if not zipfile.is_zipfile(file_path):
        logger.info(f"Not a zip file, skipping extraction: {file_path}")
        return file_path

    password_file = kwargs.get('password_file')
    specs_dir = kwargs.get('specs_dir')
    return process_zip(file_path, output_dir, specs_dir=specs_dir, password_file=password_file)


if __name__ == "__main__":
    main()
