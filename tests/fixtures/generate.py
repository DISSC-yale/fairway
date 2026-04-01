"""
Generate binary and archive test fixtures.
Run from repo root: python tests/fixtures/generate.py

Idempotent — safe to re-run, overwrites existing outputs.
Hand-crafted files (CSV, TSV, JSON, fixed-width text, YAML) are NOT
generated here — edit them directly in tests/fixtures/formats/.
"""
import zipfile
import os
from pathlib import Path

FIXTURES = Path(__file__).parent
FORMATS = FIXTURES / "formats"
ZIPPED = FIXTURES / "zipped"


def generate_parquet_fixtures():
    """Write parquet variants from CSV sources using pyarrow."""
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pq

    parquet_dir = FORMATS / "parquet"
    parquet_dir.mkdir(exist_ok=True)

    # simple.parquet — from csv/simple.csv
    table = pa_csv.read_csv(str(FORMATS / "csv" / "simple.csv"))
    pq.write_table(table, str(parquet_dir / "simple.parquet"))
    print("  Written: parquet/simple.parquet")

    # missing_values.parquet — from csv/missing_values.csv
    table = pa_csv.read_csv(str(FORMATS / "csv" / "missing_values.csv"))
    pq.write_table(table, str(parquet_dir / "missing_values.parquet"))
    print("  Written: parquet/missing_values.parquet")

    # bad_schema.parquet — from csv/bad_schema.csv
    table = pa_csv.read_csv(str(FORMATS / "csv" / "bad_schema.csv"))
    pq.write_table(table, str(parquet_dir / "bad_schema.parquet"))
    print("  Written: parquet/bad_schema.parquet")


def generate_zip_fixtures():
    """Create zipped archives of existing fixture files."""
    ZIPPED.mkdir(exist_ok=True)

    zips = {
        "csv_simple.zip": [FORMATS / "csv" / "simple.csv"],
        "csv_missing_values.zip": [FORMATS / "csv" / "missing_values.csv"],
        "tsv_simple.zip": [FORMATS / "tsv" / "simple.tsv"],
        "json_simple.zip": [FORMATS / "json" / "records.json"],
        "fixed_width_simple.zip": [
            FORMATS / "fixed_width" / "simple.txt",
            FORMATS / "fixed_width" / "simple_spec.yaml",
        ],
        "parquet_simple.zip": [FORMATS / "parquet" / "simple.parquet"],
    }

    for zip_name, sources in zips.items():
        zip_path = ZIPPED / zip_name
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for src in sources:
                if src.exists():
                    zf.write(src, src.name)
                else:
                    print(f"  WARNING: source not found, skipping: {src}")
        print(f"  Written: zipped/{zip_name}")


if __name__ == "__main__":
    print("Generating parquet fixtures...")
    generate_parquet_fixtures()
    print("Generating zip fixtures...")
    generate_zip_fixtures()
    print("Done.")
