# Test Fixtures

This directory contains test data for Fairway's test suite.

## Directory Structure

```
fixtures/
├── formats/           # Sample files for each supported format
│   ├── csv/
│   │   ├── simple.csv
│   │   └── with_headers.csv
│   ├── tsv/
│   │   └── simple.tsv
│   ├── json/
│   │   ├── records.json
│   │   └── lines.jsonl
│   ├── parquet/
│   │   └── simple.parquet
│   └── fixed_width/   # Implemented - requires spec file
│       ├── simple.txt
│       ├── simple_spec.yaml
│       └── README.md
├── schema_merge/      # Files for testing schema union behavior
│   ├── file_a.csv     # id, name, age
│   ├── file_b.csv     # id, name, city
│   ├── file_c.csv     # id, name, country
│   └── expected_schema.yaml
└── zip_handling/      # Zip extraction test scenarios (created dynamically)
```

## Supported Formats

| Format | Test Files | Notes |
|--------|------------|-------|
| CSV | `formats/csv/simple.csv` | Standard comma-separated |
| TSV | `formats/tsv/simple.tsv` | Tab-separated |
| JSON | `formats/json/records.json` | Array of objects |
| JSONL | `formats/json/lines.jsonl` | Newline-delimited JSON |
| Parquet | `formats/parquet/simple.parquet` | Binary columnar format |
| Fixed-Width | `formats/fixed_width/` | Spec-based (no schema inference) |

## Adding New Format Support

When adding a new file format, use this checklist (see RULE-116 in `rules.md`):

- [ ] Create `fixtures/formats/<format>/simple.<ext>` — Basic test file
- [ ] Create `fixtures/schema_merge/<format>/` — Files with different columns for merge testing
- [ ] Add tests in `test_ingestion_formats.py` — Basic read and ingest tests
- [ ] Add tests in `test_schema_merge.py` — Schema union tests
- [ ] Update this README with the new format

## Standard Test Schema

Most format fixtures use a consistent schema for easy comparison:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Unique identifier |
| `name` | STRING | Name field |
| `value` | INTEGER | Numeric value |

## Schema Merge Test Data

The `schema_merge/` directory contains files designed to test schema union:

- **file_a.csv**: `id, name, age` — unique column: `age`
- **file_b.csv**: `id, name, city` — unique column: `city`
- **file_c.csv**: `id, name, country` — unique column: `country`

Expected merged schema should contain ALL columns: `id, name, age, city, country`
