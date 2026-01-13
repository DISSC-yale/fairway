# test_verification_v3

Initialized by fairway on 2026-01-12T21:45:11.970911

**Engine**: duckdb

## Quick Start

### 1. Generate Test Data

```bash
fairway generate-data --size small --partitioned
```

### 2. Generate Schema (optional)

```bash
fairway generate-schema data/raw/your_data.csv
```

### 3. Update Configuration

Edit `config/fairway.yaml` to define your data sources, validations, and enrichments.

### 4. Run the Pipeline

**Local execution:**
```bash
fairway run --config config/fairway.yaml
```

**Slurm cluster:**
```bash
fairway run --config config/fairway.yaml --profile slurm --with-spark --account YOUR_ACCOUNT
```

## Project Structure

- `config/` - Pipeline configuration files
- `data/raw/` - Input data files
- `data/intermediate/` - Intermediate processing outputs
- `data/final/` - Final processed data
- `src/transformations/` - Custom transformation scripts
- `docs/` - Project documentation
- `logs/` - Execution logs

## Documentation

See the [fairway documentation](https://github.com/DISSC-yale/fairway) for more details.
