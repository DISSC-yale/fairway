# {name}

Initialized by fairway on {timestamp}

**Engine**: {engine}

## Quick Start

> **Note:** If you are using a virtual environment, ensure it is activated (`source .venv/bin/activate`) before running commands.


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
make run
```

**Slurm cluster:**
```bash
make run-hpc
```
*Note: This automatically detects if you are using Spark or DuckDB. If Spark, it handles cluster provisioning for you.*

### 5. Manual Spark Management (Debugging)

If you need to manually manage the Spark cluster on HPC:

```bash
# Start cluster
fairway spark start --slurm-nodes 2

# Stop cluster
fairway spark stop
```

**Containerized execution:**
```bash
nextflow run main.nf -profile apptainer
```

## Extending the Pipeline

To add a post-processing step (e.g., reshaping), edit `main.nf`. For example:

```groovy
process RESHAPE {{
    input:
    path "data/final/*"
 
    output:
    path "data/reshaped/*"
 
    script:
    \"\"\"
    python3 src/reshape.py ...
    \"\"\"
}}

workflow {{
    // ... existing ...
    RESHAPE(run_fairway.out)
}}
```
