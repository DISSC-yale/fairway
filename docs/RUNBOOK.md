# Operations Runbook

Deployment procedures, monitoring, troubleshooting, and rollback procedures for Fairway pipelines.

## Deployment Procedures

### Local Deployment

1. **Install Fairway**
   ```bash
   pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[duckdb]"
   ```

2. **Initialize Project**
   ```bash
   fairway init my_project
   cd my_project
   ```

3. **Configure Pipeline**
   - Edit `config/fairway.yaml` with your data sources
   - Set `engine: duckdb` for local execution

4. **Run Pipeline**
   ```bash
   fairway run
   ```

### HPC/Slurm Deployment

1. **Install with Spark Support**
   ```bash
   pip install "git+https://github.com/DISSC-yale/fairway.git#egg=fairway[spark]"
   ```

2. **Initialize Project**
   ```bash
   fairway init my_project --engine spark
   cd my_project
   ```

3. **Configure Resources** (`config/spark.yaml`)
   ```yaml
   nodes: 2
   cpus_per_node: 32
   mem_per_node: "200G"
   account: "your_slurm_account"
   partition: "day"
   time: "24:00:00"
   ```

4. **Build Container** (recommended for reproducibility)
   ```bash
   fairway build
   ```

5. **Submit Pipeline**
   ```bash
   fairway submit --with-spark
   ```

### Container Deployment

1. **Build Container Image**
   ```bash
   # Apptainer (preferred for HPC)
   fairway build --apptainer

   # Docker (alternative)
   fairway build --docker
   ```

2. **Pull Pre-built Image**
   ```bash
   fairway pull
   ```

3. **Run Inside Container**
   ```bash
   fairway shell
   # Inside container:
   fairway run
   ```

## Monitoring

### Job Status

```bash
# Check running jobs
fairway status

# View detailed job info (Slurm)
squeue -u $USER --format="%.18i %.9P %.30j %.8T %.10M %.6D %R"

# Check Spark cluster status
fairway spark status
```

### Logs

```bash
# View all logs
fairway logs

# View last 20 entries
fairway logs --last 20

# View errors only
fairway logs --errors

# Filter by batch ID
fairway logs --batch claims_CT_2023

# Raw JSON output for advanced filtering
fairway logs --json | jq 'select(.level == "ERROR")'
```

### Manifest (Processed Files)

```bash
# Show manifest entries
fairway manifest show

# Query by status
fairway manifest query --status completed
fairway manifest query --status failed

# Query by batch
fairway manifest query --batch claims_2023
```

### Resource Usage

```bash
# Slurm job efficiency
seff <JOB_ID>

# Real-time resource monitoring
sstat -j <JOB_ID> --format=JobID,MaxRSS,MaxVMSize,AveCPU
```

## Common Issues and Fixes

### Issue: Pipeline Fails with Exit Code 115

**Cause:** Data integrity violation (RULE-115) - schema mismatch detected.

**Solution:**
1. Check for new columns in source data:
   ```bash
   fairway generate-schema data/raw/new_file.csv
   ```
2. Update schema in `fairway.yaml` or switch to Delta Lake format:
   ```yaml
   storage:
     format: "delta"  # Enables schema evolution
   ```

### Issue: Out of Memory (OOM) Errors

**Cause:** Data too large for allocated resources.

**Solution:**
1. Increase memory allocation:
   ```bash
   fairway submit --with-spark --mem 64G
   ```
2. Enable salting for skewed data:
   ```yaml
   performance:
     salting: true
   ```
3. Reduce parallelism:
   ```yaml
   performance:
     target_file_size_mb: 256  # Larger files = fewer tasks
   ```

### Issue: Spark Cluster Won't Start

**Cause:** Network/connectivity issues, resource unavailability.

**Solution:**
1. Check Slurm node availability:
   ```bash
   sinfo -p day
   ```
2. Verify SPARK_LOCAL_IP:
   ```bash
   export SPARK_LOCAL_IP=$(hostname -i)
   ```
3. Check firewall/port access on compute nodes

### Issue: Files Not Being Processed

**Cause:** Files already in manifest as "completed".

**Solution:**
1. Check manifest status:
   ```bash
   fairway manifest query --status completed
   ```
2. Reset specific files for reprocessing:
   ```bash
   fairway manifest reset --file "data/raw/claims_CT_2023.csv"
   ```
3. Clear entire cache:
   ```bash
   fairway cache clear
   ```

### Issue: Java Not Found

**Cause:** JAVA_HOME not set or Java not installed.

**Solution:**
1. Load Java module (HPC):
   ```bash
   module load Java/11
   export JAVA_HOME=$JAVA_HOME
   ```
2. Install Java locally:
   ```bash
   # macOS
   brew install openjdk@11
   export JAVA_HOME=/opt/homebrew/opt/openjdk@11
   ```

### Issue: Container Mount Fails (Bind Path Not Found)

**Cause:** Apptainer trying to bind a path that doesn't exist on your cluster (e.g., `/vast` on a non-Yale cluster).

**Error:**
```
FATAL: container creation failed: mount hook function failure: mount /vast->/vast error: while mounting /vast: mount source /vast doesn't exist
```

**Solution:**
1. Set the correct bind path for your cluster:
   ```bash
   # Environment variable (temporary)
   export FAIRWAY_BINDS="/scratch/$USER,/gpfs/data"
   fairway submit --with-spark
   ```

2. Or add to `spark.yaml` (permanent):
   ```yaml
   apptainer_binds: "/scratch,/gpfs"
   ```

3. Common cluster paths:
   - Yale: `/vast`
   - Grace/Farnam: `/gpfs/ycga`
   - Generic: `/scratch`, `/project`, `/home`

### Issue: Container Build Fails

**Cause:** Missing dependencies or disk space.

**Solution:**
1. Eject and customize container definition:
   ```bash
   # Eject everything
   fairway eject

   # Or eject just container files
   fairway eject --container

   # Or eject to custom directory
   fairway eject --output custom/
   ```
2. Build with verbose output:
   ```bash
   apptainer build --debug fairway.sif Apptainer.def
   ```
3. Try Docker fallback:
   ```bash
   fairway build --docker
   ```

### Issue: Preprocessed Files Not All Ingested

**Cause:** When multiple archives contain files with the same basename, older versions of Fairway could lose track of some files in the manifest.

**Solution:**
1. Clear the table manifest to force reprocessing:
   ```bash
   rm manifest/<table_name>.json
   ```
2. Verify all extracted files exist in the preprocessing scratch dir:
   ```bash
   ls -R /path/to/scratch/fairway/<table_name>_v1/
   ```
3. Re-run the pipeline — Fairway now uses the preprocessing output directory as the manifest root, producing unique keys for files from different archives.

### Issue: Slow Pipeline Performance

**Cause:** Data skew, suboptimal file sizes, or insufficient resources.

**Solution:**
1. Enable salting for skewed partition keys:
   ```yaml
   performance:
     salting: true
     target_rows: 500000
   ```
2. Tune file sizes:
   ```yaml
   performance:
     target_file_size_mb: 128  # ~128MB files
   ```
3. Use scratch storage for intermediate writes:
   ```yaml
   storage:
     scratch_dir: "/scratch/$USER/fairway"
   ```

## Rollback Procedures

### Rollback Pipeline Run

1. **Identify Failed Batch**
   ```bash
   fairway logs --errors
   fairway manifest query --status failed
   ```

2. **Reset Failed Files**
   ```bash
   fairway manifest reset --batch <BATCH_ID>
   ```

3. **Restore Previous Output** (if needed)
   ```bash
   # If using Delta Lake, use time travel
   # Otherwise, restore from backup
   cp -r data/final.backup/* data/final/
   ```

4. **Re-run Pipeline**
   ```bash
   fairway run  # Will only process reset files
   ```

### Rollback Schema Changes

1. **Identify Schema Version**
   ```bash
   ls -la config/*_schema.yaml
   git log --oneline config/
   ```

2. **Restore Previous Schema**
   ```bash
   git checkout HEAD~1 -- config/my_schema.yaml
   ```

3. **Clear and Reprocess**
   ```bash
   fairway cache clear
   fairway run
   ```

### Cancel Running Jobs

```bash
# Cancel specific job
fairway cancel <JOB_ID>

# Cancel all your jobs (requires confirmation)
fairway cancel --all

# Force cancel without confirmation
scancel -u $USER
```

## Health Checks

### Pre-Run Checklist

- [ ] Config file exists and is valid: `fairway run --dry-run`
- [ ] Source data accessible: `ls data/raw/`
- [ ] Sufficient disk space: `df -h data/`
- [ ] Java available (for Spark): `java -version`
- [ ] Container built (if using): `ls *.sif`

### Post-Run Validation

- [ ] Check exit code: `echo $?` (0 = success)
- [ ] Review logs for errors: `fairway logs --errors`
- [ ] Verify output files: `ls data/final/`
- [ ] Check manifest status: `fairway manifest show`
- [ ] Validate row counts match expectations

## Exit Codes Reference

| Code | Meaning | Action |
|------|---------|--------|
| 0 | Success | None required |
| 1 | General error | Check logs for details |
| 2 | Configuration error | Validate YAML syntax and required fields |
| 115 | Data integrity error (RULE-115) | Schema mismatch - update schema or use Delta Lake |

## Contact and Escalation

For issues not covered in this runbook:
1. Check existing GitHub issues
2. Open a new issue at https://github.com/DISSC-yale/fairway/issues
3. Include: error message, logs, config (sanitized), and steps to reproduce
