#!/usr/bin/env nextflow

/*
 * Fairway Main Workflow - Fan-Out/Fan-In Orchestration
 *
 * This workflow orchestrates parallel batch processing for large datasets.
 * It uses fairway CLI commands as workers, coordinated by Nextflow.
 *
 * Usage:
 *   nextflow run main.nf [--table TABLE_NAME] [--n_batches N] [--spark_master URL]
 *
 * Modes:
 *   - With --table: Process a single table
 *   - Without --table: Process all tables from config
 */

nextflow.enable.dsl=2

// Robust include path: uses FAIRWAY_NF_LIB env var or falls back to workflow.projectDir
def nf_lib = System.getenv('FAIRWAY_NF_LIB') ?: "${workflow.projectDir}"
include { SCHEMA_SCAN; SCHEMA_MERGE; INGEST; FINALIZE } from "${nf_lib}/fairway_processes.nf"

// Parameters - config is single source of truth, these are runtime overrides
params.table = null           // If null, process all tables from config
params.config = 'config/fairway.yaml'
params.n_batches = null       // If null, calculated dynamically via fairway batches
params.spark_master = null    // Passed by driver.sh after starting cluster
params.work_dir = '.fairway/work'


/*
 * Helper: Get fairway command (uses FAIRWAY_VENV if set)
 */
def getFairwayCmd() {
    def venv = System.getenv('FAIRWAY_VENV')
    if (venv) {
        return "${venv}/bin/fairway"
    }
    return "fairway"
}


/*
 * Helper: Get list of table names from config
 */
def getTablesFromConfig() {
    def fairway = getFairwayCmd()
    def cmd = "${fairway} list-tables --config ${params.config}"
    def proc = cmd.execute()
    proc.waitFor()
    if (proc.exitValue() != 0) {
        log.warn "Failed to get tables: ${proc.err.text}"
        return []
    }
    def tables = proc.text.trim().split('\n').findAll { it }
    return tables
}


/*
 * Helper: Get batch count for a table
 */
def getBatchCount(table_name) {
    def fairway = getFairwayCmd()
    def cmd = "${fairway} batches --config ${params.config} --table ${table_name}"
    def proc = cmd.execute()
    proc.waitFor()
    if (proc.exitValue() != 0) {
        log.error "Failed to get batch count for ${table_name}: ${proc.err.text}"
        return 1
    }
    def count = proc.text.trim()
    return count ? count.toInteger() : 1
}


/*
 * Helper: Build table info list with batch counts
 * Returns list of maps: [{name: 'table1', n_batches: 5}, ...]
 * Uses params.n_batches if provided (single table mode only)
 */
def getTableInfo(tables) {
    return tables.collect { table_name ->
        def n_batches
        if (params.n_batches != null && tables.size() == 1) {
            // Use provided batch count for single table mode
            n_batches = params.n_batches
        } else {
            n_batches = getBatchCount(table_name)
        }
        log.info "  ${table_name}: ${n_batches} batches"
        [name: table_name, n_batches: n_batches]
    }
}


/*
 * Helper: Get tables and batches info (shared by workflows)
 */
def getWorkflowSetup() {
    def tables
    if (params.table) {
        tables = [params.table]
        log.info "Mode: Single table (${params.table})"
    } else {
        tables = getTablesFromConfig()
        log.info "Mode: All tables (${tables.size()} tables)"
        log.info "Tables: ${tables.join(', ')}"
    }

    if (tables.size() == 0) {
        log.error "No tables to process"
        return [tables: [], batches: []]
    }

    log.info "Calculating batches..."
    def table_info = getTableInfo(tables)

    def all_batches = []
    table_info.each { info ->
        (0..<info.n_batches).each { batch_id ->
            all_batches << [info.name, batch_id]
        }
    }
    log.info "Total batch jobs: ${all_batches.size()}"

    return [tables: tables, batches: all_batches, table_info: table_info]
}


/*
 * SCHEMA workflow - Phase 1: Schema discovery (no Spark required)
 *
 * Usage: nextflow run main.nf -entry schema
 */
workflow schema {
    log.info "=== SCHEMA PHASE (no Spark required) ==="

    def setup = getWorkflowSetup()
    if (setup.batches.size() == 0) return

    batch_ch = Channel.fromList(setup.batches)

    // Schema scan (fan-out across all tables/batches)
    schemas = SCHEMA_SCAN(batch_ch)

    // Group schemas by table for merge phase
    schemas_by_table = schemas.groupTuple()

    // Schema merge (fan-in per table)
    SCHEMA_MERGE(schemas_by_table)
}


/*
 * INGEST workflow - Phase 2: Data ingestion (requires Spark)
 *
 * Usage: nextflow run main.nf -entry ingest --spark-master <URL>
 */
workflow ingest {
    def spark_master = params.spark_master ?: "local[*]"
    log.info "=== INGEST PHASE (Spark: ${spark_master}) ==="

    def setup = getWorkflowSetup()
    if (setup.batches.size() == 0) return

    // Load unified schemas from previous phase
    // Expect schema files at: {work_dir}/{table}/unified_schema.json
    def schema_files = []
    setup.tables.each { table_name ->
        def schema_path = "${params.work_dir}/${table_name}/unified_schema.json"
        if (file(schema_path).exists()) {
            schema_files << [table_name, file(schema_path)]
        } else {
            log.error "Schema not found for ${table_name}: ${schema_path}"
            log.error "Run 'make schema' first to generate schemas"
        }
    }

    if (schema_files.size() == 0) {
        log.error "No schemas found. Run schema phase first."
        return
    }

    unified_schemas = Channel.fromList(schema_files)
    ingest_batch_ch = Channel.fromList(setup.batches)

    // Combine batch info with unified schema
    ingest_input = ingest_batch_ch
        .map { table_name, batch_id -> [table_name, batch_id] }
        .combine(unified_schemas, by: 0)

    partitions = INGEST(ingest_input, spark_master)

    // Finalize (per table)
    partitions_by_table = partitions.groupTuple()
    FINALIZE(partitions_by_table)
}


/*
 * Default workflow - Full pipeline (schema + ingest)
 *
 * Usage: nextflow run main.nf --spark-master <URL>
 */
workflow {
    def spark_master = params.spark_master ?: "local[*]"
    log.info "=== FULL PIPELINE (Spark: ${spark_master}) ==="

    def setup = getWorkflowSetup()
    if (setup.batches.size() == 0) return

    batch_ch = Channel.fromList(setup.batches)

    // Phase 1: Schema scan (fan-out across all tables/batches)
    schemas = SCHEMA_SCAN(batch_ch)
    schemas_by_table = schemas.groupTuple()

    // Phase 2: Schema merge (fan-in per table)
    unified_schemas = SCHEMA_MERGE(schemas_by_table)

    // Phase 3: Ingest (fan-out across all tables/batches)
    ingest_batch_ch = Channel.fromList(setup.batches)
    ingest_input = ingest_batch_ch
        .map { table_name, batch_id -> [table_name, batch_id] }
        .combine(unified_schemas, by: 0)

    partitions = INGEST(ingest_input, spark_master)

    // Phase 4: Finalize (per table)
    partitions_by_table = partitions.groupTuple()
    FINALIZE(partitions_by_table)
}


/*
 * Workflow completion handler
 */
workflow.onComplete {
    log.info "Pipeline completed at: ${workflow.complete}"
    log.info "Duration: ${workflow.duration}"
    log.info "Success: ${workflow.success}"

    if (!workflow.success) {
        log.error "Pipeline failed. Check .nextflow.log for details."
    }
}
