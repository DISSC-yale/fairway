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


workflow {
    // Spark master URL (passed by driver.sh, defaults to local for testing)
    def spark_master = params.spark_master ?: "local[*]"
    log.info "Spark master: ${spark_master}"

    // Determine which tables to process
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
        return
    }

    // Get batch info for each table
    log.info "Calculating batches..."
    def table_info = getTableInfo(tables)

    // Build channel of (table_name, batch_id) tuples for all tables
    // This enables parallel processing across ALL batches of ALL tables
    def all_batches = []
    table_info.each { info ->
        (0..<info.n_batches).each { batch_id ->
            all_batches << [info.name, batch_id]
        }
    }
    log.info "Total batch jobs: ${all_batches.size()}"

    // Create channels
    batch_ch = Channel.fromList(all_batches)

    // ========================================
    // Phase 1: Schema scan (fan-out across all tables/batches)
    // ========================================
    schemas = SCHEMA_SCAN(batch_ch)

    // Group schemas by table for merge phase
    schemas_by_table = schemas.groupTuple()

    // ========================================
    // Phase 2: Schema merge (fan-in per table)
    // ========================================
    unified_schemas = SCHEMA_MERGE(schemas_by_table)

    // ========================================
    // Phase 3: Ingest (fan-out across all tables/batches)
    // ========================================
    // Rebuild batch channel for ingest phase
    ingest_batch_ch = Channel.fromList(all_batches)

    // Combine batch info with unified schema
    // Create (table_name, batch_id, schema) tuples
    ingest_input = ingest_batch_ch
        .map { table_name, batch_id -> [table_name, batch_id] }
        .combine(unified_schemas, by: 0)  // Join on table_name

    partitions = INGEST(ingest_input, spark_master)

    // ========================================
    // Phase 4: Finalize (per table)
    // ========================================
    // Group partitions by table for finalization
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
