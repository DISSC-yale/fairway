#!/usr/bin/env nextflow

/*
 * Fairway Main Workflow - Fan-Out/Fan-In Orchestration
 *
 * This workflow orchestrates parallel batch processing for large datasets.
 * It uses fairway CLI commands as workers, coordinated by Nextflow.
 *
 * Usage:
 *   nextflow run main.nf --table TABLE_NAME [--n_batches N] [--spark_master URL]
 */

nextflow.enable.dsl=2

// Robust include path: uses FAIRWAY_NF_LIB env var or falls back to workflow.projectDir
def nf_lib = System.getenv('FAIRWAY_NF_LIB') ?: "${workflow.projectDir}"
include { SCHEMA_SCAN; SCHEMA_MERGE; INGEST; TRANSFORM_BATCH; TRANSFORM_FANIN; FINALIZE } from "${nf_lib}/fairway_processes.nf"

// Parameters - config is single source of truth, these are runtime overrides
params.table = null
params.config = 'config/fairway.yaml'
params.n_batches = null       // If null, calculated dynamically via fairway batches
params.spark_master = null    // Passed by driver.sh after starting cluster
params.work_dir = '.fairway/work'

// Validate required parameters
if (!params.table) {
    error "ERROR: --table parameter is required. Example: nextflow run main.nf --table my_table"
}


workflow {
    // Calculate batch count dynamically if not provided
    def n_batches
    if (params.n_batches != null) {
        n_batches = params.n_batches
    } else {
        // Execute fairway batches command to get count
        def cmd = "fairway batches --config ${params.config} --table ${params.table}"
        def proc = cmd.execute()
        proc.waitFor()
        n_batches = proc.text.trim().toInteger()
    }

    log.info "Processing table: ${params.table}"
    log.info "Number of batches: ${n_batches}"

    // Spark master URL (passed by driver.sh, defaults to local for testing)
    def spark_master = params.spark_master ?: "local[*]"
    log.info "Spark master: ${spark_master}"

    // Create batch channel (0 to n_batches-1)
    batches = Channel.of(0..<n_batches)

    // ========================================
    // Phase 1: Schema scan (fan-out)
    // ========================================
    // Each batch scans its files independently
    schemas = SCHEMA_SCAN(batches)

    // ========================================
    // Phase 2: Schema merge (fan-in)
    // ========================================
    // Wait for all partial schemas, then merge
    unified = SCHEMA_MERGE(schemas.collect())

    // ========================================
    // Phase 3: Ingest (fan-out)
    // ========================================
    // Each batch ingests independently using unified schema
    // Reset batches channel for ingest phase
    batches_for_ingest = Channel.of(0..<n_batches)
    partitions = INGEST(batches_for_ingest, unified, spark_master)

    // ========================================
    // Phase 4: Transforms (customize this section)
    // ========================================
    // Example: Map partitions for transform processing
    // result = partitions.map { p -> tuple(p.baseName.split('_')[-1].toInteger(), p) }

    // Example: Parallel transform (uncomment to use)
    // result = TRANSFORM_BATCH(result, 'standardize', spark_master)

    // Example: Fan-in transform (uncomment to use)
    // collected = result.map { it[1] }.collect()
    // result = TRANSFORM_FANIN(collected, 'deduplicate', spark_master)

    // For now, just use partitions directly
    result = partitions

    // ========================================
    // Phase 5: Finalize
    // ========================================
    FINALIZE(result.collect())
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
