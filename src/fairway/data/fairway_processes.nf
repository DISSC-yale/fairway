/*
 * Fairway Nextflow Process Definitions
 *
 * Reusable process library for batch-aware data processing.
 * These processes call fairway CLI commands for each batch.
 *
 * NOTE: Spark cluster is started/stopped OUTSIDE Nextflow by driver.sh
 * Spark master URL is passed via --spark-master parameter
 */

nextflow.enable.dsl=2


/*
 * SCHEMA_SCAN - Scan schema for a single batch
 *
 * Fan-out process: runs in parallel for each batch
 */
process SCHEMA_SCAN {
    tag "batch_${batch_id}"

    input:
    val batch_id

    output:
    path "schema_${batch_id}.json"

    script:
    """
    fairway schema-scan --config ${params.config} --table ${params.table} --batch ${batch_id}

    # Copy schema file to current directory for Nextflow to collect
    cp ${params.work_dir}/${params.table}/batch_${batch_id}/schema_${batch_id}.json .
    """
}


/*
 * SCHEMA_MERGE - Merge all partial schemas into unified schema
 *
 * Fan-in process: waits for all SCHEMA_SCAN to complete
 */
process SCHEMA_MERGE {
    input:
    path schemas

    output:
    path "unified_schema.json"

    script:
    """
    fairway schema-merge --config ${params.config} --table ${params.table}

    # Copy unified schema to current directory
    cp ${params.work_dir}/${params.table}/unified_schema.json .
    """
}


/*
 * INGEST - Ingest a single batch
 *
 * Fan-out process: runs in parallel for each batch
 * Requires unified schema from SCHEMA_MERGE
 */
process INGEST {
    tag "batch_${batch_id}"

    input:
    val batch_id
    path schema
    val spark_master

    output:
    path "partition_${batch_id}.parquet", optional: true

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway ingest --config ${params.config} --table ${params.table} --batch ${batch_id} ${master_arg}

    # Touch output file to signal completion
    touch partition_${batch_id}.parquet
    """
}


/*
 * TRANSFORM_BATCH - Run parallel transform on single batch
 *
 * Fan-out process: processes each batch independently
 */
process TRANSFORM_BATCH {
    tag "${transform_name}_${batch_id}"

    input:
    tuple val(batch_id), path(partition)
    val transform_name
    val spark_master

    output:
    tuple val(batch_id), path("${transform_name}_${batch_id}.parquet")

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway transform-batch --config ${params.config} --table ${params.table} \
        --name ${transform_name} --batch ${batch_id} ${master_arg}

    touch ${transform_name}_${batch_id}.parquet
    """
}


/*
 * TRANSFORM_FANIN - Run fan-in transform across all batches
 *
 * Fan-in process: waits for all batches, processes together
 */
process TRANSFORM_FANIN {
    input:
    path partitions
    val transform_name
    val spark_master

    output:
    path "${transform_name}_out/*.parquet"

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway transform-fanin --config ${params.config} --table ${params.table} \
        --name ${transform_name} ${master_arg}

    mkdir -p ${transform_name}_out
    touch ${transform_name}_out/part_0.parquet
    """
}


/*
 * FINALIZE - Validate, export, and cleanup
 *
 * Final process: runs after all transforms complete
 */
process FINALIZE {
    input:
    path partitions

    script:
    """
    fairway finalize --config ${params.config} --table ${params.table}
    """
}
