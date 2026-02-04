/*
 * Fairway Nextflow Process Definitions
 *
 * Reusable process library for batch-aware data processing.
 * These processes call fairway CLI commands for each batch.
 *
 * All processes use tuple inputs: (table_name, batch_id, ...)
 * This enables multi-table parallel processing.
 *
 * NOTE: Spark cluster is started/stopped OUTSIDE Nextflow by driver.sh
 * Spark master URL is passed via --spark-master parameter
 */

nextflow.enable.dsl=2

// Resolve paths to absolute (processes run in isolated work dirs)
def config_path = file(params.config).toAbsolutePath().toString()
def work_path = file(params.work_dir).toAbsolutePath().toString()
// Schema output goes to project root (where Nextflow was launched)
def schema_path = file("schema").toAbsolutePath().toString()
// Final output directory (defaults to data/final relative to workflow launch dir)
def final_path = params.final_dir ? file(params.final_dir).toAbsolutePath().toString() : file("data/final").toAbsolutePath().toString()

/*
 * SCHEMA_SCAN - Scan schema for a single batch
 *
 * Input: tuple(table_name, batch_id)
 * Output: tuple(table_name, schema_file)
 */
process SCHEMA_SCAN {
    tag "${table_name}_batch_${batch_id}"

    input:
    tuple val(table_name), val(batch_id)

    output:
    tuple val(table_name), path("schema_${batch_id}.json")

    script:
    """
    fairway schema-scan --config ${config_path} --table ${table_name} --batch ${batch_id} --work-dir ${work_path}

    # Copy schema file to current directory for Nextflow to collect
    cp ${work_path}/${table_name}/batch_${batch_id}/schema_${batch_id}.json .
    """
}


/*
 * SCHEMA_MERGE - Merge all partial schemas into unified schema
 *
 * Input: tuple(table_name, [schema_files])
 * Output: tuple(table_name, unified_schema)
 */
process SCHEMA_MERGE {
    tag "${table_name}"

    input:
    tuple val(table_name), path(schemas)

    output:
    tuple val(table_name), path("unified_schema.json")

    script:
    """
    fairway schema-merge --config ${config_path} --table ${table_name} --work-dir ${work_path} --schema-dir ${schema_path}

    # Copy unified schema to current directory for Nextflow
    cp ${work_path}/${table_name}/unified_schema.json .
    """
}


/*
 * INGEST - Ingest a single batch directly to final output
 *
 * Input: tuple(table_name, batch_id, schema), spark_master
 * Output: tuple(table_name, done_marker) - actual data written to final_dir
 */
process INGEST {
    tag "${table_name}_batch_${batch_id}"

    input:
    tuple val(table_name), val(batch_id), path(schema)
    val spark_master

    output:
    tuple val(table_name), path("batch_${batch_id}.done")

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway ingest --config ${config_path} --table ${table_name} --batch ${batch_id} --final-dir ${final_path} ${master_arg}

    # Create marker file - actual parquet is in final_dir/{table}/partition_{batch}.parquet
    echo "${final_path}/${table_name}/partition_${batch_id}.parquet" > batch_${batch_id}.done
    """
}


/*
 * TRANSFORM_BATCH - Run parallel transform on single batch
 *
 * Input: tuple(table_name, batch_id, partition), transform_name, spark_master
 * Output: tuple(table_name, batch_id, transformed_partition)
 */
process TRANSFORM_BATCH {
    tag "${table_name}_${transform_name}_${batch_id}"

    input:
    tuple val(table_name), val(batch_id), path(partition)
    val transform_name
    val spark_master

    output:
    tuple val(table_name), val(batch_id), path("${transform_name}_${batch_id}.parquet")

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway transform-batch --config ${config_path} --table ${table_name} \
        --name ${transform_name} --batch ${batch_id} ${master_arg}

    touch ${transform_name}_${batch_id}.parquet
    """
}


/*
 * TRANSFORM_FANIN - Run fan-in transform across all batches
 *
 * Input: tuple(table_name, [partitions]), transform_name, spark_master
 * Output: tuple(table_name, transformed_output)
 */
process TRANSFORM_FANIN {
    tag "${table_name}_${transform_name}"

    input:
    tuple val(table_name), path(partitions)
    val transform_name
    val spark_master

    output:
    tuple val(table_name), path("${transform_name}_out/*.parquet")

    script:
    def master_arg = spark_master ? "--spark-master ${spark_master}" : ""
    """
    fairway transform-fanin --config ${config_path} --table ${table_name} \
        --name ${transform_name} ${master_arg}

    mkdir -p ${transform_name}_out
    touch ${transform_name}_out/part_0.parquet
    """
}


/*
 * FINALIZE - Validate and update manifest (data already in final_dir)
 *
 * Input: tuple(table_name, [done_markers])
 * Output: none (side effects only)
 */
process FINALIZE {
    tag "${table_name}"

    input:
    tuple val(table_name), path(done_markers)

    script:
    """
    # Data is already in final_dir - just log completion
    echo "Table ${table_name} complete. Output: ${final_path}/${table_name}/"
    echo "Partitions:"
    cat ${done_markers}
    """
}
