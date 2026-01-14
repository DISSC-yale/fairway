nextflow.enable.dsl=2

params.config = "config/example_config.yaml"
params.outdir = "data"
params.container = "ingestion_framework:latest"
params.spark_master = null
params.batch_size = 30
params.slurm_nodes = 1
params.slurm_cpus_per_task = 4
params.slurm_mem = "16G"
params.slurm_time = "24:00:00"
params.slurm_partition = "day"


process RUN_INGESTION {
    maxForks params.batch_size
    tag "Ingesting ${params.config}"
    publishDir "${params.outdir}", mode: 'copy'

    container params.container

    input:
    path config_file
    path src

    output:
    path "data/intermediate/*", optional: true
    path "data/final/*", optional: true
    path "data/fmanifest.json", optional: true

    script:
    // Only pass spark_master if set; explicitly ignored by DuckDB engine in pipeline.py
    def spark_arg = params.spark_master ? "--spark_master \"${params.spark_master}\"" : ""
    """
    export PYTHONPATH=\$(pwd)/src:\$PYTHONPATH
    python3 -m fairway.pipeline ${config_file} ${spark_arg}
    """
}

workflow {
    config_ch = Channel.fromPath(params.config)
    src_ch = Channel.fromPath("src")
    RUN_INGESTION(config_ch, src_ch)
}
