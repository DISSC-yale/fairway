nextflow.enable.dsl=2

params.config = "config/example_config.yaml"
params.outdir = "data"
params.container = "ingestion_framework:latest"
params.spark_master = ""
params.batch_size = 30
params.slurm_nodes = 1
params.slurm_cpus_per_task = 4
params.slurm_mem = "16G"
params.slurm_time = "24:00:00"
params.slurm_partition = "day"
params.account = "borzekowski"

process RUN_INGESTION {
    maxForks params.batch_size
    tag "Ingesting ${params.config}"
    publishDir "${params.outdir}", mode: 'copy'

    container params.container

    input:
    path config_file

    output:
    path "intermediate/*"
    path "final/*"
    path "fmanifest.json"

    script:
    """
    export PYTHONPATH=\$PYTHONPATH:\$(pwd)/src
    python3 -m fairway.pipeline ${config_file} --spark_master "${params.spark_master}"
    """
}

workflow {
    config_ch = Channel.fromPath(params.config)
    RUN_INGESTION(config_ch)
}
