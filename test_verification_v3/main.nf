#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.config = "config/fairway.yaml"
params.batch_size = 30

process run_fairway {
    publishDir "${params.outdir}", mode: 'copy'
    
    input:
    path config_file

    output:
    path "data/final/*"

    script:
    """
    fairway run --config ${config_file}
    """
}

workflow {
    config_ch = Channel.fromPath(params.config)
    run_fairway(config_ch)
}
