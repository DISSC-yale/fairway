#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.config = "config/fairway.yaml"
params.spark_master = null

import org.yaml.snakeyaml.Yaml

def configPath = params.config
def configFile = new File(configPath)
def appConfig = new Yaml().load(configFile.text)

// Default to 'data/intermediate' and 'data/final' if not specified, but prefer config
def intermediateDir = appConfig.storage?.intermediate_dir ?: 'data/intermediate'
def finalDir = appConfig.storage?.final_dir ?: 'data/final'

process run_fairway {
    publishDir "${params.outdir}/intermediate", mode: 'copy', pattern: "${intermediateDir}/*", saveAs: { fn -> new File(fn).name }
    publishDir "${params.outdir}/final", mode: 'copy', pattern: "${finalDir}/*", saveAs: { fn -> new File(fn).name }
    
    input:
    path config_file

    output:
    path "${intermediateDir}/*"
    path "${finalDir}/*"

    script:
    def master_arg = params.spark_master ? "--spark-master ${params.spark_master}" : ""
    """
    # Link the data directory from the launch context to the work directory
    # so that relative paths in fairway.yaml (like 'data/raw/...') resolve correctly.
    if [ -d "${workflow.launchDir}/data" ]; then
        ln -s "${workflow.launchDir}/data" data
    fi

    # Using 'fairway run' as a worker process
    # It will handle Spark provisioning (if --with-spark is implicit or passed in config) 
    # and then execute the pipeline.
    fairway run --config ${config_file} ${master_arg}
    """
}

workflow {
    config_ch = Channel.fromPath(params.config)
    run_fairway(config_ch)
}
