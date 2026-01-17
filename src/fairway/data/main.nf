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
    # Dynamic Data Discovery
    # Traverse up directory tree to find 'data' directory (Project Root)
    search_dir="${workflow.launchDir}"
    found_data=""
    
    # Check up to 5 levels up
    for i in {1..5}; do
        if [ -d "\$search_dir/data/raw" ]; then
            found_data="\$search_dir/data"
            break
        fi
        # Move up one level
        search_dir="\$(dirname "\$search_dir")"
    done
    
    if [ -n "\$found_data" ]; then
        echo "Found data directory at: \$found_data"
        ln -s "\$found_data" data
    else
        echo "WARNING: Could not find 'data' directory in hierarchy of launchDir."
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
