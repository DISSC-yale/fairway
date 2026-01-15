#!/usr/bin/env nextflow

import org.yaml.snakeyaml.Yaml

def configPath = params.config ?: 'config/example_config.yaml'
def configFile = new File(configPath)
def config = new Yaml().load(configFile.text)

def finalDir = config.storage.final_dir
println "Final Dir: ${finalDir}"
