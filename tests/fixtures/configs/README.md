# Fixture Configs

These YAML files document the config scenario for each test.
Tests use `tests/helpers.build_config()` to write real configs at runtime
with correct absolute paths injected.

| File | Feature tested |
|------|---------------|
| csv_simple.yaml | Basic CSV ingestion, schema inline |
| csv_missing_values.yaml | check_nulls validation |
| csv_zipped.yaml | preprocess: unzip |
| parquet_simple.yaml | Parquet format |
| fixed_width_simple.yaml | fixed_width format + spec file |
| fixed_width_zipped.yaml | fixed_width inside zip |
| json_simple.yaml | JSON records format |
| tsv_simple.yaml | TSV (tab-delimited) format |
| preprocess_unzip.yaml | preprocess action=unzip, scope=per_file |
| preprocess_script.yaml | preprocess action=script path |
| transform_simple.yaml | transformation script |
| validation_all_checks.yaml | All validation check types |
| salting.yaml | performance.salting=true |
| hive_partitioning.yaml | hive_partitioning=true |
| partition_aware.yaml | batch_strategy=partition_aware |
| schema_from_file.yaml | schema loaded from .yaml file |
| parquet_file_size.yaml | performance.max_records_per_file |
| output_layer_processed.yaml | output_layer=processed |
| write_mode_append.yaml | write_mode=append |
