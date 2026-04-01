.PHONY: test test-docker test-apptainer test-hpc fixtures build-docker build-apptainer

## Run full test suite locally (requires Java + PySpark)
test:
	pytest tests/ -m "not hpc"

## Run full test suite inside Docker container
test-docker:
	docker run --rm -v $(PWD):/app fairway-dev pytest tests/ -m "not hpc"

## Run full test suite inside Apptainer container
## Note: use 'apptainer run' (not exec) so %runscript fires
test-apptainer:
	apptainer run --bind $(PWD):/workspace/fairway fairway-dev.sif pytest tests/ -m "not hpc"

## Run HPC-only tests (requires Slurm cluster)
test-hpc:
	pytest tests/ -m hpc

## Regenerate binary fixtures (parquet files, zip archives)
fixtures:
	python tests/fixtures/generate.py

## Build Docker dev image (run once, then use test-docker)
build-docker:
	docker build -f Dockerfile.dev -t fairway-dev .

## Build Apptainer dev image (run once, then use test-apptainer)
build-apptainer:
	apptainer build fairway-dev.sif fairway-dev.def
