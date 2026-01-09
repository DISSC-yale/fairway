Centralized Data Ingestion Framework
Goal is to create a data ingestion framework for centralized data at Yale that makes the long term management sustainable. We want to streamline the production of data products.
What is a data product in a research setting?
•	Trustworthy: High quality, with clear lineage and governance.
•	Reusable: Designed for multiple use cases and consumers.
•	Governed: Meets security, privacy, and compliance standards.
•	Accessible: Delivered via defined interfaces (APIs, dashboards, etc.).
•	Value-driven: Works for a community of data researchers. 
We want to build an ingestion framework which makes each of these easier to implement.
Functional Requirements
•	Metadata (files + ) 
o	Files level, standardized manifest generation (fmanifest) to track raw data file and feed redivis.
o	tie into governance (nice to have)
o	tables schema level metadata?
•	Documentation:
o	Templates for doc sites and README
o	summary tables.
•	ETL framework:
o	Standardization (orchestration, query engine/library, file format, containerization, transformations )
o	storage format
o	efficient storage - can we save diffs
•	Data Validation: 
•	Automation:
o	Pipeline should be easy to run and run end to end via an orchestration engine
o	Event driven where it make sense
•	Versioning:
o	Not sure we need this as much as efficient storage. Maybe for transformation files
Common Pain points
•	Numerator
o	changing schemas
o	duplicated data
•	L2
o	new data on storage@yale → hard to get to ycrc
•	verisk
•	IPUMS
•	
Outstanding quesitons:
1.	Ingestion package
o	TODO: Spark vs Arrow vs DuckDB/Polars right and compare test on L2 data also test query of iceburg 
	Lets choose 2, pyspark primary and other for smaller files
2.	Q: what does Redevis require?
1.	Need to put specs in the api
3.	Q: Is Iceburg/Deltalake worth the added complexity?
o	Solves the version issue
o	what about an efficient panel?
o	DECISION: Stick with parquet, researchers favor simplicity over speed.
o	TODO: How does one migrate from parquet to iceburg
4.	Q: Does a framework exist for saving files diffs?
5.	Q: Does apptainer lend itself to running in the cloud?
1.	what are the downsides of containerization
2.	DECISION: Lets use apptainer with nextflow
6.	intake process
1.	Process for estimating size needed
1.	Raw data
2.	intermediate
3.	final
2.	harmonization?
3.	schema drift?
4.	
Data Pipeline Architecture Plan (Ignore a sketch for now)
1. High-Level Overview
This pipeline ingests data from external vendors via SFTP, processes it within a portable containerized environment (supporting YCRC/NERC), and publishes two distinct data products to Redivis:
1.	Raw Data Product: Cleaned, validated, and geo-enriched data in its original schema.
2.	Transformed Data Product: Reshaped and aggregated data ready for analysis.
3.	Should be config driven
2. Infrastructure & Tech Stack
•	Orchestration: Nextflow / Slurm bash script
•	Compute: Docker/Apptainer Containers (deployed on YCRC High Performance Computing), explore NERC OpenShift.
•	Storage:
o	Landing: Where does data from Vender saved? Different for different resource (L2 vs Verisk/Numerator) 
o	Intermediate: YCRC Bouchet: Parquet files (partitioned).
o	Production: Redivis (Dataset hosting).
•	Versioning: Git Commit Hash $\approx$ Redivis Dataset Version.
 
3. Workflow Specification
Phase I: Ingestion & Triggering
•	Source: Vendor .
•	Trigger: Cron job initiates the container or we do
•	Step 1: File Discovery
o	Scan the landing zone.
o	Manifest Check: Load existing fmanifest. Compare incoming file fname, hash, and date.
o	Logic:
	If Hash matches existing -> Skip.
	If Hash differs or New File -> Proceed to Phase II.
Phase II: The "Raw" Processing Container
•	Goal: Convert vendor delivery into a queryable, verifiable format without altering the schema.
1.	Pre-process & Validation (validate.py - Level 1)
o	Load Data Dictionary.
o	Basic Sanity Checks: Verify column counts (ncol), expected observation counts, and file integrity errors.
o	Gate: If fail -> Trigger Alert / Log Error.
2.	Ingestion (ingest.py)
o	Conversion: Convert source format (CSV/Excel) to Parquet.
o	Partitioning: Optimize file layout (e.g., by date or region).
3.	Post-Processing & Enrichment
o	Geocoding: Resolve address fields to coordinates.
o	Indexing: Assign H3 Indices (Hexagonal hierarchical spatial index) for efficient spatial joining.
o	Geo-Digest: Generate spatial distribution summaries.
4.	Content Validation (validate.py - Level 2)
o	Distribution Check: Flag "weird distributions" (e.g., null spikes, value outliers).
o	Schema Check: Ensure strict adherence to the Data Dictionary.
5.	Documentation Generator (summarize.py)
o	Auto-generate summary statistics tables.
o	Update the README for the project.
6.	Production Push (Raw)
o	Action: Upload validated Parquet files to the Redivis Raw Dataset.
o	Version Sync: Tag the Redivis version with the current Git Commit Hash.
Phase III: The "Transform" Stage
•	Goal: Create an analysis-ready dataset (reshaped/aggregated).
•	Input: The validated raw.parquet from Phase II.
1.	Transformation
o	Execute reshaping logic (long-to-wide, aggregations, etc.).
2.	Validation (validate.py - Level 3)
o	Verify integrity of the transformed logic (e.g., "Did we lose rows during the reshape?").
3.	Manifest Generation
o	Create a lineage manifest linking the Transformed file back to the specific Raw file hash used.
4.	Production Push (Transformed)
o	Action: Upload to the Redivis Transformed Dataset.
o	Gate: Manual or Automated approval (depending on configuration).

# Spark Cluster Sizing on Slurm

## 1. Node Allocation (Physical Layer)
Controlled via `fairway run` CLI arguments (`--slurm-nodes`, `--slurm-cpus`, etc.).
- Default: 1 node, 4 CPUs (CLI) / 2 nodes, 32 CPUs (Internal Fallback).
- Logic: `src/engines/slurm_cluster.py` generates an sbatch script.

## 2. Spark Dynamic Allocation (Logical Layer)
Spark manages executors within allocated nodes using **Dynamic Allocation**.
- Hardcoded in `src/engines/slurm_cluster.py`:
  - `spark.dynamicAllocation.minExecutors 5`
  - `spark.dynamicAllocation.maxExecutors 150`
  - `spark.dynamicAllocation.initialExecutors 15`
- Implication: Executors scale between 5 and 150 based on load, bounded by physical resources.

 

