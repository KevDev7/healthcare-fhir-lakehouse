# Tech Stack

## Overview

This project can be implemented with a production-style lakehouse stack or a
lighter local-first stack. The source data is small enough for local development
but realistic enough to model the same layers used in Spark/Delta pipelines.

---

## Recommended Development Path

Start locally, then port to Databricks/Spark once the transformation logic is
clear.

Local-first development is useful because the demo dataset is only about 50 MB
compressed and contains 100 patients. That is large enough to exercise nested
FHIR parsing, joins, and aggregation, but small enough to iterate quickly without
cloud infrastructure.

---

## Local Development Stack

### Python

Primary language for ingestion, parsing, validation, and tests.

Useful libraries:

* `gzip` and `json` for reading compressed NDJSON
* `pandas` for quick profiling and small-table transformations
* `duckdb` for local SQL analytics over Parquet
* `pyarrow` for Parquet IO
* `pytest` for transformation and privacy validation tests

### DuckDB

Useful for local analytics and fast iteration.

Good fit for:

* Inspecting flattened Silver tables
* Querying Parquet outputs
* Prototyping Gold aggregations
* Running lightweight SQL tests

### PySpark Local Mode

Optional if the goal is to keep transformation code close to a Databricks
implementation from the beginning.

Good fit for:

* Spark DataFrame transformation logic
* Schema handling for nested JSON
* Easier migration to Databricks

---

## Production-Style Stack

### Databricks / Apache Spark

Target compute engine for a production-like lakehouse implementation.

Good fit for:

* Distributed processing
* Incremental ingestion
* Bronze/Silver/Gold jobs
* SQL analytics and notebooks

For this demo dataset, Spark is not required for scale. It is useful because it
matches common healthcare data platform tooling.

### Delta Lake

Recommended table format for Bronze, Silver, and Gold layers.

Good fit for:

* ACID table writes
* Schema evolution
* Time travel
* Data lineage between transformation runs

### Amazon S3

Recommended cloud object storage for a production-style setup.

Good fit for:

* Raw FHIR landing zone
* Delta/Parquet storage
* Separation of storage and compute

For local development, the checked-in dataset folder can stand in for the S3 raw
landing zone.

### Unity Catalog Managed Volumes

Implemented storage target for the first Databricks run.

Good fit for:

* Small portfolio demo datasets
* Avoiding AWS IAM and external-location setup for the first cloud proof
* Keeping runtime data in Databricks while source code stays GitHub-friendly

The current cloud implementation uses a managed volume under the `workspace`
catalog and project-prefixed schemas for Bronze, Silver, Gold, and audit Delta
tables. S3 remains the likely next step for a more production-like deployment.

---

## Data Formats

### Input

* `.ndjson.gz`
* One FHIR JSON resource per line
* Resource families split across files such as `MimicPatient.ndjson.gz`,
  `MimicEncounter.ndjson.gz`, and `MimicObservationLabevents.ndjson.gz`

### Intermediate and Output

Recommended:

* Delta tables in Databricks
* Parquet files locally

Optional:

* CSV only for small exported reports or examples, not primary tables

---

## Layer-to-Technology Mapping

### Bronze

Local:

* Python reads `.ndjson.gz`
* Writes raw resources to Parquet

Databricks:

* Spark reads compressed NDJSON
* Writes Delta Bronze tables
* Implemented in `src/healthcare_fhir_lakehouse_spark/cloud_pipeline.py`

### Silver

Local:

* Python, pandas, DuckDB, or PySpark flatten FHIR resources
* Writes normalized Parquet tables

Databricks:

* Spark DataFrames or SQL parse nested fields
* Writes resource-specific Delta tables
* Implemented for Patient, Encounter, Observation, and Condition

### Privacy Validation

Local:

* Python validation functions
* pytest tests for expected pass/fail cases
* Audit report as Parquet, JSON, or Markdown

Databricks:

* Spark validation jobs
* Delta audit table
* SQL dashboards or notebook summaries
* Implemented as Delta audit tables for privacy inventory and data quality

### Gold

Local:

* DuckDB SQL or Python builds aggregated Parquet tables

Databricks:

* Spark SQL / Delta Live Tables / notebooks build curated analytics tables
* Implemented as Spark SQL/DataFrame transformations in the cloud pipeline

---

## Privacy Tooling

The first implementation should use explicit, testable validation rules rather
than heavy NLP.

Recommended first checks:

* Identifier-column detection
* Pattern checks for email, phone, SSN-like values, IP addresses, and URLs
* Date precision checks for Gold outputs
* Age-over-89 grouping checks
* Allowlist/denylist checks for columns permitted in analytics tables

Future optional tools:

* Microsoft Presidio for named-entity PHI detection
* Great Expectations or Soda for data quality rules
* Databricks expectations or Delta Live Tables constraints

---

## Orchestration

The local project uses a `make pipeline` command and Typer CLI runner. The cloud
project now uses a Databricks serverless job.

Good later options:

* Databricks Workflows
* Apache Airflow
* Dagster

For this portfolio project, the current progression is:

* local `make pipeline` for fast iteration
* `databricks.yml` for source-controlled Databricks job intent
* Databricks Jobs/Workflows for cloud execution evidence

---

## Testing

Recommended tests:

* Bronze row counts match source NDJSON line counts
* Silver tables extract expected patient, encounter, code, value, unit, and time
  fields
* FHIR references parse into stable join keys
* Gold tables do not expose raw patient identifiers
* Privacy audit catches intentionally unsafe sample outputs
* Aggregations produce deterministic counts on the demo dataset

---

## Summary

The original stack choice of Databricks, Spark, S3, and Delta Lake is reasonable
for the intended lakehouse architecture. For this specific 100-patient demo
dataset, a local Python/DuckDB/Parquet implementation is the fastest way to prove
the design, and the Databricks/Spark/Delta port demonstrates that the same
concepts can run on a healthcare-relevant cloud lakehouse platform.
