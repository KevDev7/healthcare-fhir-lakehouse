# Portfolio Brief

## Project Signal

This project demonstrates healthcare data engineering with realistic FHIR data:
ingestion, normalization, relationship validation, privacy-aware auditing,
analytics table design, automated testing, local orchestration, and a Databricks
cloud run.

It is designed to show more than generic ETL. The project works with linked
clinical entities such as patients, encounters, observations, conditions, labs,
vitals, medication orders/events, procedures, and FHIR references, which makes it
a stronger healthcare-domain signal than a flat CSV dashboard project.

## What Was Built

Local lakehouse pipeline:

* Source profiling for compressed FHIR NDJSON files
* Bronze Parquet table preserving raw FHIR resources and lineage metadata
* Silver Patient, Encounter, Observation, Condition, Medication, Medication
  Request, Medication Administration, Medication Dispense, Medication Statement,
  and Procedure tables
* FHIR relationship audit for patient, encounter, medication, and medication
  request references
* HIPAA Safe Harbor-inspired privacy validation audit
* Gold analytics tables for encounters, diagnoses, daily vitals/labs, medication
  activity, medication order fulfillment, and procedure summaries
* Consolidated data quality report
* One-command local pipeline runner with a run manifest

Cloud lakehouse pipeline:

* Databricks/Spark implementation of the expanded lakehouse flow
* Unity Catalog managed volume for raw FHIR files
* Delta tables for Bronze, Silver, Gold, and audit layers
* Databricks serverless job execution
* Cloud data quality checks that fail the job on hard failures

## Proof Points

Local:

* 928,935 total FHIR resources profiled and ingested
* 100 patients
* 637 encounters
* 813,540 observations
* 5,051 conditions
* 93,667 medication-related source resources
* 3,450 procedures
* 115 automated tests passing

Databricks:

* Successful serverless job run: `377334542675458`
* Raw source files uploaded to Unity Catalog volume: 30
* Cloud Bronze row count: 928,935
* Cloud Silver row counts match local expanded clinical counts
* Cloud Gold outputs populated, including medication and procedure analytics
* Cloud data quality checks: 19 passing, 0 failing

See `documentation/cloud_run_evidence.md` for full Databricks evidence.

## Why It Matters For Healthcare Data Engineering

The project demonstrates practical healthcare patterns:

* FHIR JSON parsing and reference handling
* Raw-to-curated lakehouse layering
* Clinical event normalization across encounters, patients, medications, and
  procedures
* Data quality checks grounded in expected clinical resource counts
* Privacy-oriented review of identifiers, dates, lineage fields, and text-like
  outputs
* Databricks/Spark/Delta execution, which maps to common healthcare analytics
  and lakehouse environments

## Honest Scope

This is a demo-scale project using the public MIMIC-IV Clinical Database Demo on
FHIR dataset. It is suitable for portfolio and interview discussion, but it is
not a production HIPAA compliance system and does not process the full
credentialed MIMIC-IV dataset.

The first Databricks implementation uses Unity Catalog managed volumes rather
than external S3/IAM/Terraform. That is a deliberate scope choice: it proves the
lakehouse and Databricks execution model while keeping cloud access manageable.

## Extension Opportunities

Future extensions that would build on the finished project:

* Add Specimen, Location, and Organization Silver tables.
* Split the Databricks job into multi-task Workflows with separate quality gates.
* Add S3 external locations and Terraform if infrastructure signal becomes more
  important than healthcare modeling depth.
* Add Databricks SQL dashboards over Gold clinical analytics tables.
* Add Great Expectations, Soda, or Databricks expectations for richer data
  quality presentation.
