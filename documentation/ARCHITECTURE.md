# Architecture

## Overview

This project is designed as a demo-scale healthcare lakehouse for FHIR-formatted
clinical data. It uses the MIMIC-IV Clinical Database Demo on FHIR as the source
and transforms compressed NDJSON FHIR resources into curated analytics tables.

The architecture is viable for this dataset because the source includes linked
FHIR resources for patients, encounters, observations, conditions, procedures,
medications, specimens, locations, and organization metadata.

---

## Current Implementation Status

The repository currently contains documentation, the downloaded demo dataset, a
local Python project foundation, source profiling utilities, a local Bronze
Parquet ingestion layer, core Silver clinical tables, FHIR relationship auditing,
privacy validation, the first Gold analytics tables, and consolidated data
quality reporting. A local pipeline runner now executes the full flow and writes
a run manifest.

Milestone 10 also ports the core pipeline to Databricks/Spark/Delta. The
Databricks version has run successfully on serverless compute and writes Bronze,
Silver, Gold, and audit Delta tables in Unity Catalog.

The architecture below is the intended implementation target.

---

## High-Level Flow

```text
                  FHIR NDJSON files
          MIMIC-IV demo on FHIR, 100 patients
                          |
                          v
                    Bronze layer
              Raw resource preservation
                          |
                          v
                    Silver layer
          FHIR parsing and normalization
                          |
                          v
                  Privacy validation
       Identifier, date, and output-safety checks
                          |
                          v
                     Gold layer
              Analytics-ready datasets
```

Cloud implementation:

```text
Unity Catalog managed volume
  -> Spark Bronze Delta table
  -> Spark Silver Delta tables
  -> Spark audit Delta tables
  -> Spark Gold Delta tables
  -> Databricks job run evidence
```

---

## Source Data

Source: [MIMIC-IV Clinical Database Demo on FHIR v2.1.0](https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/)

Format:

* Compressed NDJSON (`.ndjson.gz`)
* One FHIR resource per line
* FHIR JSON resources generated from MIMIC-IV Clinical Database Demo v2.2 and
  MIMIC-IV-ED Demo v2.2

Local resource families:

* Patient
* Encounter, EncounterED, EncounterICU
* Observation, including labs, ICU chart events, ED vitals, microbiology, and
  output events
* Condition and ConditionED
* Procedure, ProcedureED, ProcedureICU
* Medication, MedicationRequest, MedicationAdministration, MedicationDispense,
  MedicationStatementED
* Specimen
* Location
* Organization

---

## Bronze Layer

### Purpose

Preserve the source data exactly as received while adding operational metadata.

### Recommended Table Shape

Implemented local table: `output/bronze/fhir_resources/*.parquet`

Suggested columns:

* `resource_type`
* `resource_id`
* `source_file`
* `resource_family`
* `profile_url`
* `source_dataset_name`
* `source_dataset_version`
* `ingested_at`
* `raw_json`

This project currently uses one generic raw Bronze table. That is the simplest
local shape and keeps resource-specific parsing in Silver.

Additional Bronze artifact:

* `output/bronze/bronze_manifest.json`

The manifest records dataset version, output path, total row count, Parquet part
files, and expected source-file row counts.

### Design Notes

Bronze should not flatten, filter, redact, or reinterpret the resources. Its job
is reproducibility and lineage. Because it intentionally preserves raw FHIR
payloads and source identifiers, privacy-safe outputs must be enforced in later
Silver/Privacy/Gold layers.

---

## Silver Layer

### Purpose

Convert nested FHIR resources into queryable relational-style tables while
retaining enough FHIR context to trace values back to the original resources.

### Core Silver Tables

Implemented core slice:

* `silver_patient`
* `silver_encounter`
* `silver_observation`
* `silver_condition`

Local outputs:

```text
output/silver/
  patient/
  encounter/
  observation/
  condition/
```

Planned later extensions:

* `silver_procedure`
* `silver_medication`
* `silver_medication_request`
* `silver_medication_administration`
* `silver_medication_dispense`
* `silver_specimen`

### Common Normalized Fields

Across clinical event tables, prefer consistent columns where available:

* `resource_id`
* `patient_id`
* `encounter_id`
* `effective_datetime` or event timestamp
* `code`
* `code_system`
* `display`
* `category`
* `status`
* `value`
* `unit`
* `source_file`

### Design Notes

FHIR references such as `Patient/<uuid>` and `Encounter/<uuid>` should be parsed
into stable join keys. Keep original resource ids available in Silver, but avoid
exposing raw source identifiers in final Gold outputs unless intentionally
approved.

---

## FHIR Relationship Audit

### Purpose

Validate that populated core Silver relationships resolve before Gold tables rely
on them.

Implemented local artifacts:

* `output/silver/relationship_audit.json`
* `documentation/relationship_audit.md`

Current checks cover:

* Patient, Encounter, Observation, and Condition row counts
* Missing patient and encounter reference coverage
* Orphan populated patient references
* Orphan populated encounter references

Missing Observation encounter references are reported rather than failed because
the FHIR schema can support observations without encounter context. Populated
references should resolve.

---

## Privacy Validation Layer

### Purpose

Prevent analytics outputs from accidentally exposing fields that are unsuitable
for downstream sharing or presentation.

### What It Should Check

Implemented checks:

* Direct patient-facing identifiers, including synthetic names like
  `Patient_10007795`
* Raw MIMIC subject identifiers stored in `identifier.value`
* Fine-grained birth dates, death dates, admission dates, discharge dates,
  observation effective datetimes, and issued datetimes
* Clinical text-like values that may require review before publishing
* Unexpected URL, email, phone, SSN-like, or IP-address-like patterns
* Unique row-level identifiers and lineage fields that should not appear in
  aggregated outputs unless they are explicitly needed

Implemented local artifacts:

* `output/privacy/privacy_audit.json`
* `documentation/privacy_audit.md`

The current audit treats Silver findings as expected governance inventory rather
than publish-blocking failures. Bronze and Silver preserve identifiers for
lineage; future Gold validation should enforce stricter publishable-output rules.

### Scope Boundary

This layer is intentionally described as HIPAA Safe Harbor-inspired. It can
demonstrate privacy engineering and governance thinking, but it should not claim
to certify legal HIPAA compliance.

The source data is already de-identified by MIMIC. The validation layer is still
useful because lakehouse transformations can accidentally re-expose raw source
identifiers, unnecessary date precision, or row-level linkage keys in Gold
tables.

---

## Gold Layer

### Purpose

Create analytics-ready tables from validated Silver data.

### Recommended Gold Tables

Implemented local outputs:

* `output/gold/encounter_summary/*.parquet`
* `output/gold/condition_summary/*.parquet`
* `output/gold/vitals_daily/*.parquet`
* `output/gold/labs_daily/*.parquet`

Planned later outputs:

* `gold_patient_timeline`
* `gold_medication_activity`
* `gold_phi_audit_report`

### Design Notes

Gold tables use pseudonymous stable keys such as `patient_key` and
`encounter_key` instead of raw Patient, Encounter, or source subject identifiers.
Exact shifted timestamps are generalized into year/month or encounter-relative
day indexes where that is sufficient for analytics.

Gold validation checks that required tables exist, have rows, and do not expose
direct source identifiers, raw resource ids, Bronze lineage ids, or raw JSON.

---

## Data Quality Layer

### Purpose

Provide a single reviewer-friendly quality report across generated local
lakehouse outputs.

Implemented local artifacts:

* `output/quality/data_quality_report.json`
* `documentation/data_quality_report.md`

Current checks cover:

* Bronze manifest validation
* Silver row-count validation against Bronze resource counts
* Silver required id coverage
* FHIR relationship orphan checks
* Privacy pattern findings
* Gold row presence and forbidden identifier column checks
* Gold key uniqueness and aggregate metric sanity checks

Warnings are visible but do not fail the report. For example, missing Observation
encounter references are reported as warnings because they are valid FHIR
coverage gaps rather than orphan reference failures.

---

## Local Pipeline Orchestration

### Purpose

Run the complete local lakehouse workflow in dependency order and record the run.

Implemented local artifacts:

* `output/pipeline/pipeline_run.json`
* `documentation/pipeline_run.md`

Current steps:

1. Source profile report
2. Bronze ingest and validate
3. Silver build and validate
4. Relationship report
5. Privacy report
6. Gold build and validate
7. Data quality report

The local runner is intentionally linear and stops on the first failed step.
Scheduling, backfills, incremental processing, and cloud execution belong to
later milestones.

### Example Questions Supported

* What clinical events occurred during each encounter?
* How did heart rate, respiratory rate, temperature, oxygen saturation, or lab
  values change over time?
* What diagnoses are most common in the demo population?
* Which medications were ordered, dispensed, or administered during encounters?
* Which Gold outputs pass privacy validation checks?

---

## Implementation Strategy

Recommended order:

1. Use the local Bronze ingestion from `.ndjson.gz`.
2. Use the core Silver Patient, Encounter, Observation, and Condition tables.
3. Add privacy validation checks against Silver and Gold schemas.
4. Build Gold encounter summaries and vitals/labs aggregates.
5. Add medication and procedure tables.
6. Port the pipeline to Spark/Delta/Databricks if production-style execution is
   desired.

This order proves the end-to-end architecture quickly while keeping the first
slice grounded in the highest-value, highest-volume resources.

## Cloud Lakehouse Implementation

Implemented Databricks target:

```text
catalog: workspace
raw schema: healthcare_fhir_lakehouse_raw
bronze schema: healthcare_fhir_lakehouse_bronze
silver schema: healthcare_fhir_lakehouse_silver
gold schema: healthcare_fhir_lakehouse_gold
audit schema: healthcare_fhir_lakehouse_audit
raw volume: workspace.healthcare_fhir_lakehouse_raw.fhir_demo
```

The cloud pipeline is implemented in
`src/healthcare_fhir_lakehouse_spark/cloud_pipeline.py` and configured in
`databricks.yml`. The first successful run used Databricks serverless Jobs
compute and wrote:

* Bronze: `workspace.healthcare_fhir_lakehouse_bronze.fhir_resources`
* Silver: `patient`, `encounter`, `observation`, `condition`
* Gold: `encounter_summary`, `condition_summary`, `vitals_daily`, `labs_daily`
* Audit: `relationship_audit`, `privacy_audit`, `data_quality_report`

Cloud run evidence is captured in `documentation/cloud_run_evidence.md`.

---

## Known Constraints

* The demo has only 100 patients, so population-level analytics should be framed
  as examples rather than clinical findings.
* Some terminology uses MIMIC-specific codes rather than standard vocabularies.
* Dates are shifted/de-identified but still precise within the synthetic timeline.
* The source is FHIR-shaped JSON, not an existing FHIR server. Server-specific
  features such as search APIs or Bulk Data export would need separate tooling.
* Full production scale would require the credentialed full MIMIC-IV-on-FHIR
  dataset or another larger FHIR source.
* The implemented Databricks demo uses Unity Catalog managed volumes rather than
  S3/IAM/Terraform. That is sufficient for platform evidence, while a production
  deployment would usually externalize storage and infrastructure management.

---

## Summary

The planned architecture is feasible. The dataset is well matched to a healthcare
lakehouse demo because it contains realistic linked FHIR resources across
patients, encounters, labs, vitals, diagnoses, procedures, medications, and ED/ICU
events. The main adjustment is scope: describe the project as a demo-scale,
privacy-aware lakehouse rather than a production HIPAA compliance system.
