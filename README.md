# Healthcare FHIR Lakehouse

## Overview

This project is a demo-scale healthcare data engineering lakehouse built around the
[MIMIC-IV Clinical Database Demo on FHIR v2.1.0](https://physionet.org/content/mimic-iv-fhir-demo/2.1.0/).
The dataset contains 100 de-identified demo patients and their associated FHIR
resources, distributed as compressed NDJSON files.

The goal is to turn realistic clinical FHIR data into structured, analytics-ready
tables while demonstrating the data engineering patterns used in healthcare:
raw ingestion, normalization, privacy-aware validation, and curated analytics
outputs.

This repository currently contains the project documentation, the downloaded demo
dataset, the Python project foundation, source profiling utilities, local Bronze
ingestion, core Silver clinical tables, FHIR relationship auditing, a privacy
validation audit, Gold analytics tables, consolidated data quality reporting, and
a one-command local pipeline runner. It also includes a Databricks/Spark cloud
port that has run successfully on Databricks serverless compute.

---

## Current Status

The core portfolio project is implemented end to end.

* Local pipeline: implemented and runnable with `make pipeline`
* Local validation: 76 automated tests passing
* Cloud target: Databricks serverless job with Delta tables in Unity Catalog
* Cloud run: successful on 2026-05-06
* Cloud data quality: 10 passing checks, 0 failing checks

Start here for review:

* `documentation/portfolio_brief.md` for the one-page project signal summary
* `documentation/runbook.md` for local and Databricks reproduction steps
* `documentation/ARCHITECTURE.md` for the system design
* `documentation/source_data_profile.md` for dataset profiling
* `documentation/cloud_run_evidence.md` for Databricks run evidence
* `documentation/data_quality_report.md` for quality results
* `documentation/privacy_audit.md` for privacy validation results

---

## Why This Dataset Works

The selected source is appropriate for this project. It includes the core clinical
entities needed for a lakehouse pipeline:

* Patient demographics
* Hospital, ICU, and emergency department encounters
* Lab results, charted ICU observations, ED vitals, microbiology, and outputs
* Diagnoses and procedures
* Medication orders, administrations, dispenses, statements, and medication
  definitions
* Specimens, locations, and organization metadata

Local dataset check:

* 100 patients
* 637 encounter resources
* 813,540 observation resources
* 93,667 medication-related resources
* 928,935 total FHIR resources

That is enough to build realistic demo tables such as patient timelines, encounter
summaries, vitals/lab aggregates, diagnosis summaries, medication activity tables,
and privacy audit reports.

---

## Key Features

Implemented capabilities:

* Ingest compressed FHIR NDJSON files into a Bronze layer
* Preserve raw source payloads with ingestion metadata
* Normalize FHIR resources into Silver clinical tables
* Audit FHIR patient and encounter relationships across Silver tables
* Apply privacy validation checks inspired by HIPAA Safe Harbor
* Produce Gold analytics tables for common clinical questions
* Generate audit outputs describing relationship and privacy validation results
* Run consolidated data quality checks across generated lakehouse outputs
* Run the full local lakehouse pipeline with a recorded run manifest
* Run a Databricks serverless Spark job that writes Bronze, Silver, Gold, and
  audit Delta tables

---

## Architecture

The project follows a layered lakehouse design:

```text
FHIR NDJSON files
  -> Bronze raw tables
  -> Silver normalized FHIR tables
  -> Privacy validation
  -> Gold analytics tables
```

### Bronze

Raw ingestion of each `.ndjson.gz` file. Bronze preserves the original FHIR JSON
resource as `raw_json` and adds source filename, resource family, resource type,
resource id, profile URL, dataset version, and ingestion timestamp.

Current local output:

```text
output/bronze/
  bronze_manifest.json
  fhir_resources/
    *.parquet
```

The generic Bronze table intentionally preserves identifiers and raw payloads.
Privacy filtering is enforced later, after Silver and Gold define downstream
outputs.

### Silver

Flattened and normalized resource-specific tables. The current implemented core
Silver tables are:

```text
output/silver/
  patient/
  encounter/
  observation/
  condition/
```

These tables parse common FHIR references and coding fields while preserving
lineage back to Bronze.

### Privacy Layer

Validation checks that identify fields unsuitable for analytics outputs, such as
direct patient identifiers, synthetic patient names, raw source identifiers,
fine-grained date fields where not needed, and free-text fields that may require
special handling.

Current local output:

```text
output/privacy/
  privacy_audit.json
documentation/privacy_audit.md
```

This layer is educational and governance-oriented. It should be described as
HIPAA Safe Harbor-inspired, not as a guarantee of HIPAA compliance.

### Gold

Curated analytics datasets built from validated Silver tables, such as:

Implemented local outputs:

```text
output/gold/
  encounter_summary/
  condition_summary/
  vitals_daily/
  labs_daily/
```

Implemented tables:

* Encounter summary
* Daily vitals and labs
* Condition and diagnosis summary

Planned later tables:

* Patient timeline
* Medication activity summary
* PHI/privacy validation report

---

## Tech Stack

Target production-style stack:

* Databricks / Apache Spark
* Delta Lake
* Unity Catalog managed volumes for the implemented demo cloud run
* Amazon S3 as the natural production object-storage extension
* Python and SQL
* Parquet / Delta tables

Current local development stack:

* Python
* DuckDB or pandas
* pyarrow for Parquet output
* Typer for local CLI commands
* pytest for transformation and validation tests
* Ruff for linting
* uv for environment and dependency management

---

## Project Structure

Current repository:

```text
healthcare-fhir-lakehouse/
  config/
    local.example.toml
  README.md
  databricks.yml
  documentation/
    ARCHITECTURE.md
    TECH_STACK.md
    cloud_setup.md
    cloud_storage_layout.md
    cloud_workflow.md
    cloud_run_evidence.md
    source_data_profile.md
    milestones/
      *.md
  notebooks/
    README.md
  output/
    bronze/
      bronze_manifest.json
      fhir_resources/
        *.parquet
    gold/
      encounter_summary/
      condition_summary/
      vitals_daily/
      labs_daily/
    privacy/
      privacy_audit.json
    profiling/
      resource_inventory.json
      schema_profile.json
    quality/
      data_quality_report.json
    pipeline/
      pipeline_run.json
    silver/
      patient/
      encounter/
      observation/
      condition/
      relationship_audit.json
  mimic-iv-clinical-database-demo-on-fhir-2.1.0/
    README_DEMO.md
    LICENSE.txt
    SHA256SUMS.txt
    fhir/
      *.ndjson.gz
  src/
    healthcare_fhir_lakehouse/
      common/
      ingest/
      bronze/
      silver/
      privacy/
      gold/
      pipeline/
      quality/
    healthcare_fhir_lakehouse_spark/
      cloud_pipeline.py
  tests/
  Makefile
  pyproject.toml
  uv.lock
```

---

## Local Development

This project uses `uv` for Python environment and dependency management.

Install dependencies:

```bash
uv sync
```

Check that the local project scaffold and source FHIR directory are available:

```bash
make doctor
```

Run tests:

```bash
make test
```

Run linting:

```bash
make lint
```

Profile the source FHIR dataset and regenerate the source data profile report:

```bash
make profile
```

Run individual profiling steps:

```bash
uv run healthcare-fhir-lakehouse profile inventory
uv run healthcare-fhir-lakehouse profile schema
uv run healthcare-fhir-lakehouse profile report
```

Build and validate the Bronze layer:

```bash
make bronze
```

Run individual Bronze steps:

```bash
uv run healthcare-fhir-lakehouse bronze ingest
uv run healthcare-fhir-lakehouse bronze validate
```

Build and validate the core Silver tables:

```bash
make silver
```

Run individual Silver steps:

```bash
uv run healthcare-fhir-lakehouse silver build all
uv run healthcare-fhir-lakehouse silver build patient
uv run healthcare-fhir-lakehouse silver build encounter
uv run healthcare-fhir-lakehouse silver build observation
uv run healthcare-fhir-lakehouse silver build condition
uv run healthcare-fhir-lakehouse silver validate
```

Generate the FHIR relationship audit:

```bash
make relationships
```

Run individual relationship steps:

```bash
uv run healthcare-fhir-lakehouse relationships audit
uv run healthcare-fhir-lakehouse relationships report
```

Generate the privacy validation audit:

```bash
make privacy
```

Run individual privacy steps:

```bash
uv run healthcare-fhir-lakehouse privacy audit
uv run healthcare-fhir-lakehouse privacy report
```

Build and validate the Gold analytics tables:

```bash
make gold
```

Run individual Gold steps:

```bash
uv run healthcare-fhir-lakehouse gold build all
uv run healthcare-fhir-lakehouse gold build encounter_summary
uv run healthcare-fhir-lakehouse gold build condition_summary
uv run healthcare-fhir-lakehouse gold build vitals_daily
uv run healthcare-fhir-lakehouse gold build labs_daily
uv run healthcare-fhir-lakehouse gold validate
```

Generate the consolidated data quality report:

```bash
make quality
```

Run individual quality steps:

```bash
uv run healthcare-fhir-lakehouse quality check
uv run healthcare-fhir-lakehouse quality report
```

Run the full local pipeline:

```bash
make pipeline
```

Equivalent CLI command:

```bash
uv run healthcare-fhir-lakehouse pipeline run
```

Validate the Databricks Asset Bundle definition:

```bash
make cloud-validate
```

Print resolved local configuration:

```bash
uv run healthcare-fhir-lakehouse config
```

Print the package version:

```bash
uv run healthcare-fhir-lakehouse version
```

Remove generated local outputs while preserving `output/.gitkeep`:

```bash
make clean-output
```

Milestone 1 establishes the project foundation: package structure, dependency
metadata, config loading, CLI entry points, Makefile shortcuts, and smoke tests.
Milestone 2 profiles the source dataset and writes
`documentation/source_data_profile.md`. Milestone 3 builds and validates the
local Bronze layer. Milestone 4 builds the core Silver Patient, Encounter,
Observation, and Condition tables. Milestone 5 audits FHIR relationships across
core Silver tables. Milestone 6 adds privacy validation over current Silver
outputs. Milestone 7 builds the first Gold encounter, condition, vitals, and lab
analytics tables. Milestone 8 adds a consolidated data quality report. Milestone
9 adds a full local pipeline runner and run manifest. Milestone 10 ports the core
lakehouse to Databricks/Spark/Delta and records a successful cloud run in
`documentation/cloud_run_evidence.md`. Milestone 11 packages the project for
portfolio review with a brief, runbook, and final documentation polish.

---

## Databricks Cloud Run

The cloud version runs on Databricks serverless compute and writes Delta tables
under project-prefixed Unity Catalog schemas in the `workspace` catalog.

Successful run evidence:

* Databricks job: `healthcare_fhir_lakehouse_pipeline`
* Job ID: `1036260011587635`
* Successful run ID: `961090542671457`
* Raw files uploaded to a managed volume: 30
* Cloud Bronze row count: 928,935
* Cloud Silver row counts: 100 patients, 637 encounters, 813,540 observations,
  and 5,051 conditions
* Cloud data quality checks: 10 passing, 0 failing

See:

* `documentation/cloud_setup.md`
* `documentation/cloud_storage_layout.md`
* `documentation/cloud_workflow.md`
* `documentation/cloud_run_evidence.md`

---

## Important Scope Notes

This is a strong demo and portfolio project, but it has a few deliberate limits:

* The dataset has 100 patients, so it demonstrates architecture and modeling
  rather than true production scale.
* The data is already de-identified by the MIMIC project, but downstream outputs
  should still avoid exposing raw identifiers or unnecessary date precision.
* HIPAA Safe Harbor-inspired checks are valuable for learning and governance, but
  they do not constitute legal compliance certification.
* FHIR preserves rich healthcare structure, but some source concepts use MIMIC
  local codes rather than standard vocabularies such as LOINC or RxNorm.

---

## Example Outputs

* Daily vitals aggregation
* Daily lab aggregation
* Encounter summaries
* Condition and diagnosis summaries
* Medication order/admin/dispense summaries
* Relationship audit report
* Privacy validation audit report
* Data quality report
* Pipeline run report

---

## Future Improvements

* Build medication and procedure Silver/Gold tables
* Expand Databricks Workflows into a multi-task workflow with separate observability
  checkpoints
* Add data quality checks with Great Expectations or a lightweight equivalent
* Add dashboards for clinical analytics outputs

---

## Disclaimer

This project uses publicly available, de-identified demo data. It is intended for
education, portfolio development, and data engineering practice. The privacy
validation layer is inspired by HIPAA Safe Harbor concepts but does not guarantee
HIPAA compliance.
