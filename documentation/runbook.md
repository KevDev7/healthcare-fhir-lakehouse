# Reproducibility Runbook

## Purpose

Use this runbook to reproduce the local lakehouse pipeline and validate the
Databricks cloud target.

The local pipeline is the fastest way to inspect the project end to end. The
Databricks target demonstrates that the same core lakehouse architecture can run
as Spark/Delta jobs on a healthcare-relevant cloud data platform.

## Prerequisites

Local:

* Python 3.11 or newer
* `uv`
* Source dataset folder:
  `mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/`

Cloud validation:

* Databricks CLI
* Authenticated CLI profile with access to the target workspace
* Existing Unity Catalog schemas and managed volume for the project namespace
* Databricks serverless Jobs support

## Local Setup

Install dependencies:

```bash
uv sync
```

Check project paths:

```bash
make doctor
```

Expected result:

```text
Project paths resolve and the source FHIR directory is available.
```

## Local Pipeline

Run the full local pipeline:

```bash
make pipeline
```

Expected generated outputs:

```text
output/profiling/resource_inventory.json
output/profiling/schema_profile.json
output/bronze/bronze_manifest.json
output/bronze/fhir_resources/*.parquet
output/silver/patient/*.parquet
output/silver/encounter/*.parquet
output/silver/observation/*.parquet
output/silver/condition/*.parquet
output/silver/relationship_audit.json
output/privacy/privacy_audit.json
output/gold/encounter_summary/*.parquet
output/gold/condition_summary/*.parquet
output/gold/vitals_daily/*.parquet
output/gold/labs_daily/*.parquet
output/quality/data_quality_report.json
output/pipeline/pipeline_run.json
```

The local pipeline also refreshes reviewer-friendly Markdown reports under
`documentation/`.

## Local Validation

Run lint:

```bash
make lint
```

Run tests:

```bash
make test
```

Current expected test result:

```text
76 passed
```

Run individual stages if needed:

```bash
make profile
make bronze
make silver
make relationships
make privacy
make gold
make quality
```

## Databricks Validation

Validate the source-controlled Databricks Asset Bundle:

```bash
make cloud-validate
```

Expected result:

```text
Validation OK!
```

Cloud objects used by the Databricks implementation:

```text
catalog: workspace
raw schema: healthcare_fhir_lakehouse_raw
bronze schema: healthcare_fhir_lakehouse_bronze
silver schema: healthcare_fhir_lakehouse_silver
gold schema: healthcare_fhir_lakehouse_gold
audit schema: healthcare_fhir_lakehouse_audit
raw volume: workspace.healthcare_fhir_lakehouse_raw.fhir_demo
job name: healthcare_fhir_lakehouse_pipeline
job id: 1036260011587635
```

Successful run evidence:

```text
run id: 961090542671457
result: SUCCESS
execution duration: 115 seconds
```

See `documentation/cloud_run_evidence.md` for table names, row counts, and data
quality results.

## Expected Cloud Row Counts

| Layer | Table | Expected rows |
| --- | --- | ---: |
| Bronze | `fhir_resources` | 928,935 |
| Silver | `patient` | 100 |
| Silver | `encounter` | 637 |
| Silver | `observation` | 813,540 |
| Silver | `condition` | 5,051 |
| Gold | `encounter_summary` | 637 |
| Gold | `condition_summary` | 2,319 |
| Gold | `vitals_daily` | 3,986 |
| Gold | `labs_daily` | 90,719 |

Expected cloud data quality result:

```text
10 passing checks, 0 failing checks
```

## Cloud Deployment Note

`databricks bundle validate` succeeds, but the first successful run used a Jobs
API/CLI deployment path because `databricks bundle deploy` failed locally while
downloading Terraform with an expired OpenPGP signing key.

This does not hide project logic. The source-controlled files contain the Spark
pipeline and bundle definition. Databricks contains runtime data, generated
tables, logs, and job runs.

## Cleanup

Remove generated local outputs while preserving `output/.gitkeep`:

```bash
make clean-output
```

This does not remove Databricks resources.
