# Cloud Workflow

## Databricks Job

The cloud lakehouse runs as a Databricks serverless job.

```text
job name: healthcare_fhir_lakehouse_pipeline
job id: 1036260011587635
task key: run_cloud_pipeline
compute: serverless job environment version 2
```

The job executes:

```text
dbfs:/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/code/cloud_pipeline.py
```

That file is a mirror of the source-controlled implementation:

```text
src/healthcare_fhir_lakehouse_spark/cloud_pipeline.py
```

## Workflow Steps

The Spark task runs the same lakehouse flow as the local pipeline:

1. Read compressed FHIR NDJSON from the managed Unity Catalog volume.
2. Build Bronze Delta table `workspace.healthcare_fhir_lakehouse_bronze.fhir_resources`.
3. Build Silver Delta tables for Patient, Encounter, Observation, Condition,
   Medication catalog, medication event resources, and Procedure.
4. Write relationship and privacy audit Delta tables across the expanded Silver
   clinical model.
5. Build Gold Delta tables for encounter summaries, condition summaries, daily
   vitals/labs, medication activity, medication order fulfillment, and procedure
   summaries.
6. Write a cloud data quality report and fail the task if a hard check fails.

## Parameters

```text
--catalog workspace
--raw-schema healthcare_fhir_lakehouse_raw
--bronze-schema healthcare_fhir_lakehouse_bronze
--silver-schema healthcare_fhir_lakehouse_silver
--gold-schema healthcare_fhir_lakehouse_gold
--audit-schema healthcare_fhir_lakehouse_audit
--raw-volume fhir_demo
--raw-subdir fhir
```

## Source-Controlled Bundle

`databricks.yml` defines the intended Databricks Asset Bundle job. Bundle
validation succeeds:

```bash
make cloud-validate
```

The validated run used the Databricks Jobs API/CLI job path because
`databricks bundle deploy` failed locally while downloading Terraform with an
expired OpenPGP signing key. This does not change the project architecture:
GitHub stores the implementation and bundle definition, while Databricks stores
runtime objects and generated Delta tables.
