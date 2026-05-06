# Cloud Storage Layout

## Selected Layout

Milestone 10 uses Unity Catalog managed storage in the existing Databricks
workspace.

The initial preferred namespace was a dedicated catalog:

```text
catalog: healthcare_fhir_lakehouse
schemas:
  raw
  bronze
  silver
  gold
  audit
```

Actual implemented namespace:

```text
catalog: workspace
schemas:
  healthcare_fhir_lakehouse_raw
  healthcare_fhir_lakehouse_bronze
  healthcare_fhir_lakehouse_silver
  healthcare_fhir_lakehouse_gold
  healthcare_fhir_lakehouse_audit
```

The fallback was necessary because dedicated catalog creation failed without a
configured metastore storage root.

## Raw Volume

Raw FHIR files are stored in a managed Unity Catalog volume:

```text
/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/fhir/
dbfs:/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/fhir/
```

Uploaded source status:

* 30 compressed FHIR NDJSON files uploaded
* source path: `mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/`

The cloud Spark task also reads its mirrored execution script from:

```text
dbfs:/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/code/cloud_pipeline.py
```

## Delta Tables

Bronze:

```text
workspace.healthcare_fhir_lakehouse_bronze.fhir_resources
```

Silver:

```text
workspace.healthcare_fhir_lakehouse_silver.patient
workspace.healthcare_fhir_lakehouse_silver.encounter
workspace.healthcare_fhir_lakehouse_silver.observation
workspace.healthcare_fhir_lakehouse_silver.condition
```

Gold:

```text
workspace.healthcare_fhir_lakehouse_gold.encounter_summary
workspace.healthcare_fhir_lakehouse_gold.condition_summary
workspace.healthcare_fhir_lakehouse_gold.vitals_daily
workspace.healthcare_fhir_lakehouse_gold.labs_daily
```

Audit:

```text
workspace.healthcare_fhir_lakehouse_audit.relationship_audit
workspace.healthcare_fhir_lakehouse_audit.privacy_audit
workspace.healthcare_fhir_lakehouse_audit.data_quality_report
```

## Why Managed Volumes

Managed volumes keep the first cloud version focused on Databricks lakehouse
engineering instead of AWS IAM/S3 setup.

This is still GitHub-friendly because the repository stores the code, bundle
configuration, workflow definition, and setup documentation. The Databricks
workspace stores runtime data and generated cloud tables.

## Uploaded Source

Actual uploaded files:

* 30 compressed FHIR NDJSON files
* about 50 MB total compressed
* source path: `mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/`
