# Cloud Storage Layout

## Selected Layout

The Databricks implementation uses Unity Catalog managed storage in the existing
workspace.

Production-style namespace:

```text
catalog: healthcare_fhir_lakehouse
schemas:
  raw
  bronze
  silver
  gold
  audit
```

Implemented demo namespace:

```text
catalog: workspace
schemas:
  healthcare_fhir_lakehouse_raw
  healthcare_fhir_lakehouse_bronze
  healthcare_fhir_lakehouse_silver
  healthcare_fhir_lakehouse_gold
  healthcare_fhir_lakehouse_audit
```

The implemented demo uses the existing `workspace` catalog because the metastore
does not expose a storage root for a dedicated project catalog.

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
workspace.healthcare_fhir_lakehouse_silver.medication
workspace.healthcare_fhir_lakehouse_silver.medication_ingredient
workspace.healthcare_fhir_lakehouse_silver.medication_request
workspace.healthcare_fhir_lakehouse_silver.medication_administration
workspace.healthcare_fhir_lakehouse_silver.medication_dispense
workspace.healthcare_fhir_lakehouse_silver.medication_statement
workspace.healthcare_fhir_lakehouse_silver.procedure
```

Gold:

```text
workspace.healthcare_fhir_lakehouse_gold.encounter_summary
workspace.healthcare_fhir_lakehouse_gold.condition_summary
workspace.healthcare_fhir_lakehouse_gold.vitals_daily
workspace.healthcare_fhir_lakehouse_gold.labs_daily
workspace.healthcare_fhir_lakehouse_gold.medication_activity
workspace.healthcare_fhir_lakehouse_gold.medication_order_fulfillment
workspace.healthcare_fhir_lakehouse_gold.procedure_summary
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
